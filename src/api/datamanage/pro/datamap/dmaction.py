# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import arrow
import datetime

from django.db import connections
from django.conf import settings
from django.utils.translation import ugettext as _

from common.local import get_local_param
from common.exceptions import ValidationError

from datamanage.lite.tag import tagaction
from datamanage.utils.api.meta import MetaApi
from datamanage.pro.datamap.format import format_search_res
from datamanage.pro.datamap.ql_process import (
    is_filter_by_lifecycle,
    filter_dataset_by_lifecycle,
    lifecycle_order_format,
    get_lifecycle_type,
    filter_dataset_by_storage_type,
)
from datamanage.lite.datamap.dmaction import build_tree, meta_dgraph_complex_search

DATA_MART_ROOT_NAME = 'metric_domain'
TAG_DIMENSION = 'dimension'
DATA_MART_OTHER_NAME = 'other'
CAL_TYPE_STANDARD = 'standard'
CAL_TYPE_ONLY_STANDARD = 'only_standard'
CAL_TYPE_ONLY_NOT_STANDARD = 'only_not_standard'
CAL_TYPE_TAG = 'tag'
VIRTUAL_DATA_MART_ROOT_NAME = 'virtual_data_mart'
VIRTUAL_DM_POS_FIELD = 'dm_pos'
DATAMAP_LOC_FIELD = 'loc'
TDW_BK_BIZ_ID = 100616

NEED_DEFAULT_DETAIL = 0
NEED_FLOATING_DETAIL = 1
NEED_STANDARD_FLOATING_DETAIL = 2
NEED_RECENT_DETAIL = 3
NEED_DATA_SET_ID_DETAIL = 4

PARAM_RT_FILTER = '${rt_filter}'
PARAM_DS_FILTER = '${ds_filter}'
TYPE_ALL = 'all'
TYPE_RESULT_TABLE = 'result_table'
TYPE_RAW_DATA = 'raw_data'
TYPE_CLEAN = 'clean'

TYPE_BK_DATA = 'bk_data'
TYPE_TDW = 'tdw'

VIRTUAL_OTHER_NODE_PRE = 'virtual_other_node_pre_'
VIRTUAL_OTHER_NOT_STANDARD_NODE_PRE = 'virtual_other_not_standard_node_pre_'

DGRAPH_OVERALL_MAP = 'overall_map'
DGRAPH_FLOATING = 'floating'

D_UID_RESULT_TABLE_KEY = 'rt_uids'
D_UID_RAW_DATA_KEY = 'rd_uids'
D_UID_TDW_TABLE_KEY = 'tdw_uids'

DGRAPH_CACHE_KEY = 'd_cache_key'
DGRAPH_TIME_OUT = 60 * 60 * 2

DATA_MART_STR = _('数据集市')
OTHER_STR = _('其他')

RUN_MODE = getattr(settings, 'RUN_MODE', 'DEVELOP')


processing_type_dict = {
    "batch_model": _("ModelFlow模型"),
    "stream": _("实时计算"),
    "stream_model": _("ModelFlow模型"),
    "batch": _("离线计算"),
    "transform": _("转换"),
    "clean": _("清洗"),
    "model": _("场景化模型"),
    "storage": _("存储类计算"),
    "view": _("视图"),
}
processing_type_list = list(processing_type_dict.keys())


def exclude_bk_biz_id_cond(table_pre=''):
    appenv = get_local_param('appenv', 'ieod')
    cond = ''
    if table_pre:
        table_pre += '.'
    if appenv == 'ieod':
        cond = ' and {table_pre}bk_biz_id<200000'
    elif appenv == 'clouds':
        cond = ' and {table_pre}bk_biz_id>200000'
    return cond.format(table_pre=table_pre)


def dgraph_exclude_bk_biz_id_cond(table_pre=''):
    """
    判断是都要在查询语句中过滤掉海外业务
    :param table_pre:
    :return:
    """
    appenv = get_local_param('appenv', 'ieod')
    cond = ''
    if table_pre:
        table_pre += '.'
    if appenv == 'ieod':
        cond = 'lt({table_pre}bk_biz_id,200000)'
    elif appenv == 'clouds':
        cond = 'gt({table_pre}bk_biz_id,200000)'
    return cond.format(table_pre=table_pre)


def total_count(conn):
    total_count_sql = """select(
    (select count(*) from result_table)+(select count(*) from access_raw_data)) as total_count"""
    total_count_result = tagaction.query_direct_sql_to_map_list(conn, total_count_sql)
    return total_count_result[0]['total_count']


def index_total_count(conn):
    result_dict = {}
    index_total_sql = """select count(*) total_count,count(distinct project_id) total_project_count from result_table
        union all
        select count(*) total_count,0 as total_project_count from access_raw_data
        union all
        select count(distinct bk_biz_id) total_count,0 as total_project_count from(
        select bk_biz_id from result_table
        union all
        select bk_biz_id from access_raw_data)tmp"""
    index_total_result = tagaction.query_direct_sql_to_map_list(conn, index_total_sql)
    result_dict['total_dataset_count'] = index_total_result[0]['total_count']
    result_dict['total_project_count'] = index_total_result[0]['total_project_count']
    result_dict['total_data_source_count'] = index_total_result[1]['total_count']
    result_dict['total_bk_biz_count'] = index_total_result[2]['total_count']
    return result_dict


def is_standard_node(node_dict):  # 判断是否标准节点
    me_type = node_dict['me_type']
    if me_type == CAL_TYPE_STANDARD:
        return True
    return False


def build_data_mart_tree(conn):  # 构建数据集市tree
    main_part_sql = """select t1.*,t2.loc,t2.seq_index datamap_seq_index from(
    select 0 as is_selected,'tag' as me_type,a.id category_id,a.code category_name,a.alias category_alias,a.parent_id,
    a.seq_index,a.sync,a.tag_type,a.kpath,a.icon,a.description,b.code parent_code from tag a
    left join tag b on a.parent_id=b.id where a.active=1 order by parent_code,seq_index) t1
    left join datamap_layout t2 on t1.category_name=t2.category order by t2.loc asc,t2.seq_index asc"""
    main_list = tagaction.query_direct_sql_to_map_list(conn, main_part_sql)
    tree_list = build_tree(main_list, 'category_id')
    metric_domain_list, dimension_list = [], []
    for node_dict in tree_list:
        category_name = node_dict['category_name']
        if category_name == DATA_MART_ROOT_NAME:
            node_dict['category_alias'] = DATA_MART_STR
            metric_domain_list.append(node_dict)
        elif category_name == TAG_DIMENSION:
            dimension_list.append(node_dict)
    return metric_domain_list, dimension_list


def get_day_list():
    day_list = []
    now = datetime.date.today()

    for i in range(7):
        td = now - datetime.timedelta(days=i)
        day_list.append(td.strftime('%Y%m%d'))

    day_list.reverse()
    return day_list


def index_trend_result(day_list, result_list, index_name):
    index_count_dict = {}
    if result_list:
        for dc_dict in result_list:
            index_count_dict[dc_dict['created_day']] = dc_dict['index_count']

    index_count_sum = 0
    index_count_list = []
    for day in day_list:
        dc_count = index_count_dict.get(day, 0)
        index_count_sum += dc_count
        index_count_list.append({'day': day, index_name: dc_count})

    return index_count_list, index_count_sum


def get_standard_count_sql(keyword, bk_biz_id, project_id, standard_version_id_cond, rt_where_cond):
    data_set_id_where_cond = get_data_set_id_cond(keyword, bk_biz_id, project_id, rt_where_cond)
    st_cal_sql = """select count(if(a.task_type='detaildata',true,null)) detaildata_dataset_count,
        count(if(a.task_type='indicator',true,null)) indicator_dataset_count,
        count(distinct a.bk_biz_id) bk_biz_count,count(distinct a.project_id) project_count,
        count(distinct a.data_set_id) dataset_count """
    detail_sql = """ from dm_task_detail a,dm_standard_version_config b where a.standard_version_id=b.id
    and b.standard_version_status='online' and
    a.active=1 and a.data_set_type='result_table' ${standard_version_id} ${data_set_id_where_cond}"""
    detail_sql = detail_sql.replace('${standard_version_id}', standard_version_id_cond)
    detail_sql = detail_sql.replace('${rt_where_cond}', rt_where_cond)
    detail_sql = detail_sql.replace('${data_set_id_where_cond}', data_set_id_where_cond)
    return st_cal_sql + detail_sql, "select ${need_fields} " + detail_sql


def get_optimize_bk_biz_count_sql(
    sys_where_cond, bk_biz_id, project_id
):  # keyword为空,不需要根据内容过滤result_table表和access_raw_data表的sql优化
    bk_biz_count_sql = """select count(distinct bk_biz_id) bk_biz_count from tag_target where active=1
    and target_type in('result_table','table','raw_data','data_id')
    ${bus_where_cond} ${source_tag_where_cond}"""
    if bk_biz_id is not None:
        bk_biz_count_sql += ' and bk_biz_id=' + str(bk_biz_id)
    if project_id:
        bk_biz_count_sql += ' and project_id=' + str(project_id)
    if sys_where_cond:
        bk_biz_count_sql += """ and target_id in(select target_id from tag_target
            where target_type in('result_table','table','raw_data','data_id') ${sys_where_cond})"""
        bk_biz_count_sql = bk_biz_count_sql.replace('${sys_where_cond}', sys_where_cond)

    return bk_biz_count_sql


def get_only_standard_filter_cond(filter_field, only_rt=False, only_rd=False):
    only_standard_cond = ' and ' + filter_field
    target_type_cond = ''
    if only_rt:
        target_type_cond = " and data_set_type='result_table'"
    if only_rd:
        target_type_cond = " and data_set_type='raw_data'"

    only_standard_cond += """ in(select data_set_id from dm_task_detail where active=1 {target_type_cond}
    and standard_version_id in(
    select b.id standard_version_id  from dm_standard_config a,dm_standard_version_config b,tag_target c
    where b.standard_version_status='online'
    and a.id=b.standard_id and a.id=c.target_id and a.active=1  and c.active=1 and c.active=1
     and c.tag_code=c.source_tag_code and c.target_type='standard')) """.format(
        target_type_cond=target_type_cond
    )
    return only_standard_cond


def get_optimize_floating_index_count(
    sys_where_cond_list,
    bk_biz_id,
    project_id,
    is_overall_search,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    tdw_where_cond,
    only_standard=False,
    data_set_type=TYPE_ALL,
    need_detail_type=NEED_DEFAULT_DETAIL,
    platform=TYPE_ALL,
):
    if is_overall_search:
        cal_sql = """select tag_code as data_category, count(distinct target_id) total_count,
        count(target_id) all_count """
    else:
        cal_sql = """select
        count(distinct (case when target_type in('result_table','table','tdw_table') then target_id end)) dataset_count,
        count(distinct (case when target_type in('raw_data','data_id') then target_id end)) data_source_count,
        count(distinct bk_biz_id) bk_biz_count,
        count(distinct project_id) project_count """

    target_type_cond = "'result_table','table','raw_data','data_id'"
    if platform == TYPE_ALL:
        target_type_cond += ",'tdw_table'"
    elif platform == TYPE_TDW:
        target_type_cond = "'tdw_table'"

    detail_sql = """ from tag_target where active=1 ${bus_where_cond} ${source_tag_where_cond} """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RESULT_TABLE:
        detail_sql += " and target_type in('result_table','table','tdw_table') "
    elif need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        detail_sql += " and target_type in('raw_data','data_id') "
    else:
        detail_sql += " and target_type in({}) ".format(target_type_cond)

    if bk_biz_id is not None:
        detail_sql += ' and bk_biz_id=' + str(bk_biz_id)
    else:
        detail_sql += exclude_bk_biz_id_cond()

    if project_id:
        detail_sql += ' and project_id=' + str(project_id)
    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            detail_sql += """ and target_id in(select target_id from tag_target
                                where target_type in(${target_type_cond}) ${sys_where_cond})"""
            detail_sql = detail_sql.replace('${target_type_cond}', target_type_cond)
            detail_sql = detail_sql.replace('${sys_where_cond}', sys_where_cond)

    if only_standard:
        detail_sql += get_only_standard_filter_cond('target_id')

    detail_sql = detail_sql.replace('${bus_where_cond}', bus_where_cond)
    detail_sql = detail_sql.replace('${source_tag_where_cond}', source_tag_where_cond)

    cal_sql += detail_sql

    if is_overall_search:
        cal_sql += " group by tag_code"

    detail_sql = "select ${need_fields} " + detail_sql
    return cal_sql, detail_sql


def get_common_sql(
    sys_where_cond_list,
    is_overall_search,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    tdw_where_cond,
    only_standard=False,
    data_set_type=TYPE_ALL,
    need_detail_type=NEED_DEFAULT_DETAIL,
    platform=TYPE_ALL,
):
    if is_overall_search:
        cal_sql = """select data_category,count(distinct target_id) total_count,count(target_id) all_count """
    else:
        cal_sql = """select count(distinct
        (case when target_type in('result_table','table','tdw_table') then target_id end)) dataset_count,
        count(distinct (case when target_type in('raw_data','data_id') then target_id end)) data_source_count,
        count(distinct bk_biz_id) bk_biz_count,
        count(distinct project_id) project_count
        """
    cal_sql += " from ("
    detail_sql = ""
    rt_cal_sql = """ select ${need_fields} from tag_target where active=1 and target_type in('result_table','table')
            ${bus_where_cond} ${source_tag_where_cond} and target_id in(select result_table_id from result_table
            where 1=1 ${rt_where_cond})
            """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        rt_cal_sql += ' and 1=0 '

    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            rt_cal_sql += """ and target_id in(select target_id from tag_target
                    where active=1 and target_type in('result_table','table') ${sys_where_cond})"""
            rt_cal_sql = rt_cal_sql.replace('${sys_where_cond}', sys_where_cond)

    if only_standard:
        rt_cal_sql += get_only_standard_filter_cond('target_id', only_rt=True)

    detail_sql += rt_cal_sql
    detail_sql += """ union all """
    ds_cal_sql = """ select ${need_fields} from tag_target where active=1 and target_type in('raw_data','data_id')
            ${bus_where_cond} ${source_tag_where_cond} and target_id in(select id from access_raw_data
            where 1=1 ${ds_where_cond})"""
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RESULT_TABLE:
        ds_cal_sql += ' and 1=0 '
    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            ds_cal_sql += """ and target_id in(select target_id from tag_target
                        where active=1 and target_type in('raw_data','data_id') ${sys_where_cond})"""
            ds_cal_sql = ds_cal_sql.replace('${sys_where_cond}', sys_where_cond)

    if only_standard:
        ds_cal_sql += get_only_standard_filter_cond('target_id', only_rd=True)

    detail_sql += ds_cal_sql
    # 添加tdw的数据
    tdw_cal_sql = """ select ${need_fields} from tag_target where active=1 and target_type in('tdw_table')
                ${bus_where_cond} ${source_tag_where_cond} and target_id in(select table_id from tdw_table
                where 1=1 ${tdw_where_cond})"""
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        tdw_cal_sql += ' and 1=0 '
    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            tdw_cal_sql += """ and target_id in(select target_id from tag_target
                            where active=1 and target_type in('tdw_table') ${sys_where_cond})"""
            tdw_cal_sql = tdw_cal_sql.replace('${sys_where_cond}', sys_where_cond)

    if only_standard:
        tdw_cal_sql += get_only_standard_filter_cond('target_id', only_rd=True)

    if platform == TYPE_ALL:
        detail_sql += """ union all """
        detail_sql += tdw_cal_sql
    elif platform == TYPE_TDW:
        detail_sql = tdw_cal_sql

    detail_sql = detail_sql.replace('${bus_where_cond}', bus_where_cond)
    detail_sql = detail_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
    detail_sql = detail_sql.replace('${rt_where_cond}', rt_where_cond)
    detail_sql = detail_sql.replace('${ds_where_cond}', ds_where_cond)
    detail_sql = detail_sql.replace('${tdw_where_cond}', tdw_where_cond)

    cal_sql += detail_sql.replace('${need_fields}', 'tag_code data_category,target_type,target_id,bk_biz_id,project_id')
    cal_sql += """)tmp """
    if is_overall_search:
        cal_sql += " group by data_category"

    return cal_sql, detail_sql


def get_all_bizs():
    query_result = MetaApi.get_bizs({}, raw=True)
    result = query_result['result']
    message = query_result['message']
    if not result:
        raise Exception(message)
    data = query_result['data']
    result_dict = {}
    if data:
        for d in data:
            result_dict[d['bk_biz_id']] = d
    return result_dict


def get_single_standard_node_cal_sql(
    sys_where_cond_list,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    standard_version_id_where_cond,
):  # 悬浮在标准节点
    cal_sql = """select count(distinct a1.target_id) dataset_count,
    count(distinct a2.bk_biz_id) bk_biz_count,
    count(distinct a2.project_id) project_count """

    detail_sql = """ from (
    select standard_version_id data_category,data_set_id target_id from dm_task_detail where active=1
    and data_set_type='result_table'
     ${standard_version_id_where_cond} and data_set_id in(select result_table_id from result_table
     where 1=1 ${rt_where_cond})) a1
    left join
    tag_target a2 on a1.target_id=a2.target_id where a2.tag_code=a2.source_tag_code
    ${source_tag_where_cond} ${bus_where_cond} ${sys_where_cond2} """

    sys_where_cond2 = ''
    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            sys_where_cond2 = """ and a2.target_id in(select target_id from tag_target
                            where active=1 and target_type in('result_table','table') ${sys_where_cond})"""
            sys_where_cond2 = sys_where_cond2.replace('${sys_where_cond}', sys_where_cond)

    detail_sql = detail_sql.replace('${rt_where_cond}', rt_where_cond)
    detail_sql = detail_sql.replace('${standard_version_id_where_cond}', standard_version_id_where_cond)
    detail_sql = detail_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
    detail_sql = detail_sql.replace('${bus_where_cond}', bus_where_cond)
    detail_sql = detail_sql.replace('${sys_where_cond2}', sys_where_cond2)

    return cal_sql + detail_sql, "select ${need_fields} " + detail_sql


def get_match_tag(conn, keyword, tag_dimension_list):
    kw_tag_code_list = []
    if keyword:
        tag_id_list = tagaction.query_direct_sql_to_map_list(
            conn, "select code from tag where active=1 and alias='" + tagaction.escape_string(keyword) + "'"
        )
        if tag_id_list:
            for tag_dict in tag_id_list:
                kw_tag_code = tag_dict['code']
                b_code_node = overall_handler2(kw_tag_code, tag_dimension_list, 'category_name')
                if not b_code_node:
                    kw_tag_code_list.append(kw_tag_code)
    return kw_tag_code_list


def judge_tag_type(conn, tag_codes_list):
    tag_type_dict = {}  # 判断tag_code的tag_type类型是属于业务标签还是来源标签
    business_code_list, system_code_list, desc_code_list = [], [], []
    if tag_codes_list:
        query_sql = (
            "select code,tag_type from tag where active=1 " + " and code in ('" + "','".join(tag_codes_list) + "')"
        )
        tag_config = tagaction.query_direct_sql_to_map_list(conn, query_sql)
        if tag_config:
            for tc in tag_config:
                tag_type_dict[tc['code']] = tc['tag_type']
        for tag_code in tag_codes_list:
            if tag_type_dict[tag_code] == 'business':
                business_code_list.append(tag_code)
            elif tag_type_dict[tag_code] == 'system':
                system_code_list.append(tag_code)
            elif tag_type_dict[tag_code] == 'desc':
                desc_code_list.append(tag_code)
    return business_code_list, system_code_list, desc_code_list


def floating_zero_result(result_dict):
    result_dict['data_source_count'] = 0
    result_dict['dataset_count'] = 0
    result_dict['project_count'] = 0
    result_dict['bk_biz_count'] = 0
    result_dict['project_list'] = []
    result_dict['bk_biz_list'] = []


def get_detail_via_erp(result_dict, extra_retrieve_dict):
    results = result_dict['results']
    if not results:
        return
    rt_erp_dict = extra_retrieve_dict['ResultTable']
    rd_erp_dict = extra_retrieve_dict['AccessRawData']
    tdw_erp_dict = extra_retrieve_dict['TdwTable']
    retrieve_args = {}
    rt_starts_list, rd_starts_list, tdw_starts_list = [], [], []
    for data_set_dict in results:
        data_set_type = data_set_dict['data_set_type']
        data_set_id = data_set_dict['data_set_id']
        if data_set_type == 'result_table':
            rt_starts_list.append(data_set_id)
        elif data_set_type == 'raw_data':
            rd_starts_list.append(data_set_id)
        elif data_set_type == 'tdw_table':
            tdw_starts_list.append(data_set_id)

    if rt_starts_list:
        rt_erp_dict['starts'] = rt_starts_list
        retrieve_args['ResultTable'] = rt_erp_dict
    if rd_starts_list:
        rd_erp_dict['starts'] = rd_starts_list
        retrieve_args['AccessRawData'] = rd_erp_dict
    if tdw_starts_list:
        tdw_erp_dict['starts'] = tdw_starts_list
        retrieve_args['TdwTable'] = tdw_erp_dict

    query_result = MetaApi.query_via_erp({'retrieve_args': retrieve_args}, raw=True)
    # print ('query_result:', query_result)
    result = query_result['result']
    message = query_result['message']
    if not result:
        raise Exception(message)

    data = query_result['data']
    erp_res_dict = {}
    if data:
        for val_list in list(data.values()):
            for val_dict in val_list:
                erp_res_dict[str(val_dict['identifier_value'])] = val_dict

    if erp_res_dict:
        for data_set_dict in results:
            data_set_id = data_set_dict['data_set_id']
            erp_data_value = erp_res_dict.get(data_set_id)
            if erp_data_value:
                data_set_dict.update(erp_data_value)


def get_biz_and_project_info(
    conn, detail_sql, result_dict, params, need_detail_type, tdw_dataset_count=0, platform=TYPE_ALL
):  # 返回业务和项目列表
    if need_detail_type == NEED_DATA_SET_ID_DETAIL:  # 数据字典列表查询
        cal_type_list = params.get('cal_type')
        page = params['page']
        page_size = params['page_size']
        start = (page - 1) * page_size
        end = page_size
        data_set_id_detail_sql = (
            detail_sql.replace('${need_fields}', ' target_id,target_type,updated_at ')
            + ' order by updated_at desc limit '
            + str(start)
            + ','
            + str(end)
        )
        if CAL_TYPE_ONLY_STANDARD not in cal_type_list:
            data_set_id_detail_sql = (
                'select t1.*,case when t2.id is null then 0 else 1 end is_standard from(' + data_set_id_detail_sql
            )
            data_set_id_detail_sql += ') t1 left join dm_task_detail t2 on t1.target_id=t2.data_set_id'

        print('data_set_id_detail_sql:', data_set_id_detail_sql)
        data_set_id_result = tagaction.query_direct_sql_to_map_list(conn, data_set_id_detail_sql)
        result_dict['data_set_list'] = data_set_id_result
    # tag节点或standard节点
    elif need_detail_type == NEED_FLOATING_DETAIL or need_detail_type == NEED_STANDARD_FLOATING_DETAIL:
        if platform == TYPE_TDW:
            result_dict['project_list'] = []
            result_dict['bk_biz_list'] = []
        else:
            bk_biz_id_detail_sql = detail_sql.replace('${need_fields}', ' bk_biz_id ')
            bk_biz_id_detail_sql = "select bk_biz_id,count(*) c from(" + bk_biz_id_detail_sql + ") tmp "
            bk_biz_id_detail_sql += " where bk_biz_id is not null group by bk_biz_id order by c desc"
            project_id_detail_sql = detail_sql.replace('${need_fields}', ' project_id ')
            project_id_detail_sql = "select project_id,count(*) c from(" + project_id_detail_sql + ") tmp "
            project_id_detail_sql += " where project_id is not null group by project_id order by c desc"
            print('bk_biz_id_detail_sql:', bk_biz_id_detail_sql)
            print('project_id_detail_sql:', project_id_detail_sql)

            project_info_result = tagaction.query_direct_sql_to_map_list(conn, project_id_detail_sql)
            result_dict['project_list'] = [obj['project_id'] for obj in project_info_result]

            bk_biz_id_result = tagaction.query_direct_sql_to_map_list(conn, bk_biz_id_detail_sql)
            result_dict['bk_biz_list'] = [obj['bk_biz_id'] for obj in bk_biz_id_result]

        if tdw_dataset_count > 0:
            bk_biz_list = result_dict['bk_biz_list']
            if TDW_BK_BIZ_ID not in bk_biz_list:
                bk_biz_list.append(TDW_BK_BIZ_ID)


def get_standard_with_tag_cal_sql(
    sys_where_cond_list, source_tag_where_cond, rt_where_cond, bus_where_cond
):  # 选择了具体的标签的
    rt_cal_sql = """select ${need_fields} from tag_target where active=1 and target_type in('result_table','table')
            ${bus_where_cond} ${source_tag_where_cond}
            and target_id in(select data_set_id from dm_task_detail a,dm_standard_version_config b
            where a.standard_version_id=b.id and b.standard_version_status='online'
            and data_set_type in('result_table','table')
            and data_set_id in(select result_table_id from result_table where 1=1 ${rt_where_cond}))"""
    if sys_where_cond_list:
        for sys_where_cond in sys_where_cond_list:
            rt_cal_sql += """ and target_id in(select target_id from tag_target
                        where active=1 and target_type in('result_table','table') ${sys_where_cond})"""
            rt_cal_sql = rt_cal_sql.replace('${sys_where_cond}', sys_where_cond)

    rt_cal_sql = rt_cal_sql.replace('${bus_where_cond}', bus_where_cond)
    rt_cal_sql = rt_cal_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
    rt_cal_sql = rt_cal_sql.replace('${rt_where_cond}', rt_where_cond)
    return rt_cal_sql.replace('${need_fields}', " count(distinct target_id) dataset_count "), rt_cal_sql.replace(
        '${need_fields}', 'distinct target_id'
    )


def get_recent_standard_rt_result(conn, result_dict, re_st_dataset_count_sql, day_list):
    re_st_dataset_count_list = tagaction.query_direct_sql_to_map_list(conn, re_st_dataset_count_sql)
    parse_recent_standard_result(result_dict, re_st_dataset_count_list, day_list)


def parse_recent_standard_result(result_dict, re_st_dataset_count_list, day_list):
    re_st_dataset_count_result, re_st_dataset_count_sum = index_trend_result(
        day_list, re_st_dataset_count_list, 'standard_dataset_count'
    )

    result_dict['recent_standard_dataset_count_details'] = re_st_dataset_count_result
    result_dict['recent_standard_dataset_count_sum'] = re_st_dataset_count_sum


def parse_standard_result(result_dict):
    recent_standard_dataset_count_details = result_dict['recent_standard_dataset_count_details']
    recent_standard_dataset_count_sum = result_dict['recent_standard_dataset_count_sum']

    recent_dataset_count_details = []
    recent_data_source_count_details = []
    for re_dict in recent_standard_dataset_count_details:
        recent_dataset_count_details.append({'day': re_dict['day'], 'dataset_count': re_dict['standard_dataset_count']})
        recent_data_source_count_details.append({'day': re_dict['day'], 'data_source_count': 0})

    result_dict['recent_dataset_count_details'] = recent_dataset_count_details
    result_dict['recent_dataset_count_sum'] = recent_standard_dataset_count_sum

    result_dict['recent_data_source_count_details'] = recent_data_source_count_details
    result_dict['recent_data_source_count_sum'] = 0


def get_recent_rt_result(conn, result_dict, re_dataset_count_sql, day_list):
    re_dataset_count_list = tagaction.query_direct_sql_to_map_list(conn, re_dataset_count_sql)
    parse_recent_rt_result(result_dict, re_dataset_count_list, day_list)


def parse_recent_rt_result(result_dict, re_dataset_count_list, day_list):
    rt_index_name = 'dataset_count'
    re_dataset_count_result, re_dataset_count_sum = index_trend_result(day_list, re_dataset_count_list, rt_index_name)
    if 'recent_dataset_count_details' in result_dict:
        result_dict['recent_dataset_count_sum'] += re_dataset_count_sum
        recent_dataset_count_details = result_dict['recent_dataset_count_details']
        for i, val_dict in enumerate(recent_dataset_count_details):
            val_dict[rt_index_name] += re_dataset_count_result[i][rt_index_name]
    else:
        result_dict['recent_dataset_count_details'] = re_dataset_count_result
        result_dict['recent_dataset_count_sum'] = re_dataset_count_sum


def get_recent_ds_result(conn, result_dict, re_datasource_count_sql, day_list):
    re_datasource_list = tagaction.query_direct_sql_to_map_list(conn, re_datasource_count_sql)
    parse_recent_ds_result(result_dict, re_datasource_list, day_list)


def parse_recent_ds_result(result_dict, re_datasource_list, day_list):
    re_datasource_count_result, re_datasource_count_sum = index_trend_result(
        day_list, re_datasource_list, 'data_source_count'
    )

    result_dict['recent_data_source_count_details'] = re_datasource_count_result
    result_dict['recent_data_source_count_sum'] = re_datasource_count_sum


def get_other_node_with_keyword_sql(
    sys_where_cond, bus_where_cond, source_tag_where_cond, rt_where_cond, ds_where_cond, keyword
):  # 有查询条件
    _, data_mart_detail_sql = get_optimize_floating_index_count(
        sys_where_cond, None, None, False, '', source_tag_where_cond, '', ''
    )  # [数据集市]节点的数据集
    data_mart_detail_sql = data_mart_detail_sql.replace(
        '${need_fields}', ' target_type,target_id,bk_biz_id,project_id '
    )
    data_mart_detail_sql = data_mart_detail_sql + " and tag_code='metric_domain' "
    print('other_with_keyword_data_mart_detail_sql:', data_mart_detail_sql)
    # 打上了标签的数据
    tag_detail_sql = ''
    rt_cal_sql = """ select target_id from tag_target where active=1 and target_type in('result_table','table')
    and tag_code='metric_domain' ${source_tag_where_cond} and target_id in(select result_table_id from result_table
    where 1=1 ${rt_where_cond})"""

    tag_detail_sql += rt_cal_sql
    tag_detail_sql += """ union all """
    ds_cal_sql = """ select target_id from tag_target where active=1 and target_type in('raw_data','data_id')
    and tag_code='metric_domain'
    ${source_tag_where_cond} and target_id in(select id from access_raw_data where 1=1 ${ds_where_cond})"""

    tag_detail_sql += ds_cal_sql
    tag_detail_sql = tag_detail_sql.replace('${rt_where_cond}', rt_where_cond)
    tag_detail_sql = tag_detail_sql.replace('${ds_where_cond}', ds_where_cond)
    tag_detail_sql = tag_detail_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
    print('--other_tag_detail_sql:', tag_detail_sql)

    cal_sql = """select
            count(t1.target_id) total_count,
            count(case when t1.target_type in('result_table','table') then t1.target_id end) dataset_count,
            count(case when t1.target_type in('raw_data','data_id') then t1.target_id end) data_source_count,
            count(distinct t1.bk_biz_id) bk_biz_count,count(distinct t1.project_id) project_count """
    tmp_sql = ''
    tmp_sql += " from ("
    tmp_sql += data_mart_detail_sql
    tmp_sql += ") t1 left join ("
    tmp_sql += tag_detail_sql
    tmp_sql += ") t2 on t1.target_id=t2.target_id where t2.target_id is null"
    cal_sql += tmp_sql
    detail_sql = 'select t1.* ' + tmp_sql
    print('other_with_keyword_cal_sql:', cal_sql)

    return cal_sql, detail_sql


def get_other_node_sql(
    sys_where_cond,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    tdw_where_cond,
    keyword,
    bk_biz_id,
    project_id,
    data_set_type=TYPE_ALL,
    need_detail_type=NEED_DEFAULT_DETAIL,
    platform=TYPE_ALL,
):  # 计算[其他]节点
    cal_sql = """select
    count(t1.target_id) total_count,
    count(case when t1.target_type in('result_table','table','tdw_table') then t1.target_id end) dataset_count,
    count(case when t1.target_type in('raw_data','data_id') then t1.target_id end) data_source_count,
    count(distinct t1.bk_biz_id) bk_biz_count,count(distinct t1.project_id) project_count """
    tmp_sql = """ from(
    select  'result_table' as target_type,result_table_id as target_id,bk_biz_id,project_id,updated_at
    from result_table where 1=1 """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        tmp_sql += ' and 1=0 '
    tmp_sql += """ ${rt_where_cond}
    and result_table_id not in(select target_id from tag_target
    where active=1 ${bk_biz_id_cond} and target_type in('result_table','table') and tag_code in('metric_domain')
    and target_id in(select result_table_id from result_table where 1=1  ${rt_where_cond}))
    union all
    select  'raw_data' as target_type,concat(id,'') target_id,bk_biz_id,null project_id,updated_at
    from access_raw_data where 1=1 """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RESULT_TABLE:
        tmp_sql += ' and 1=0 '
    tmp_sql += """ ${ds_where_cond}
     and concat(id,'') not in(select target_id  from tag_target
     where active=1 ${bk_biz_id_cond} and target_type in('raw_data','data_id')
     and tag_code in('metric_domain') and target_id in(
    select concat(id,'') from access_raw_data where 1=1 ${ds_where_cond}
    ))"""

    tdw_cal_sql = """select  ${need_fields}  from tdw_table where 1=1 """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        tdw_cal_sql += ' and 1=0 '
    tdw_cal_sql += """ ${tdw_where_cond}
         and table_id not in(select target_id  from tag_target where active=1 ${bk_biz_id_cond}
         and target_type in('tdw_table') and tag_code in('metric_domain') and target_id in(
        select table_id from tdw_table where 1=1 ${tdw_where_cond}
        ))"""
    tdw_detail_sql = tdw_cal_sql.replace(
        '${need_fields}', " 'tdw_table' as target_type,table_id as target_id,bk_biz_id,null project_id,updated_at "
    )

    tdw_cal_sql = tdw_cal_sql.replace('${need_fields}', " count(*) dataset_count ")
    detail_sql = 'select t1.* ' + tmp_sql
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and platform == TYPE_ALL:
        detail_sql += ' union all '
        detail_sql += tdw_detail_sql
    elif need_detail_type == NEED_DATA_SET_ID_DETAIL and platform == TYPE_TDW:
        detail_sql = 'select t1.* from(' + tdw_detail_sql

    detail_sql += ")t1"

    tmp_sql += """)t1"""
    tmp_sql = replace_other_params(tmp_sql, rt_where_cond, ds_where_cond, tdw_where_cond)
    detail_sql = replace_other_params(detail_sql, rt_where_cond, ds_where_cond, tdw_where_cond)
    tdw_cal_sql = replace_other_params(tdw_cal_sql, rt_where_cond, ds_where_cond, tdw_where_cond)
    cal_sql += tmp_sql
    return cal_sql, detail_sql, tdw_cal_sql


def replace_other_params(tmp_sql, rt_where_cond, ds_where_cond, tdw_where_cond):
    tmp_sql = tmp_sql.replace('${rt_where_cond}', rt_where_cond)
    tmp_sql = tmp_sql.replace('${ds_where_cond}', ds_where_cond)
    tmp_sql = tmp_sql.replace('${bk_biz_id_cond}', exclude_bk_biz_id_cond())
    tmp_sql = tmp_sql.replace('${tdw_where_cond}', tdw_where_cond)
    return tmp_sql


def get_optimize_search_sql(
    sys_where_cond, bk_biz_id, project_id
):  # keyword为空,不需要根据内容过滤result_table表和access_raw_data表的sql优化
    cal_sql = """select tag_code data_category,count(distinct target_id) dataset_count from tag_target where active=1
    and target_type in('result_table','table','raw_data','data_id') ${bus_where_cond} ${source_tag_where_cond} """
    if bk_biz_id is not None:
        cal_sql += ' and bk_biz_id=' + str(bk_biz_id)
    if project_id:
        cal_sql += ' and project_id=' + str(project_id)
    if sys_where_cond:
        cal_sql += """ and target_id in(select target_id from tag_target
            where target_type in('result_table','table','raw_data','data_id') ${sys_where_cond})"""
        cal_sql = cal_sql.replace('${sys_where_cond}', sys_where_cond)
    cal_sql += " group by data_category"
    return cal_sql


def data_mart_node_count(
    sys_where_cond,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    tdw_where_cond,
    only_standard=False,
    data_set_type=TYPE_ALL,
    need_detail_type=NEED_DEFAULT_DETAIL,
    platform=TYPE_ALL,
):  # 数据集市节点的计算
    cal_sql = """select
    count(target_id) total_count,
    count(case when target_type in('result_table','table','tdw_table') then target_id end) dataset_count,
    count(case when target_type in('raw_data','data_id') then target_id end) data_source_count,
    count(distinct bk_biz_id) bk_biz_count,count(distinct project_id) project_count
    """
    detail_sql = ''
    cal_sql += """ from("""
    rt_cal_sql = """select ${need_fields} from result_table where 1=1 ${rt_where_cond}
         """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        rt_cal_sql += ' and 1=0 '
    rt_cal_sql = rt_cal_sql.replace('${rt_where_cond}', rt_where_cond)
    if only_standard:
        rt_cal_sql += get_only_standard_filter_cond('result_table_id', only_rt=True)
    rt_detail_sql = rt_cal_sql
    rt_cal_sql = rt_cal_sql.replace(
        '${need_fields}', " 'result_table' as target_type,result_table_id as target_id,bk_biz_id,project_id,updated_at "
    )

    detail_sql += rt_cal_sql
    detail_sql += ' union all '
    ds_cal_sql = """ select ${need_fields} from access_raw_data where 1=1 ${ds_where_cond}
         """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RESULT_TABLE:
        ds_cal_sql += ' and 1=0 '
    if only_standard:
        ds_cal_sql += get_only_standard_filter_cond("id", only_rd=True)

    ds_cal_sql = ds_cal_sql.replace('${ds_where_cond}', ds_where_cond)
    ds_detail_sql = ds_cal_sql
    ds_cal_sql = ds_cal_sql.replace(
        '${need_fields}', " 'raw_data' as target_type,concat(id,'') target_id,bk_biz_id,null project_id,updated_at "
    )

    detail_sql += ds_cal_sql

    # 添加tdw的数据
    # detail_sql += ' union all '
    tdw_cal_sql = """ select ${need_fields} from tdw_table where 1=1 ${tdw_where_cond}
             """
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and data_set_type == TYPE_RAW_DATA:
        tdw_cal_sql += ' and 1=0 '
    if only_standard:
        tdw_cal_sql += ' and 1=0 '

    tdw_cal_sql = tdw_cal_sql.replace('${tdw_where_cond}', tdw_where_cond)
    tdw_detail_sql = tdw_cal_sql
    tmp_tdw_detail_sql = tdw_cal_sql.replace(
        '${need_fields}', " 'tdw_table' as target_type,table_id target_id,bk_biz_id,null project_id,updated_at "
    )

    tdw_cal_sql = tdw_cal_sql.replace('${need_fields}', " count(*) dataset_count ")

    cal_sql += detail_sql
    cal_sql += ')tmp'
    if need_detail_type == NEED_DATA_SET_ID_DETAIL and platform == TYPE_ALL:
        detail_sql += ' union all '
        detail_sql += tmp_tdw_detail_sql
    elif need_detail_type == NEED_DATA_SET_ID_DETAIL and platform == TYPE_TDW:
        detail_sql = tmp_tdw_detail_sql

    return cal_sql, detail_sql, rt_detail_sql, ds_detail_sql, tdw_cal_sql, tdw_detail_sql


def parse_tag_codes_list(tag_codes_list):
    if tag_codes_list:
        parse_result_list = []
        for tag_code in tag_codes_list:
            if '|' in tag_code:
                parse_result_list.extend(tag_code.split('|'))
            else:
                parse_result_list.append(tag_code)

        return list(set(parse_result_list))
    else:
        return tag_codes_list


def gen_filter_tag_sql(code_list):
    where_cond = ''
    if code_list:
        where_cond += ' and tag_code in ('
        for sys_code in code_list:
            where_cond += "'" + sys_code + "',"
        where_cond = where_cond[: len(where_cond) - 1] + ")"
    return where_cond


def get_data_set_id_cond(keyword, bk_biz_id, project_id, rt_where_cond):
    data_set_id_where_cond = ''
    data_set_id_where_cond += ' and data_set_id in(select result_table_id from result_table where 1=1 ${rt_where_cond})'
    data_set_id_where_cond = data_set_id_where_cond.replace('${rt_where_cond}', rt_where_cond)
    return data_set_id_where_cond


def get_standard_node_cal_sql_v2(
    keyword,
    bk_biz_id,
    project_id,
    sys_where_cond_list,
    bus_where_cond,
    source_tag_where_cond,
    rt_where_cond,
    ds_where_cond,
    platform=TYPE_ALL,
):
    platform_cond = ''
    if platform == TYPE_TDW:
        platform_cond = ' and 1=0 '
    if not sys_where_cond_list and source_tag_where_cond == '':
        data_set_id_where_cond = get_data_set_id_cond(keyword, bk_biz_id, project_id, rt_where_cond)
        cal_sql = """select t1.*,(case when t2.dataset_count is null then 0 else t2.dataset_count end) dataset_count,
        (case when t2.all_count is null then 0 else t2.all_count end) all_count from(
        select 0 as is_selected,1 as kpath,'standard' as me_type,b.standard_id,
        b.id standard_version_id,concat(b.id,'') as category_name,
        a.standard_name as category_alias,a.description,a.category_id,c.tag_code parent_code,b.standard_version_status
        from dm_standard_config a,dm_standard_version_config b,tag_target c   where b.standard_version_status='online'
        and a.id=b.standard_id and a.id=c.target_id and a.active=1   and c.active=1 and c.active=1
        and c.tag_code=c.source_tag_code and c.target_type='standard') t1
        left join
        (
        select standard_version_id data_category,count(distinct data_set_id) dataset_count,count(data_set_id) all_count
        from dm_task_detail where active=1 and data_set_type='result_table'
        ${data_set_id_where_cond} ${platform_cond}  group by standard_version_id
        ) t2
        on t1.standard_version_id=t2.data_category"""
        cal_sql = cal_sql.replace('${rt_where_cond}', rt_where_cond)
        cal_sql = cal_sql.replace('${data_set_id_where_cond}', data_set_id_where_cond)
        cal_sql = cal_sql.replace('${platform_cond}', platform_cond)
        return cal_sql
    else:
        cal_sql = """select t1.*,(case when t2.dataset_count is null then 0 else t2.dataset_count end) dataset_count,
        (case when t2.all_count is null then 0 else t2.all_count end) all_count from(
        select 0 as is_selected,1 as kpath,'standard' as me_type,b.standard_id,
        b.id standard_version_id,concat(b.id,'') as category_name,
        a.standard_name as category_alias,a.description,a.category_id,c.tag_code parent_code,b.standard_version_status
        from dm_standard_config a,dm_standard_version_config b,tag_target c   where b.standard_version_status='online'
        and a.id=b.standard_id and a.id=c.target_id and a.active=1   and c.active=1 and c.active=1
        and c.tag_code=c.source_tag_code and c.target_type='standard') t1
        left join
        (select a1.data_category,a2.tag_code as parent_code ,count(distinct a1.target_id) dataset_count,
        count(a1.target_id) all_count from (
        select standard_version_id data_category,data_set_id target_id from dm_task_detail
        where active=1 and data_set_type='result_table' ${platform_cond}
        and data_set_id in(select result_table_id from result_table where 1=1 ${rt_where_cond} ) ) a1
        left join tag_target a2 on a1.target_id=a2.target_id
        where a2.tag_code=a2.source_tag_code and active=1 and tag_type='business' ${a2_bk_biz_id_cond}
        ${source_tag_where_cond} ${bus_where_cond} ${sys_where_cond2}
        group by a1.data_category,a2.tag_code) t2
        on t1.standard_version_id=t2.data_category and t1.parent_code=t2.parent_code"""

        sys_where_cond2 = ''
        if sys_where_cond_list:
            for sys_where_cond in sys_where_cond_list:
                sys_where_cond2 = """ and a2.target_id in(select target_id from tag_target
                            where active=1 and target_type in('result_table','table') ${sys_where_cond})"""
                sys_where_cond2 = sys_where_cond2.replace('${sys_where_cond}', sys_where_cond)

        cal_sql = cal_sql.replace('${a2_bk_biz_id_cond}', exclude_bk_biz_id_cond('a2'))
        cal_sql = cal_sql.replace('${rt_where_cond}', rt_where_cond)
        cal_sql = cal_sql.replace('${ds_where_cond}', ds_where_cond)
        cal_sql = cal_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
        cal_sql = cal_sql.replace('${bus_where_cond}', bus_where_cond)
        cal_sql = cal_sql.replace('${sys_where_cond2}', sys_where_cond2)
        cal_sql = cal_sql.replace('${platform_cond}', platform_cond)

        return cal_sql


def get_standard_node_cal_sql(sys_where_cond, bus_where_cond, source_tag_where_cond, rt_where_cond, ds_where_cond):
    cal_sql = """select t1.*,(case when t2.dataset_count is null then 0 else t2.dataset_count end) dataset_count from(
    select 0 as is_selected,1 as kpath,'standard' as me_type,b.standard_id,b.id standard_version_id,
    concat(b.id,'') as category_name,
    a.standard_name as category_alias,a.description,a.category_id,c.tag_code parent_code,b.standard_version_status
    from dm_standard_config a,dm_standard_version_config b,tag_target c   where b.standard_version_status='online'
    and a.id=b.standard_id and a.id=c.target_id and a.active=1   and c.active=1 and c.active=1
    and c.tag_code=c.source_tag_code and c.target_type='standard') t1
    left join
    (select a1.data_category,a2.tag_code as parent_code ,count(distinct a1.target_id) dataset_count from (
    select standard_version_id data_category,data_set_id target_id from dm_task_detail
    where active=1 and data_set_type='result_table'
    and data_set_id in(select result_table_id from result_table where 1=1 ${rt_where_cond} )
    union all
    select standard_version_id data_category,data_set_id target_id from dm_task_detail
    where active=1 and data_set_type='raw_data'
    and data_set_id in(select id from access_raw_data where 1=1  ${ds_where_cond} )) a1
    left join tag_target a2 on a1.target_id=a2.target_id
    where a2.tag_code=a2.source_tag_code and active=1 and tag_type='business'
    ${source_tag_where_cond} ${bus_where_cond} ${sys_where_cond2}
    group by a1.data_category,a2.tag_code) t2
    on t1.standard_version_id=t2.data_category and t1.parent_code=t2.parent_code"""

    sys_where_cond2 = ''
    if sys_where_cond:
        sys_where_cond2 = """ and a2.target_id in(select target_id from tag_target
                    where active=1 and target_type in('result_table','table') ${sys_where_cond})"""
        sys_where_cond2 = sys_where_cond2.replace('${sys_where_cond}', sys_where_cond)

    cal_sql = cal_sql.replace('${rt_where_cond}', rt_where_cond)
    cal_sql = cal_sql.replace('${ds_where_cond}', ds_where_cond)
    cal_sql = cal_sql.replace('${source_tag_where_cond}', source_tag_where_cond)
    cal_sql = cal_sql.replace('${bus_where_cond}', bus_where_cond)
    cal_sql = cal_sql.replace('${sys_where_cond2}', sys_where_cond2)

    return cal_sql


def delete_not_kpath_node(tree_list):
    for index in range(len(tree_list) - 1, -1, -1):  # 从后向前删除
        node_dict = tree_list[index]
        kpath = node_dict['kpath']
        if kpath == 0:
            del tree_list[index]
        else:
            sub_list = node_dict.get('sub_list')
            if sub_list:
                delete_not_kpath_node(sub_list)


def add_virtual_other_node(show_list):
    for show_dict in show_list:
        cal_virtual_other_node(show_dict)


def cal_virtual_other_node(node_dict):
    sub_list = node_dict.get('sub_list')
    dataset_count = node_dict.get('all_count', 0)
    category_name = node_dict['category_name']
    category_alias = node_dict['category_alias']
    category_id = node_dict['category_id']
    if sub_list:
        vir_other_name = category_alias + _('-其他')
        vir_category_name = VIRTUAL_OTHER_NODE_PRE + category_name
        me_type = CAL_TYPE_TAG
        if sub_list[0]['me_type'] == CAL_TYPE_STANDARD:
            vir_other_name = _('非标准数据')
            vir_category_name = VIRTUAL_OTHER_NOT_STANDARD_NODE_PRE + category_name

        sub_dataset_count_sum = 0
        for sub_node_dict in sub_list:
            sub_category_name = sub_node_dict['category_name']
            if sub_category_name.startswith(VIRTUAL_OTHER_NODE_PRE):
                break
            sub_dataset_count_sum += sub_node_dict.get('all_count', 0)
        if dataset_count - sub_dataset_count_sum > 0:
            sub_list.append(
                {
                    "tag_type": "business",
                    "category_alias": vir_other_name,
                    "description": OTHER_STR,
                    "icon": None,
                    "is_selected": 0,
                    "kpath": 1,
                    "sync": 1,
                    "sub_list": [],
                    "parent_id": category_id,
                    "parent_code": category_name,
                    "me_type": me_type,
                    "dataset_count": dataset_count - sub_dataset_count_sum,
                    "category_id": -2,
                    "seq_index": 100,
                    "category_name": vir_category_name,
                }
            )

        for sub_node_dict in sub_list:
            cal_virtual_other_node(sub_node_dict)


def acculate_tree_node(tree_list, id_name):  # 累计各个父节点的值
    need_dict = {}  # 只是为了传参，在这里不需要
    for node_dict in tree_list:  # 累计各个节点的dataset_count的值
        sum_parent_node_dataset_counts(node_dict, need_dict, True, id_name)


def overall_handler(result1_list, tree_list, is_accumulate, id_name):  # 累计各个父节点的值并找到需要的节点
    need_dict = {}
    if result1_list:
        for tag1_obj in result1_list:
            tag_id = tag1_obj['tag_id']
            need_dict[tag_id] = {}

    for node_dict in tree_list:  # 累计各个节点的dataset_count的值
        sum_parent_node_dataset_counts(node_dict, need_dict, is_accumulate, id_name)  # 顺便找出需要的数据

    del result1_list[:]  # 清空
    for dict_val in list(need_dict.values()):
        result1_list.append(dict_val)

    result1_list.sort(key=lambda l: (l['dataset_count']), reverse=True)
    return result1_list


def get_other_tag(result1_list, result2_list, result2_map, top):
    tmp_other_top_list = []  # 为了做排序
    tmp_other_list = []
    result_other_obj = {}
    for tag3_obj in result1_list:  # 其他标签
        tag_id = tag3_obj['tag_id']
        tmp_sub_list, one_sub_list = get_sub_leaf(tag_id, result2_list, result2_map)
        tmp_other_top_list.append(tag3_obj)
        tmp_other_top_list.extend(tmp_sub_list)
        sub_list = []
        for one_sub_obj in one_sub_list:  # 直接子节点
            one_sub_tag_id = one_sub_obj['tag_id']
            tmp2_sub_list, _ = get_sub_leaf(one_sub_tag_id, result2_list, result2_map)
            one_sub_obj['sub_list'] = tmp2_sub_list
            sub_list.append(one_sub_obj)
        tag3_obj['sub_list'] = sub_list
        tmp_other_list.append(tag3_obj)

    tmp_other_top_list.sort(key=lambda l: (l['dataset_count']), reverse=True)  # 其他所有的标签一起排序

    result_other_top_list = []
    for tmp_other_obj in tmp_other_top_list[:top]:
        copy_other_obj = tmp_other_obj.copy()
        copy_other_obj.pop('sub_list', None)
        result_other_top_list.append(copy_other_obj)

    result_other_obj['sub_top_list'] = result_other_top_list
    result_other_obj['sub_list'] = tmp_other_list
    return result_other_obj


def handler_sub_leaf(node_obj, result_sub_leaf):  # 处理一个节点下的所有的子节点的数据
    sub_list = node_obj['sub_list']
    if sub_list:
        for sub_obj in sub_list:
            result_sub_leaf.append(sub_obj)
            handler_sub_leaf(sub_obj, result_sub_leaf)


def tile_all_sub_leaf(node_obj, result_sub_leaf):  # 平铺一个节点下的所有的子节点的数据,不包括它自己
    sub_list = node_obj['sub_list']
    if sub_list:
        for sub_obj in sub_list:
            result_sub_leaf.append(sub_obj)
            tile_all_sub_leaf(sub_obj, result_sub_leaf)


def get_sub_leaf(parent_tag_id, all_tag_list, all_tag_map):  # 把对应tag_id下的节点平铺排序
    tmp_sub_leaf = []
    result_sub_leaf = []
    one_sub_list = []  # 直接子节点
    for tag_obj in all_tag_list:
        tag_id = tag_obj['tag_id']
        parent_id = tag_obj['parent_id']
        if parent_id == parent_tag_id:
            tmp_sub_leaf.append(all_tag_map[tag_id])

    one_sub_list.extend(tmp_sub_leaf)
    result_sub_leaf.extend(tmp_sub_leaf)
    while len(tmp_sub_leaf) > 0:
        tmp2_sub_leaf = []
        tmp2_sub_leaf.extend(tmp_sub_leaf)
        del tmp_sub_leaf[:]
        for tmp2_obj in tmp2_sub_leaf:
            tmp_parent_tag_id = tmp2_obj['tag_id']
            for tag_obj in all_tag_list:
                tag_id = tag_obj['tag_id']
                parent_id = tag_obj['parent_id']
                if parent_id == tmp_parent_tag_id:
                    tmp_sub_leaf.append(all_tag_map[tag_id])

        result_sub_leaf.extend(tmp_sub_leaf)

    result_sub_leaf.sort(key=lambda l: (l['me_dataset_count']), reverse=True)
    return result_sub_leaf, one_sub_list


def parse_field_to_boolean(obj, field_name):
    if isinstance(obj, list):
        for single in obj:
            if single[field_name] == 1:
                single[field_name] = True
            elif single[field_name] == 0:
                single[field_name] = False
    elif isinstance(obj, dict):
        if obj[field_name] == 1:
            obj[field_name] = True
        elif obj[field_name] == 0:
            obj[field_name] = False
    return obj


def parse_tag_config_result(ret_result):
    parent_code_dict = {}  # parent_code:value_list
    if ret_result:
        for ret_dict in ret_result:
            parent_code = ret_dict['parent_code']
            if parent_code in parent_code_dict:
                parent_code_dict[parent_code].append(ret_dict)
            else:
                parent_code_dict[parent_code] = [ret_dict]
    return parent_code_dict


def parse_data_category_result(ret_result):
    parse_result = []
    tmp_result = []  # 找出有效可展示的数据
    not_visible_dict = {}  # visible=0的id与parent_id,在数据地图不过滤
    if ret_result:
        ret_result.sort(key=lambda l: (l['parent_id'], l['seq_index']), reverse=False)
        for ret_dict in ret_result:
            active = ret_dict['active']
            if not active:  # false
                continue
            tmp_result.append(ret_dict)

    if tmp_result:  # 当父节点不可见的时候，子节点继续向上挂,一直挂到0
        for ret_dict in tmp_result:
            parent_id = ret_dict['parent_id']
            if parent_id in not_visible_dict:
                p_parent_id = not_visible_dict[parent_id]
                while p_parent_id in not_visible_dict:
                    p_parent_id = not_visible_dict[p_parent_id]
                ret_dict['parent_id'] = p_parent_id

    if tmp_result:
        parent_id_dict = {}  # parent_id:value_list

        for ret_dict in tmp_result:
            parent_id = ret_dict['parent_id']
            if parent_id in parent_id_dict:
                parent_id_dict[parent_id].append(ret_dict)
            else:
                parent_id_dict[parent_id] = [ret_dict]

        for ret_dict in tmp_result:
            id_val = ret_dict['id']
            parent_id = ret_dict['parent_id']
            ret_dict['sub_list'] = parent_id_dict[id_val] if id_val in parent_id_dict else []
            if parent_id == 0:
                parse_result.append(ret_dict)

    return parse_result


def hang_to_tree_node(node_dict, parent_code_dict, has_standard=False, tree_list=None):
    if is_standard_node(node_dict):
        node_dict['sub_list'] = []
        return
    category_name = node_dict['category_name']
    sub_list = node_dict.get('sub_list', [])
    if category_name in parent_code_dict:
        standard_sub_list = parent_code_dict[category_name]
        sub_list.extend(standard_sub_list)
        node_dict['sub_list'] = sub_list
        if has_standard:
            for standard_dict in standard_sub_list:
                if standard_dict['dataset_count'] > 0:
                    node_dict['has_standard'] = 1
                    break
            if 'has_standard' in node_dict:
                parent_code = node_dict['parent_code']
                while parent_code is not None:
                    node = overall_handler2(parent_code, tree_list, 'category_name')
                    node['has_standard'] = 1
                    parent_code = node['parent_code']

    if sub_list:
        for sub_node_dict in sub_list:
            hang_to_tree_node(sub_node_dict, parent_code_dict, has_standard=has_standard, tree_list=tree_list)


def sum_parent_node_dataset_counts(node_dict, need_dict, is_accumulate, id_name):  # 子节点累加
    sum_count = 0
    sub_list = node_dict.get('sub_list')
    dataset_count = node_dict['dataset_count']
    tag_id = node_dict[id_name]
    if tag_id in need_dict:
        need_dict[tag_id] = node_dict
    if sub_list:
        for sub_node_dict in sub_list:
            sum_count += sum_parent_node_dataset_counts(sub_node_dict, need_dict, is_accumulate, id_name)
    sum_count += dataset_count
    return sum_count


def overall_handler2(child_tag_code, tree_list, id_name):  # 找到需要的节点
    need_dict = {child_tag_code: {}}
    for node_dict in tree_list:
        find_need_node(node_dict, need_dict, id_name)  # 顺便找出需要的数据

    return need_dict[child_tag_code]


def find_need_node(node_dict, need_dict, id_name):
    sub_list = node_dict.get('sub_list')
    tag_id = node_dict[id_name]
    if tag_id in need_dict:
        need_dict[tag_id] = node_dict
    if sub_list:
        for sub_node_dict in sub_list:
            find_need_node(sub_node_dict, need_dict, id_name)


def add_dm_pos_field(node_dict, dm_pos_val):  # 添加位置字段
    sub_list = node_dict.get('sub_list')
    if sub_list:
        for sub_node_dict in sub_list:
            sub_node_dict[VIRTUAL_DM_POS_FIELD] = dm_pos_val
            add_dm_pos_field(sub_node_dict, dm_pos_val)


def add_loc_field(node_dict, loc_val):  # 添加位置字段
    sub_list = node_dict.get('sub_list')
    if sub_list:
        for sub_node_dict in sub_list:
            sub_node_dict[DATAMAP_LOC_FIELD] = loc_val
            add_loc_field(sub_node_dict, loc_val)


def get_all_child_node_info(node, id_name, result_list):  # 输出它的所有子节点的某个id_name字段的值,包括它自己
    sub_list = node.get('sub_list')
    result_list.append(node[id_name])
    if sub_list:
        for sub_node_dict in sub_list:
            get_all_child_node_info(sub_node_dict, id_name, result_list)


def get_all_parent_node_info(parent_code, tree_list, result_list):  # 输出它的所有父节点的某个id_name字段的值,不包括它自己
    while parent_code is not None:
        result_list.append(parent_code)
        node = overall_handler2(parent_code, tree_list, 'category_name')
        if node.get('is_selected') == 0:
            node['is_selected'] = 2
        parent_code = node['parent_code']


def overall_clear(id_list, tree_list, id_name, index_name):  # 清空不需要的节点
    for node_dict in tree_list:
        find_need_node3(node_dict, id_list, id_name, index_name)


def find_need_node3(node_dict, id_list, id_name, index_name):
    sub_list = node_dict.get('sub_list')
    tag_id = node_dict[id_name]
    if tag_id not in id_list:
        node_dict[index_name] = 0
    if sub_list:
        for sub_node_dict in sub_list:
            find_need_node3(sub_node_dict, id_list, id_name, index_name)


def acculate_parent_node(parent_code_list, tree_list, id_name, index_name):  # 累计各个父节点的值
    for parent_code in parent_code_list:
        node_dict = overall_handler2(parent_code, tree_list, id_name)
        if node_dict.get('is_selected') == 0:
            node_dict['is_selected'] = 2
        sub_list = node_dict.get('sub_list')
        sum_count = 0
        me_dataset_count = node_dict.get('me_' + index_name)  # 它本身打标签的数量
        if me_dataset_count is not None:
            sum_count += me_dataset_count
            if sub_list:
                for sub_obj in sub_list:
                    dataset_count = sub_obj[index_name]
                    sum_count += dataset_count

            node_dict[index_name] = sum_count


def sum_parent_node_dataset_counts2(node_dict, id_name, id_list):  # 子节点累加
    sum_count = 0
    sub_list = node_dict.get('sub_list')
    me_dataset_count = node_dict['me_dataset_count']
    if sub_list:
        for sub_node_dict in sub_list:
            sum_count += sum_parent_node_dataset_counts2(sub_node_dict, id_name, id_list)
    sum_count += me_dataset_count
    node_dict['me_dataset_count'] = sum_count
    return sum_count


def parse_index_dict(cal_result_list, key_name, val_name):
    ret_dict = {}
    if cal_result_list:
        for dt in cal_result_list:
            ret_dict[dt[key_name]] = dt[val_name]

    return ret_dict


def assign_node_counts(node_dict, index_count_dict, id_name, index_name):
    if is_standard_node(node_dict):
        return
    sub_list = node_dict.get('sub_list')
    node_dict[index_name] = index_count_dict.get(node_dict[id_name], 0)
    if sub_list:
        for sub_node_dict in sub_list:
            assign_node_counts(sub_node_dict, index_count_dict, id_name, index_name)


def acculate_node(node_dict, *index_names):  # 累计各个节点指定指标的值
    for index_name in index_names:
        sum_node_counts(node_dict, index_name)


def sum_node_counts(node_dict, index_name):  # 子节点累加
    sum_count = 0
    sub_list = node_dict.get('sub_list')
    index_count = node_dict[index_name]
    node_dict['me_' + index_name] = index_count  # 保留它本身的count统计
    if sub_list:
        for sub_node_dict in sub_list:
            sum_count += sum_node_counts(sub_node_dict, index_name)
    sum_count += index_count
    node_dict[index_name] = sum_count
    return sum_count


# 以下是dgraph版本的实现
def gen_data_set_map(data_set_type, only_standard, platform=TYPE_ALL, project_id=None, only_not_standard=None):
    result_list = []
    if platform == TYPE_ALL:  # 全部平台
        if data_set_type == TYPE_ALL:
            result_list.extend([TYPE_RESULT_TABLE, TYPE_TDW, TYPE_RAW_DATA])
        if data_set_type == TYPE_RESULT_TABLE:
            result_list.extend([TYPE_RESULT_TABLE, TYPE_TDW])
        if data_set_type == TYPE_RAW_DATA:
            result_list.extend([TYPE_RAW_DATA])
    elif platform == TYPE_BK_DATA:  # 数据平台
        if data_set_type == TYPE_ALL:
            result_list.extend([TYPE_RESULT_TABLE, TYPE_RAW_DATA])
        if data_set_type == TYPE_RESULT_TABLE:
            result_list.extend([TYPE_RESULT_TABLE])
        if data_set_type == TYPE_RAW_DATA:
            result_list.extend([TYPE_RAW_DATA])
    elif platform == TYPE_TDW:  # TDW平台
        if data_set_type == TYPE_ALL:
            result_list.extend([TYPE_TDW])
        if data_set_type == TYPE_RESULT_TABLE:
            result_list.extend([TYPE_TDW])
    if project_id is not None or only_standard or only_not_standard:
        if TYPE_RESULT_TABLE in result_list:
            result_list = [TYPE_RESULT_TABLE]
        else:
            result_list = result_list[:0]
    return result_list


def gen_filter_uids(final_filter_uids_name, query_uids):
    if final_filter_uids_name:
        final_filter_uids_name += ','
    final_filter_uids_name += query_uids
    return final_filter_uids_name


def dgraph_created_cond(created_by, created_at_start, created_at_end, dataset_type='raw_data'):
    # 只存在创建时间和创建者条件拼接
    cond = '@filter(eq(active, true))' if dataset_type == 'raw_data' else ''
    active_cond = 'eq(active, true) and' if dataset_type == 'raw_data' else ''
    if created_at_start and created_at_end and not created_by:
        cond = '@filter($active_cond ge(created_at,"{}") and le(created_at,"{}"))'.format(
            created_at_start, created_at_end
        )
    elif created_at_start and created_at_end and created_by:
        cond = '@filter($active_cond ge(created_at,"{}") and le(created_at,"{}") and eq(created_by, {}))'.format(
            created_at_start, created_at_end, created_by
        )
    elif created_by:
        cond = '@filter($active_cond eq(created_by, {}))'.format(created_by)
    cond = cond.replace('$active_cond', active_cond)
    return cond


def lifecycle_query_uids(lifecycle_type, rt_query_uids, rd_query_uids):
    # 当涉及按照广度、热度过滤/排序的时候，补充热度、广度的uid集合
    if lifecycle_type:
        rt_query_uids += """
        {
          ~LifeCycle.target {
        """
        rd_query_uids += """
        {
          ~LifeCycle.target {
        """
    if 'range' in lifecycle_type:
        rt_query_uids += """
            LifeCycle.range {
              range_rt_uids as uid
            }
        """
        rd_query_uids += """
            LifeCycle.range {
              range_rd_uids as uid
            }
        """
    if 'heat' in lifecycle_type:
        rt_query_uids += """
            LifeCycle.heat {
              heat_rt_uids as uid
            }
        """
        rd_query_uids += """
            LifeCycle.heat {
              heat_rd_uids as uid
            }
        """
    if 'importance' in lifecycle_type:
        rt_query_uids += """
            LifeCycle.importance {
              importance_rt_uids as uid
            }
        """
        rd_query_uids += """
            LifeCycle.importance {
              importance_rd_uids as uid
            }
        """
    if 'asset_value' in lifecycle_type:
        rt_query_uids += """
            LifeCycle.asset_value {
              asset_value_rt_uids as uid
            }
        """
        rd_query_uids += """
            LifeCycle.asset_value {
              asset_value_rd_uids as uid
            }
        """
    if 'assetvalue_to_cost' in lifecycle_type:
        rt_query_uids += """
            assetvalue_to_cost_rt_uids as uid
        """
        rd_query_uids += """
            assetvalue_to_cost_rd_uids as uid
        """
    if 'storage_capacity' in lifecycle_type:
        rt_query_uids += """
            LifeCycle.cost {
              Cost.capacity {
                storage_capacity_rt_uids as uid
              }
            }
        """
        rd_query_uids += """
            LifeCycle.cost {
              Cost.capacity {
                storage_capacity_rd_uids as uid
              }
            }
        """
    if lifecycle_type:
        rt_query_uids += """
            }
        }
        """
        rd_query_uids += """
            }
        }
        """
    return rt_query_uids, rd_query_uids


def lifecycle_d_query_uids(lifecycle_type, data_set_type, d_query_uids, type):
    if (data_set_type == 'result_table' or data_set_type == 'all') and type == 'result_table':
        if lifecycle_type:
            d_query_uids += """
            {
              ~LifeCycle.target {
            """
        if 'range' in lifecycle_type:
            d_query_uids += """
                LifeCycle.range {
                  range_rt_uids as uid
                }
            """
        if 'heat' in lifecycle_type:
            d_query_uids += """
                LifeCycle.heat {
                  heat_rt_uids as uid
                }
            """
        if 'importance' in lifecycle_type:
            d_query_uids += """
                LifeCycle.importance {
                  importance_rt_uids as uid
                }
            """
        if 'asset_value' in lifecycle_type:
            d_query_uids += """
                LifeCycle.asset_value {
                  asset_value_rt_uids as uid
                }
            """
        if 'assetvalue_to_cost' in lifecycle_type:
            d_query_uids += """
                assetvalue_to_cost_rt_uids as uid
            """
        if 'storage_capacity' in lifecycle_type:
            d_query_uids += """
                LifeCycle.cost {
                  Cost.capacity {
                    storage_capacity_rt_uids as uid
                  }
                }
            """
        if lifecycle_type:
            d_query_uids += """
                }
            }
            """
    elif (data_set_type == 'raw_data' or data_set_type == 'all') and type == 'raw_data':
        if lifecycle_type:
            d_query_uids += """
            {
              ~LifeCycle.target {
            """
        if 'range' in lifecycle_type:
            d_query_uids += """
                LifeCycle.range {
                  range_rd_uids as uid
                }
            """
        if 'heat' in lifecycle_type:
            d_query_uids += """
                LifeCycle.heat {
                  heat_rd_uids as uid
                }
            """
        if 'importance' in lifecycle_type:
            d_query_uids += """
                LifeCycle.importance {
                  importance_rd_uids as uid
                }
            """
        if 'asset_value' in lifecycle_type:
            d_query_uids += """
                LifeCycle.asset_value {
                  asset_value_rd_uids as uid
                }
            """
        if 'assetvalue_to_cost' in lifecycle_type:
            d_query_uids += """
                assetvalue_to_cost_rd_uids as uid
            """
        if 'storage_capacity' in lifecycle_type:
            d_query_uids += """
                LifeCycle.cost {
                  Cost.capacity {
                    storage_capacity_rd_uids as uid
                  }
                }
            """
        if lifecycle_type:
            d_query_uids += """
                }
            }
            """
    return d_query_uids


def get_uids_filtered_lifecycle_metric(
    d_query_uids,
    final_filter_uids_name,
    data_set_type,
    storage_type,
    range_operate,
    range_score,
    heat_operate,
    heat_score,
    importance_operate,
    importance_score,
    asset_value_operate,
    asset_value_score,
    assetvalue_to_cost_operate,
    assetvalue_to_cost,
    storage_capacity_operate,
    storage_capacity,
):
    if storage_type:
        d_query_uids, final_filter_uids_name = filter_dataset_by_storage_type(
            d_query_uids, {}, final_filter_uids_name, storage_type=storage_type
        )
    if is_filter_by_lifecycle(
        range_operate,
        range_score,
        heat_operate,
        heat_score,
        importance_operate,
        importance_score,
        asset_value_operate,
        asset_value_score,
        assetvalue_to_cost_operate,
        assetvalue_to_cost,
        storage_capacity_operate,
        storage_capacity,
    ):
        d_query_uids, final_filter_uids_name = filter_dataset_by_lifecycle(
            d_query_uids,
            {'data_set_type': data_set_type},
            final_filter_uids_name,
            range_operate=range_operate,
            range_score=range_score,
            heat_operate=heat_operate,
            heat_score=heat_score,
            importance_operate=importance_operate,
            importance_score=importance_score,
            asset_value_operate=asset_value_operate,
            asset_value_score=asset_value_score,
            assetvalue_to_cost_operate=assetvalue_to_cost_operate,
            assetvalue_to_cost=assetvalue_to_cost,
            storage_capacity_operate=storage_capacity_operate,
            storage_capacity=storage_capacity,
        )
    return d_query_uids, final_filter_uids_name


def dgraph_data_mart_uids2(
    bk_biz_id,
    project_id,
    keyword,
    platform,
    dgrah_cal_type=DGRAPH_OVERALL_MAP,
    need_only_uids=False,
    need_detail_type=NEED_FLOATING_DETAIL,
    created_by=None,
    data_set_type=TYPE_ALL,
    only_standard=False,
    order_range=None,
    order_heat=None,
    order_assetvalue_to_cost=None,
    order_importance=None,
    order_asset_value=None,
    order_storage_capacity=None,
    created_at_start=None,
    created_at_end=None,
    range_operate=None,
    range_score=None,
    heat_operate=None,
    heat_score=None,
    importance_operate=None,
    importance_score=None,
    asset_value_operate=None,
    asset_value_score=None,
    assetvalue_to_cost_operate=None,
    assetvalue_to_cost=None,
    storage_capacity_operate=None,
    storage_capacity=None,
    storage_type=None,
    only_not_standard=None,
):
    rt_generate_type = """eq(ResultTable.generate_type,"user")"""
    rt_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('ResultTable')
    rd_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('AccessRawData')
    rd_active_cond = 'eq(active, true)'
    tdw_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('TdwTable')
    final_entitys_name = 'data_mart_supported_entitys'

    rt_generate_type_cond = ' AND ' + rt_generate_type

    data_set_type_map = gen_data_set_map(
        data_set_type, only_standard, platform=platform, project_id=project_id, only_not_standard=only_not_standard
    )
    and_all_st_data_set_uids = ''

    only_standard_query = get_all_standard_query()

    rt_created_by_field_name = '$rt_created_by'
    rd_created_by_field_name = '$rd_created_by'
    tdw_created_by_field_name = '$tdw_created_by'
    rt_created_by, rd_created_by, tdw_created_by = '', '', ''

    if created_by:
        rt_created_by = ' AND eq(created_by,"' + created_by + '")'
        rd_created_by = ' AND eq(created_by,"' + created_by + '")'
        tdw_created_by = ' AND eq(created_by,"tdw_' + created_by + '")'

    created_at_cond = (
        dgraph_created_at_cond(created_at_start, created_at_end) if created_at_start and created_at_end else ''
    )
    rd_created_cond = dgraph_created_cond(created_by, created_at_start, created_at_end)
    tdw_created_cond = dgraph_created_cond(created_by, created_at_start, created_at_end, 'tdw')

    if only_standard:
        and_all_st_data_set_uids = ' AND uid(all_st_data_set_uids)'
    elif only_not_standard:
        and_all_st_data_set_uids = ' AND not uid(all_st_data_set_uids)'
    final_filter_uids_name = ''  # 只需要uids时用到

    type_uids_dict = {}  # 每一种表类型符合条件的uid,result_table:key=rt_uids;raw_data:key=rd_uids;tdw_table:key=tdw_uids

    rt_query_uids = """
        rt_uids_dm as var(func:$rt_generate_type)
        @filter($rt_bk_biz_id_cond $rt_created_by $created_at_cond $and_all_st_data_set_uids)
    """
    rt_query_uids = rt_query_uids.replace(rt_created_by_field_name, rt_created_by)

    rd_query_uids = """
        rd_uids_dm as var(func:$rd_bk_biz_id_cond) $created_cond
    """
    rd_query_uids = rd_query_uids.replace('$created_cond', rd_created_cond)

    lifecycle_type = get_lifecycle_type(
        order_range,
        order_heat,
        range_operate,
        range_score,
        heat_operate,
        heat_score,
        importance_operate,
        importance_score,
        asset_value_operate,
        asset_value_score,
        assetvalue_to_cost_operate,
        assetvalue_to_cost,
        storage_capacity_operate,
        storage_capacity,
        order_assetvalue_to_cost,
        order_importance,
        order_asset_value,
        order_storage_capacity,
    )
    rt_query_uids, rd_query_uids = lifecycle_query_uids(lifecycle_type, rt_query_uids, rd_query_uids)

    tdw_query_uids = """
        tdw_uids_dm as var(func:$tdw_bk_biz_id_cond) $created_cond
    """
    tdw_query_uids = tdw_query_uids.replace('$created_cond', tdw_created_cond)
    d_query_uids = ''
    if dgrah_cal_type == DGRAPH_OVERALL_MAP:
        if platform == TYPE_ALL:
            if only_standard or only_not_standard:
                d_query_uids = rt_query_uids
                d_query_uids += """
                $final_entitys_name as var(func:uid(rt_uids_dm))
                """
            else:
                d_query_uids = rt_query_uids + rd_query_uids + tdw_query_uids
                d_query_uids += """
                $final_entitys_name as var(func:uid(rt_uids_dm,rd_uids_dm,tdw_uids_dm))
                """
        elif platform == TYPE_BK_DATA:
            if only_standard or only_not_standard:
                d_query_uids = rt_query_uids
                d_query_uids += """
                $final_entitys_name as var(func:uid(rt_uids_dm))
                """
            else:
                d_query_uids = rt_query_uids + rd_query_uids
                d_query_uids += """
                $final_entitys_name as var(func:uid(rt_uids_dm,rd_uids_dm))
                """
        elif platform == TYPE_TDW:
            d_query_uids = tdw_query_uids
            d_query_uids += """
                            $final_entitys_name as var(func:uid(tdw_uids_dm))
                            """
    elif dgrah_cal_type == DGRAPH_FLOATING:  # 悬浮框查询接口
        need_only_uids_query = ''
        if only_standard or only_not_standard:
            if TYPE_RESULT_TABLE in data_set_type_map:
                d_query_uids += rt_query_uids
                final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'rt_uids_dm')

        else:
            if TYPE_RESULT_TABLE in data_set_type_map:
                d_query_uids += rt_query_uids
                final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'rt_uids_dm')

            if TYPE_RAW_DATA in data_set_type_map:
                d_query_uids += rd_query_uids
                final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'rd_uids_dm')

            if TYPE_TDW in data_set_type_map:
                d_query_uids += tdw_query_uids
                final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tdw_uids_dm')

        if need_only_uids:
            d_query_uids += need_only_uids_query
        else:
            d_query_uids, final_filter_uids_name = get_uids_filtered_lifecycle_metric(
                d_query_uids,
                final_filter_uids_name,
                data_set_type,
                storage_type,
                range_operate,
                range_score,
                heat_operate,
                heat_score,
                importance_operate,
                importance_score,
                asset_value_operate,
                asset_value_score,
                assetvalue_to_cost_operate,
                assetvalue_to_cost,
                storage_capacity_operate,
                storage_capacity,
            )

            rt_cal_query = """
                tmp_rt_count as var(func:uid(rt_uids_dm)){
                rt_p as ResultTable.project
                rt_b as ResultTable.bk_biz
                }
                rt_count(func:uid(tmp_rt_count)){
                  count(uid)
                }
                p_list(func:uid(rt_p)){
                project_id:ProjectInfo.project_id
                }
                """
            rd_cal_query = """
                tmp_rd_count as var(func:uid(rd_uids_dm)){
                rd_b as AccessRawData.bk_biz
                }
                rd_count(func:uid(tmp_rd_count)){
                count(uid)
                }
                """
            tdw_cal_query = """
                tdw_count(func:uid(tdw_uids_dm)){
                count(uid)
                }
                """
            if (
                (range_operate is not None and range_score is not None)
                or (heat_operate is not None and heat_score is not None)
                or (importance_operate is not None and importance_score is not None)
                or (asset_value_operate is not None and asset_value_score is not None)
                or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
                or (storage_capacity_operate is not None and storage_capacity is not None)
                or storage_type
            ) and final_filter_uids_name in d_query_uids:
                rt_cal_query = rt_cal_query.replace(
                    '(func:uid(rt_uids_dm))', '(func:uid(rt_uids_dm))@filter(uid({}))'.format(final_filter_uids_name)
                )
                rd_cal_query = rd_cal_query.replace(
                    '(func:uid(rd_uids_dm))', '(func:uid(rd_uids_dm))@filter(uid({}))'.format(final_filter_uids_name)
                )
                tdw_cal_query = tdw_cal_query.replace(
                    '(func:uid(tdw_uids_dm))', '(func:uid(tdw_uids_dm))@filter(uid({}))'.format(final_filter_uids_name)
                )
            bk_biz_id_list_uids = ''
            # 按照热度、广度过滤

            if only_standard or only_not_standard:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    d_query_uids += rt_cal_query
                    bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
            else:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    d_query_uids += rt_cal_query
                    bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
                if TYPE_RAW_DATA in data_set_type_map:
                    d_query_uids += rd_cal_query
                    bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rd_b')
                if TYPE_TDW in data_set_type_map:
                    d_query_uids += tdw_cal_query

            if bk_biz_id_list_uids:
                d_query_uids += gen_bk_biz_id_list_query(bk_biz_id_list_uids)

    param_names = ''
    if keyword:
        if project_id is not None:  # 只有result_table才有项目
            final_entitys_name += '1'
            with_bk_biz_id_cond = ''
            bk_data_uids, bk_data_param_names = (
                """
                            var(func:regexp(ResultTable.result_table_id,/$keyword/)){
                                 meta_uids1 as uid
                                }
                                var(func:regexp(ResultTable.result_table_name_alias,/$keyword/)){
                                 meta_uids2 as uid
                                }
                                var(func:regexp(ResultTable.description,/$keyword/)){
                                 meta_uids3 as uid
                                }
                            """,
                'meta_uids1,meta_uids2,meta_uids3',
            )
            tmp_d_query_uids = bk_data_uids
            tmp_d_query_uids += """
                $final_entitys_name as var(func:uid($param_names))
                @filter(($rt_project_id_cond $with_bk_biz_id_cond
                $rt_created_by $created_at_cond $rt_generate_type_cond) $and_all_st_data_set_uids)
            """
            if bk_biz_id is not None:
                with_bk_biz_id_cond = " AND eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))
            else:
                with_bk_biz_id_cond = ' AND ' + rt_bk_biz_id_cond

            tmp_d_query_uids = tmp_d_query_uids.replace(
                '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
            )
            tmp_d_query_uids = tmp_d_query_uids.replace('$with_bk_biz_id_cond', with_bk_biz_id_cond)
            d_query_uids = tmp_d_query_uids
            param_names = bk_data_param_names
            # 悬浮框接口，后面确认下是不是可以把和lifecycle有关的去掉
            if dgrah_cal_type == DGRAPH_FLOATING:
                final_filter_uids_name = final_entitys_name
                if need_only_uids:
                    final_filter_uids_name = ''
                    d_query_uids = lifecycle_d_query_uids(lifecycle_type, data_set_type, d_query_uids, 'result_table')

                    d_query_uids += """
                        tmp_rt_count as var(func:uid($final_entitys_name))
                        """
                    final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rt_count')
                    type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
                else:
                    d_query_uids = ''
                    if (platform == TYPE_ALL or platform == TYPE_BK_DATA) and (
                        TYPE_ALL in data_set_type_map or TYPE_RESULT_TABLE in data_set_type_map
                    ):
                        d_query_uids += tmp_d_query_uids
                        if (
                            is_filter_by_lifecycle(
                                range_operate,
                                range_score,
                                heat_operate,
                                heat_score,
                                importance_operate,
                                importance_score,
                                asset_value_operate,
                                asset_value_score,
                                assetvalue_to_cost_operate,
                                assetvalue_to_cost,
                                storage_capacity_operate,
                                storage_capacity,
                            )
                            or storage_type
                        ):
                            if (not storage_type) and is_filter_by_lifecycle(
                                range_operate,
                                range_score,
                                heat_operate,
                                heat_score,
                                importance_operate,
                                importance_score,
                                asset_value_operate,
                                asset_value_score,
                                assetvalue_to_cost_operate,
                                assetvalue_to_cost,
                                storage_capacity_operate,
                                storage_capacity,
                            ):
                                d_query_uids = lifecycle_d_query_uids(
                                    lifecycle_type, data_set_type, d_query_uids, 'result_table'
                                )
                                d_query_uids, final_filter_uids_name = filter_dataset_by_lifecycle(
                                    d_query_uids,
                                    {'data_set_type': data_set_type},
                                    final_filter_uids_name,
                                    range_operate=range_operate,
                                    range_score=range_score,
                                    heat_operate=heat_operate,
                                    heat_score=heat_score,
                                    importance_operate=importance_operate,
                                    importance_score=importance_score,
                                    asset_value_operate=asset_value_operate,
                                    asset_value_score=asset_value_score,
                                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                                    assetvalue_to_cost=assetvalue_to_cost,
                                    storage_capacity_operate=storage_capacity_operate,
                                    storage_capacity=storage_capacity,
                                )

                            elif storage_type and (
                                not is_filter_by_lifecycle(
                                    range_operate,
                                    range_score,
                                    heat_operate,
                                    heat_score,
                                    importance_operate,
                                    importance_score,
                                    asset_value_operate,
                                    asset_value_score,
                                    assetvalue_to_cost_operate,
                                    assetvalue_to_cost,
                                    storage_capacity_operate,
                                    storage_capacity,
                                )
                            ):
                                d_query_uids, final_filter_uids_name = filter_dataset_by_storage_type(
                                    d_query_uids, {}, final_filter_uids_name, storage_type=storage_type
                                )
                            elif storage_type and is_filter_by_lifecycle(
                                range_operate,
                                range_score,
                                heat_operate,
                                heat_score,
                                importance_operate,
                                importance_score,
                                asset_value_operate,
                                asset_value_score,
                                assetvalue_to_cost_operate,
                                assetvalue_to_cost,
                                storage_capacity_operate,
                                storage_capacity,
                            ):
                                d_query_uids = lifecycle_d_query_uids(
                                    lifecycle_type, data_set_type, d_query_uids, 'result_table'
                                )
                                d_query_uids, final_filter_uids_name = filter_dataset_by_storage_type(
                                    d_query_uids, {}, final_filter_uids_name, storage_type=storage_type
                                )
                                d_query_uids, final_filter_uids_name = filter_dataset_by_lifecycle(
                                    d_query_uids,
                                    {'data_set_type': data_set_type},
                                    final_filter_uids_name,
                                    range_operate=range_operate,
                                    range_score=range_score,
                                    heat_operate=heat_operate,
                                    heat_score=heat_score,
                                    importance_operate=importance_operate,
                                    importance_score=importance_score,
                                    asset_value_operate=asset_value_operate,
                                    asset_value_score=asset_value_score,
                                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                                    assetvalue_to_cost=assetvalue_to_cost,
                                    storage_capacity_operate=storage_capacity_operate,
                                    storage_capacity=storage_capacity,
                                )
                            d_query_uids += """tmp_rt_count as var(func:uid($final_filter_uids_name)){"""

                        else:
                            d_query_uids += """tmp_rt_count as var(func:uid($final_entitys_name)){"""
                        d_query_uids += """
                            rt_p as ResultTable.project
                            rt_b as ResultTable.bk_biz
                            }
                            rt_count(func:uid(tmp_rt_count)){
                              count(uid)
                            }
                            p_list(func:uid(rt_p)){
                            project_id:ProjectInfo.project_id
                            }
                            bk_biz_id_list(func:uid(rt_b)){
                            bk_biz_id : BKBiz.id
                            }
                            """
                        if (
                            is_filter_by_lifecycle(
                                range_operate,
                                range_score,
                                heat_operate,
                                heat_score,
                                importance_operate,
                                importance_score,
                                asset_value_operate,
                                asset_value_score,
                                assetvalue_to_cost_operate,
                                assetvalue_to_cost,
                                storage_capacity_operate,
                                storage_capacity,
                            )
                            or storage_type
                        ) and final_filter_uids_name:
                            d_query_uids = d_query_uids.replace('$final_filter_uids_name', final_filter_uids_name)
                    else:
                        final_filter_uids_name = ''
                    type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
        else:
            rt_data_uids, rt_data_param_names = (
                """
                var(func:regexp(ResultTable.result_table_id,/$keyword/)){
                     meta_uids1 as uid
                    }
                    var(func:regexp(ResultTable.result_table_name_alias,/$keyword/)){
                     meta_uids2 as uid
                    }
                    var(func:regexp(ResultTable.description,/$keyword/)){
                     meta_uids3 as uid
                    }
                """,
                'meta_uids1,meta_uids2,meta_uids3',
            )

            rd_data_uids, rd_data_param_names = (
                """
                                var(func:regexp(AccessRawData.raw_data_name,/$keyword/)){
                                 meta_uids4 as uid
                                }
                                var(func:regexp(AccessRawData.description,/$keyword/)){
                                 meta_uids5 as uid
                                }
                                var(func:regexp(AccessRawData.raw_data_alias,/$keyword/)){
                                 meta_uids6 as uid
                                }
                            """,
                'meta_uids4,meta_uids5,meta_uids6',
            )
            if 'data_mart_supported_entitys' not in d_query_uids:
                if data_set_type == TYPE_ALL:
                    final_entitys_name = 'meta_uids1,meta_uids2,meta_uids3,meta_uids4,meta_uids5,meta_uids6'
                elif data_set_type == TYPE_RESULT_TABLE:
                    final_entitys_name = 'meta_uids1,meta_uids2,meta_uids3'
                elif data_set_type == TYPE_RAW_DATA:
                    final_entitys_name = 'meta_uids4,meta_uids5,meta_uids6'

            if keyword.isdigit():
                rd_data_uids += """
                var(func:eq(AccessRawData.id,$keyword)){
                 meta_uids10 as uid
                }
                """
                rd_data_param_names += ',meta_uids10'

            tdw_uids, tdw_param_names = (
                """
                    var(func:regexp(TdwTable.table_id,/$keyword/)){
                     meta_uids7 as uid
                    }
                    var(func:regexp(TdwTable.table_name,/$keyword/)){
                     meta_uids8 as uid
                    }
                    var(func:regexp(TdwTable.table_comment,/$keyword/)){
                     meta_uids9 as uid
                    }""",
                'meta_uids7,meta_uids8,meta_uids9',
            )
            d_query_uids = ''
            if only_standard or only_not_standard:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    d_query_uids += rt_data_uids
                    param_names = gen_filter_uids(param_names, rt_data_param_names)
            else:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    d_query_uids += rt_data_uids
                    param_names = gen_filter_uids(param_names, rt_data_param_names)
                if TYPE_RAW_DATA in data_set_type_map:
                    d_query_uids += rd_data_uids
                    param_names = gen_filter_uids(param_names, rd_data_param_names)
                if TYPE_TDW in data_set_type_map:
                    d_query_uids += tdw_uids
                    param_names = gen_filter_uids(param_names, tdw_param_names)

            # final_filter_uids_name = param_names
            if dgrah_cal_type == DGRAPH_OVERALL_MAP:
                d_query_uids += """
                    $final_entitys_name as var(func:uid($param_names))
                    @filter(($rt_bk_biz_id_cond and $rt_generate_type_cond) or ($rd_bk_biz_id_cond and $rd_active_cond)
                    or $tdw_bk_biz_id_cond) $and_all_st_data_set_uids) """
            elif dgrah_cal_type == DGRAPH_FLOATING:
                final_filter_uids_name = ''
                if need_only_uids:
                    if TYPE_RESULT_TABLE in data_set_type_map:
                        type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
                        d_query_uids += """
                        tmp_rt_count as var(func:uid($rt_data_param_names))
                        @filter($rt_bk_biz_id_cond $rt_created_by $created_at_cond
                        $rt_generate_type_cond $and_all_st_data_set_uids)
                        """
                        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rt_count')
                        # 有key_word时且按照热度/广度排序
                        d_query_uids = lifecycle_d_query_uids(
                            lifecycle_type, data_set_type, d_query_uids, 'result_table'
                        )

                    if TYPE_RAW_DATA in data_set_type_map:
                        type_uids_dict[D_UID_RAW_DATA_KEY] = 'tmp_rd_count'
                        d_query_uids += """
                        tmp_rd_count as var(func:uid($rd_data_param_names))
                        @filter($rd_bk_biz_id_cond and $rd_active_cond $rd_created_by $created_at_cond)
                        """
                        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rd_count')
                        # 有key_word时且按照热度/广度排序
                        d_query_uids = lifecycle_d_query_uids(lifecycle_type, data_set_type, d_query_uids, 'raw_data')

                    if TYPE_TDW in data_set_type_map:
                        type_uids_dict[D_UID_TDW_TABLE_KEY] = 'tdw_count'
                        d_query_uids += """
                        tdw_count as var(func:uid($tdw_param_names))
                        @filter($tdw_bk_biz_id_cond $tdw_created_by $created_at_cond)
                        """
                        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tdw_count')
                else:
                    if storage_type:
                        if (not final_filter_uids_name) and final_entitys_name:
                            final_filter_uids_name = final_entitys_name
                        d_query_uids, final_filter_uids_name = filter_dataset_by_storage_type(
                            d_query_uids, {}, final_filter_uids_name, storage_type=storage_type
                        )
                    if is_filter_by_lifecycle(
                        range_operate,
                        range_score,
                        heat_operate,
                        heat_score,
                        importance_operate,
                        importance_score,
                        asset_value_operate,
                        asset_value_score,
                        assetvalue_to_cost_operate,
                        assetvalue_to_cost,
                        storage_capacity_operate,
                        storage_capacity,
                    ):
                        if (not final_filter_uids_name) and final_entitys_name:
                            final_filter_uids_name = final_entitys_name
                        d_query_uids, final_filter_uids_name = filter_dataset_by_lifecycle(
                            d_query_uids,
                            {'data_set_type': data_set_type},
                            final_filter_uids_name,
                            range_operate=range_operate,
                            range_score=range_score,
                            heat_operate=heat_operate,
                            heat_score=heat_score,
                            importance_operate=importance_operate,
                            importance_score=importance_score,
                            asset_value_operate=asset_value_operate,
                            asset_value_score=asset_value_score,
                            assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                            assetvalue_to_cost=assetvalue_to_cost,
                            storage_capacity_operate=storage_capacity_operate,
                            storage_capacity=storage_capacity,
                        )
                    final_filter_uids_name_tmp = final_filter_uids_name
                    rt_cal_query = """
                    tmp_rt_count as var(func:uid($rt_data_param_names))
                    @filter($rt_bk_biz_id_cond $rt_created_by $created_at_cond $rt_generate_type_cond
                    $and_all_st_data_set_uids $and_final_filter_uids_name){
                    rt_p as ResultTable.project
                    rt_b as ResultTable.bk_biz
                    }
                    rt_count(func:uid(tmp_rt_count)){
                      count(uid)
                    }
                    p_list(func:uid(rt_p)){
                    project_id:ProjectInfo.project_id
                    }
                    """
                    rd_cal_query = """
                    tmp_rd_count as var(func:uid($rd_data_param_names))
                    @filter($rd_bk_biz_id_cond and $rd_active_cond $rd_created_by $created_at_cond
                    $and_final_filter_uids_name){
                    rd_b as AccessRawData.bk_biz
                    }
                    rd_count(func:uid(tmp_rd_count)){
                    count(uid)
                    }
                    """
                    tdw_cal_query = """
                    tdw_uids_dm as var(func:uid($tdw_param_names))
                    @filter($tdw_bk_biz_id_cond $tdw_created_by $created_at_cond $and_final_filter_uids_name)
                    tdw_count(func:uid(tdw_uids_dm)){
                    count(uid)
                    }
                    """
                    bk_biz_id_list_uids = ''
                    if only_standard or only_not_standard:
                        if TYPE_RESULT_TABLE in data_set_type_map:
                            d_query_uids += rt_cal_query
                            type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
                            final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rt_count')
                            bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
                    else:
                        if TYPE_RESULT_TABLE in data_set_type_map:
                            d_query_uids += rt_cal_query
                            type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
                            final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rt_count')
                            bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
                        if TYPE_RAW_DATA in data_set_type_map:
                            d_query_uids += rd_cal_query
                            type_uids_dict[D_UID_RAW_DATA_KEY] = 'tmp_rd_count'
                            final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tmp_rd_count')
                            bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rd_b')
                        if TYPE_TDW in data_set_type_map:
                            d_query_uids += tdw_cal_query
                            final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tdw_uids_dm')
                            type_uids_dict[D_UID_TDW_TABLE_KEY] = 'tdw_uids_dm'

                    if (
                        storage_type
                        or is_filter_by_lifecycle(
                            range_operate,
                            range_score,
                            heat_operate,
                            heat_score,
                            importance_operate,
                            importance_score,
                            asset_value_operate,
                            asset_value_score,
                            assetvalue_to_cost_operate,
                            assetvalue_to_cost,
                            storage_capacity_operate,
                            storage_capacity,
                        )
                    ) and final_filter_uids_name_tmp in d_query_uids:
                        and_final_filter_uids_name = 'and uid({})'.format(final_filter_uids_name_tmp)
                    else:
                        and_final_filter_uids_name = ''
                    d_query_uids = d_query_uids.replace('$and_final_filter_uids_name', and_final_filter_uids_name)

                    if bk_biz_id_list_uids:
                        d_query_uids += gen_bk_biz_id_list_query(bk_biz_id_list_uids)

                d_query_uids = d_query_uids.replace('$rt_data_param_names', rt_data_param_names)
                d_query_uids = d_query_uids.replace('$rd_data_param_names', rd_data_param_names)
                d_query_uids = d_query_uids.replace('$tdw_param_names', tdw_param_names)

    elif project_id is not None:
        with_bk_biz_id_cond = ''
        with_rt_bk_biz_id_cond = rt_bk_biz_id_cond
        d_query_uids = """
            $final_entitys_name as var(func:$rt_project_id_cond) @filter(($with_rt_bk_biz_id_cond $with_bk_biz_id_cond
            $rt_created_by $created_at_cond) $and_all_st_data_set_uids)"""
        # project_id不为空的时候
        d_query_uids = lifecycle_d_query_uids(lifecycle_type, data_set_type, d_query_uids, 'result_table')

        d_query_uids = d_query_uids.replace(
            '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
        )
        if bk_biz_id is not None:
            with_rt_bk_biz_id_cond = ''
            with_bk_biz_id_cond = " eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))

        d_query_uids = d_query_uids.replace('$with_bk_biz_id_cond', with_bk_biz_id_cond)
        d_query_uids = d_query_uids.replace('$with_rt_bk_biz_id_cond', with_rt_bk_biz_id_cond)
        if dgrah_cal_type == DGRAPH_FLOATING:
            final_filter_uids_name = final_entitys_name
            type_uids_dict[D_UID_RESULT_TABLE_KEY] = final_filter_uids_name
            if need_only_uids:
                pass

            else:
                d_query_uids, final_filter_uids_name = get_uids_filtered_lifecycle_metric(
                    d_query_uids,
                    final_filter_uids_name,
                    data_set_type,
                    storage_type,
                    range_operate,
                    range_score,
                    heat_operate,
                    heat_score,
                    importance_operate,
                    importance_score,
                    asset_value_operate,
                    asset_value_score,
                    assetvalue_to_cost_operate,
                    assetvalue_to_cost,
                    storage_capacity_operate,
                    storage_capacity,
                )
                if storage_type or is_filter_by_lifecycle(
                    range_operate,
                    range_score,
                    heat_operate,
                    heat_score,
                    importance_operate,
                    importance_score,
                    asset_value_operate,
                    asset_value_score,
                    assetvalue_to_cost_operate,
                    assetvalue_to_cost,
                    storage_capacity_operate,
                    storage_capacity,
                ):
                    d_query_uids += """
                        tmp_rt_count as var(func:uid($final_filter_uids_name)){
                    """
                else:
                    d_query_uids += """
                        tmp_rt_count as var(func:uid($final_entitys_name)){
                    """
                d_query_uids += """
                    rt_p as ResultTable.project
                    rt_b as ResultTable.bk_biz
                    }
                    rt_count(func:uid(tmp_rt_count)){
                      count(uid)
                    }
                    p_list(func:uid(rt_p)){
                    project_id:ProjectInfo.project_id
                    }
                    bk_biz_id_list(func:uid(rt_b)){
                    bk_biz_id : BKBiz.id
                    }
                    """
                if (
                    storage_type
                    or is_filter_by_lifecycle(
                        range_operate,
                        range_score,
                        heat_operate,
                        heat_score,
                        importance_operate,
                        importance_score,
                        asset_value_operate,
                        asset_value_score,
                        assetvalue_to_cost_operate,
                        assetvalue_to_cost,
                        storage_capacity_operate,
                        storage_capacity,
                    )
                    and final_filter_uids_name
                ):
                    d_query_uids = d_query_uids.replace('$final_filter_uids_name', final_filter_uids_name)
                type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'tmp_rt_count'
    if bk_biz_id is not None:
        rt_bk_biz_id_cond = "eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))
        rd_bk_biz_id_cond = "eq(AccessRawData.bk_biz_id,{})".format(str(bk_biz_id))
        tdw_bk_biz_id_cond = "eq(TdwTable.bk_biz_id,{})".format(str(bk_biz_id))

    if (only_standard or only_not_standard) and dgrah_cal_type != DGRAPH_OVERALL_MAP:
        d_query_uids = only_standard_query + d_query_uids
    if dgrah_cal_type == DGRAPH_OVERALL_MAP:
        d_query_uids += """
            virtual_data_mart(func:uid($final_entitys_name)){
              count(uid)
            }
            """
    elif dgrah_cal_type == DGRAPH_FLOATING:
        if not type_uids_dict:
            if only_standard or only_not_standard:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'rt_uids_dm'
            else:
                if TYPE_RESULT_TABLE in data_set_type_map:
                    type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'rt_uids_dm'
                if TYPE_RAW_DATA in data_set_type_map:
                    type_uids_dict[D_UID_RAW_DATA_KEY] = 'rd_uids_dm'
                if TYPE_TDW in data_set_type_map:
                    type_uids_dict[D_UID_TDW_TABLE_KEY] = 'tdw_uids_dm'

    d_query_uids = d_query_uids.replace('$rt_generate_type_cond', rt_generate_type_cond)
    d_query_uids = d_query_uids.replace('$and_all_st_data_set_uids', and_all_st_data_set_uids)
    d_query_uids = d_query_uids.replace('$rt_generate_type', rt_generate_type)
    d_query_uids = d_query_uids.replace('$final_entitys_name', final_entitys_name)
    d_query_uids = d_query_uids.replace('$param_names', param_names)
    d_query_uids = d_query_uids.replace('$rt_bk_biz_id_cond', rt_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rd_bk_biz_id_cond', rd_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rd_active_cond', rd_active_cond)
    d_query_uids = d_query_uids.replace('$tdw_bk_biz_id_cond', tdw_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$keyword', keyword)
    d_query_uids = d_query_uids.replace(rt_created_by_field_name, rt_created_by)
    d_query_uids = d_query_uids.replace(rd_created_by_field_name, rd_created_by)
    d_query_uids = d_query_uids.replace(tdw_created_by_field_name, tdw_created_by)
    d_query_uids = d_query_uids.replace('$created_at_cond', created_at_cond)
    if need_only_uids or need_detail_type == NEED_DATA_SET_ID_DETAIL or dgrah_cal_type == DGRAPH_FLOATING:
        return d_query_uids, final_filter_uids_name, type_uids_dict
    else:
        return '{\n' + d_query_uids + '\n}', final_entitys_name, type_uids_dict


def gen_bk_biz_id_list_uids(bk_biz_id_list_uids, uid_name):
    if bk_biz_id_list_uids:
        bk_biz_id_list_uids += ','
    bk_biz_id_list_uids += uid_name
    return bk_biz_id_list_uids


def gen_bk_biz_id_list_query(bk_biz_id_list_uids):
    return """
        bk_biz_id_list(func:uid($bk_biz_id_list_uids)){
        bk_biz_id : BKBiz.id
        }
        """.replace(
        '$bk_biz_id_list_uids', bk_biz_id_list_uids
    )


def dgraph_standard_uids2(
    bk_biz_id,
    project_id,
    keyword,
    platform,
    business_code_list,
    system_code_list,
    desc_code_list,
    tag_uids_name,
    order_range=None,
    order_heat=None,
):
    rt_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('DmTaskDetail')
    final_entitys_name = 'supported_entitys_st'
    d_query_uids = """
        var(func:has(DmTaskDetail.id)){
          $final_entitys_name as DmTaskDetail.data_set @filter(uid($tag_uids_name))
        }
    """
    d_query_uids = d_query_uids.replace('$tag_uids_name', tag_uids_name)
    d_query_uids = d_query_uids.replace('$final_entitys_name', final_entitys_name)
    d_query_uids = d_query_uids.replace('$rt_bk_biz_id_cond', rt_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$keyword', keyword)

    return d_query_uids, final_entitys_name


def dgraph_get_selected_tags_uids(business_code_list, system_code_list, desc_code_list):  # 拿到选择了标签后的uids列表
    d_query_uids = ''
    business_type_name = ''
    system_type_name = ''
    final_entitys_name = ''
    if business_code_list:
        business_dgraph_query, business_type_name = dgraph_tag_type_query(
            business_code_list, 'business', filter_content=''
        )
        d_query_uids += business_dgraph_query
        final_entitys_name = business_type_name
    if system_code_list:
        system_dgraph_query, system_type_name = dgraph_tag_type_query(system_code_list, 'system', filter_content='')
        d_query_uids += system_dgraph_query
        final_entitys_name = system_type_name
        if business_type_name:
            final_entitys_name = system_type_name + '1'
            d_query_uids += """
                        {} as var(func:uid({})) @filter(uid({}))
                        """.format(
                final_entitys_name, system_type_name, business_type_name
            )
    if desc_code_list:
        desc_dgraph_query, desc_type_name = dgraph_tag_type_query(desc_code_list, 'desc', filter_content='')
        d_query_uids += desc_dgraph_query
        if system_type_name:
            tmp_final_entitys_name = final_entitys_name
            final_entitys_name = desc_type_name + '1'
            d_query_uids += """
                                    {} as var(func:uid({})) @filter(uid({}))
                                    """.format(
                final_entitys_name, desc_type_name, tmp_final_entitys_name
            )
        elif business_type_name:  # 说明没有system类型的标签
            final_entitys_name = desc_type_name + '1'
            d_query_uids += """
                                    {} as var(func:uid({})) @filter(uid({}))
                                    """.format(
                final_entitys_name, desc_type_name, business_type_name
            )
        else:
            final_entitys_name = desc_type_name
    return d_query_uids, final_entitys_name


def get_all_standard_query():
    return """
        var(func:eq(DmStandardVersionConfig.standard_version_status,"online")) {
           ~DmTaskDetail.standard_version{
            all_st_data_set_uids as DmTaskDetail.data_set
          }
        }
        """


def dgraph_cond_uids2(
    tag_code,
    bk_biz_id,
    project_id,
    keyword,
    platform,
    business_code_list,
    system_code_list,
    desc_code_list,
    created_by=None,
    is_virtual_other_node_pre=None,
    is_virtual_other_not_standard_node_pre=False,
    data_set_type=TYPE_ALL,
    only_standard=False,
    need_only_uids=False,
    order_range=None,
    order_heat=None,
    order_assetvalue_to_cost=None,
    order_importance=None,
    order_asset_value=None,
    order_storage_capacity=None,
    created_at_start=None,
    created_at_end=None,
    range_operate=None,
    range_score=None,
    heat_operate=None,
    heat_score=None,
    importance_operate=None,
    importance_score=None,
    asset_value_operate=None,
    asset_value_score=None,
    assetvalue_to_cost_operate=None,
    assetvalue_to_cost=None,
    storage_capacity_operate=None,
    storage_capacity=None,
    storage_type=None,
    only_not_standard=False,
):
    # is_virtual_other_node_pre和is_virtual_other_not_standard_node_pre这两个参数不可能同时为True的
    rt_generate_type = """eq(ResultTable.generate_type,"user")"""
    rt_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('ResultTable')
    rd_bk_biz_id_cond = '(' + dgraph_exclude_bk_biz_id_cond('AccessRawData') + 'and eq(active, true)' + ')'
    tdw_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('TdwTable')
    final_entitys_name = 'supported_entitys'

    only_standard_query = get_all_standard_query()
    and_all_st_data_set_uids = ''
    if only_standard:
        all_filter_uids = 'uid(all_st_data_set_uids)'
        and_all_st_data_set_uids = ' AND ' + all_filter_uids
    if only_not_standard:
        all_filter_uids = 'not uid(all_st_data_set_uids)'
        and_all_st_data_set_uids = ' AND ' + all_filter_uids

    data_set_type_map = gen_data_set_map(
        data_set_type, only_standard, platform=platform, project_id=project_id, only_not_standard=only_not_standard
    )

    type_uids_dict = {}

    virtual_other_filter_cond = ''
    if is_virtual_other_not_standard_node_pre:
        virtual_other_filter_cond = """
        var(func:has(DmTaskDetail.id)){
            other_exclude_uids as DmTaskDetail.data_set
          }
        """

    rt_created_by_field_name = '$rt_created_by'
    rd_created_by_field_name = '$rd_created_by'
    tdw_created_by_field_name = '$tdw_created_by'
    rt_created_by, rd_created_by, tdw_created_by = '', '', ''
    if created_by:
        rt_created_by = ' AND eq(created_by,"' + created_by + '")'
        rd_created_by = ' AND eq(created_by,"' + created_by + '")'
        tdw_created_by = ' AND eq(created_by,"tdw_' + created_by + '")'
    created_at_cond = (
        dgraph_created_at_cond(created_at_start, created_at_end) if created_at_start and created_at_end else ''
    )

    final_filter_uids_name = final_entitys_name

    if is_virtual_other_node_pre:
        d_query_uids = """
        var(func:eq(Tag.code,"$tag_code")){
            supported_entitys as Tag.targets $selected_tags_uids
          }
        """
    else:
        d_query_uids = (
            virtual_other_filter_cond
            + """
                my as var(func:eq(Tag.code,"$tag_code")) @recurse{
                parentids as ~Tag.parent_tag
              }

                 var(func:uid(parentids,my)){
                   supported_entitys as Tag.targets $selected_tags_uids
                  }
            """
        )

    selected_tag_query, selected_tag_entitys_name = dgraph_get_selected_tags_uids(
        business_code_list, system_code_list, desc_code_list
    )
    record_selected_tag_entitys_name = selected_tag_entitys_name
    if selected_tag_entitys_name:
        selected_tag_entitys_name = '@filter(uid(' + selected_tag_entitys_name + ")"
        if only_standard or only_not_standard:
            selected_tag_entitys_name += and_all_st_data_set_uids
        if is_virtual_other_not_standard_node_pre:
            selected_tag_entitys_name += ' AND not uid(other_exclude_uids)'
        selected_tag_entitys_name += ')'
        d_query_uids = selected_tag_query + d_query_uids
    else:
        if is_virtual_other_not_standard_node_pre:
            selected_tag_entitys_name = '@filter(not uid(other_exclude_uids))'

    if tag_code == VIRTUAL_DATA_MART_ROOT_NAME:  # [数据集市]节点
        d_query_uids = (
            selected_tag_query
            + virtual_other_filter_cond
            + """
        supported_entitys as var(func:uid($selected_tags_uids))
        """
        )
        d_query_uids = d_query_uids.replace('$selected_tags_uids', record_selected_tag_entitys_name)
    elif tag_code == DATA_MART_OTHER_NAME:
        d_query_uids = selected_tag_query
        final_entitys_name = record_selected_tag_entitys_name
    else:
        d_query_uids = d_query_uids.replace('$selected_tags_uids', selected_tag_entitys_name)

    rt_query_uids = """
     rt_uids as var(func:uid($final_entitys_name))
     @filter($rt_bk_biz_id_cond AND $rt_generate_type $rt_created_by $created_at_cond $and_all_st_data_set_uids)
    """
    rd_query_uids = """
     rd_uids as var(func:uid($final_entitys_name)) @filter($rd_bk_biz_id_cond $rd_created_by $created_at_cond)
    """
    tdw_query_uids = """
     tdw_uids as var(func:uid($final_entitys_name)) @filter($tdw_bk_biz_id_cond $tdw_created_by $created_at_cond)
    """

    param_names = ''
    if keyword:
        if project_id is not None:
            with_rt_bk_biz_id_cond = ' AND ' + rt_bk_biz_id_cond
            rt_query_uids = """
            rt_uids1 as var(func:uid($final_entitys_name))
            @filter($rt_project_id_cond $with_rt_bk_biz_id_cond AND $rt_generate_type $rt_created_by $created_at_cond
            $and_all_st_data_set_uids)
            rt_uids as var(func:uid(rt_uids1)) @filter(regexp(ResultTable.result_table_id,/$keyword/)
            or regexp(ResultTable.result_table_name_alias,/$keyword/) or regexp(ResultTable.description,/$keyword/))
            """
            rt_query_uids = rt_query_uids.replace(
                '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
            )
            if bk_biz_id is not None:
                with_rt_bk_biz_id_cond = " AND eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))

            rt_query_uids = rt_query_uids.replace('$with_rt_bk_biz_id_cond', with_rt_bk_biz_id_cond)
        else:
            rt_query_uids = """
             rt_uids1 as var(func:uid($final_entitys_name))
             @filter($rt_bk_biz_id_cond AND $rt_generate_type $rt_created_by $created_at_cond $and_all_st_data_set_uids)
             rt_uids as var(func:uid(rt_uids1)) @filter(regexp(ResultTable.result_table_id,/$keyword/)
             or regexp(ResultTable.result_table_name_alias,/$keyword/) or regexp(ResultTable.description,/$keyword/))
            """

            rd_query_uids = """
             rd_uids1 as var(func:uid($final_entitys_name))
             @filter($rd_bk_biz_id_cond $rd_created_by $created_at_cond)
             rd_uids as var(func:uid(rd_uids1))
             @filter(regexp(AccessRawData.raw_data_name,/$keyword/) or regexp(AccessRawData.raw_data_alias,/$keyword/)
             or regexp(AccessRawData.description,/$keyword/) $eq_access_raw_data_id)
            """

            eq_access_raw_data_id = ''
            if keyword.isdigit():
                eq_access_raw_data_id = ' or eq(AccessRawData.id,$keyword)'

            rd_query_uids = rd_query_uids.replace('$eq_access_raw_data_id', eq_access_raw_data_id)

            tdw_query_uids = """
            tdw_uids1 as var(func:uid($final_entitys_name))
            @filter($tdw_bk_biz_id_cond $tdw_created_by $created_at_cond)
            tdw_uids as var(func:uid(tdw_uids1))
            @filter(regexp(TdwTable.table_id,/$keyword/) or regexp(TdwTable.table_name,/$keyword/)
            or regexp(TdwTable.table_comment,/$keyword/))
            """
    elif project_id is not None:
        rt_query_uids = """
             rt_uids as var(func:uid($final_entitys_name))
             @filter($rt_project_id_cond $with_rt_bk_biz_id_cond
             AND $rt_generate_type $rt_created_by $created_at_cond $and_all_st_data_set_uids)
             """
        with_rt_bk_biz_id_cond = ' AND ' + rt_bk_biz_id_cond
        rt_query_uids = rt_query_uids.replace(
            '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
        )
        if bk_biz_id is not None:
            with_rt_bk_biz_id_cond = " AND eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))
        rt_query_uids = rt_query_uids.replace('$with_rt_bk_biz_id_cond', with_rt_bk_biz_id_cond)

    if bk_biz_id is not None and project_id is None:
        rt_bk_biz_id_cond = "eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))
        rd_bk_biz_id_cond = "( eq(AccessRawData.bk_biz_id,{}) and eq(active, true) )".format(str(bk_biz_id))
        tdw_bk_biz_id_cond = "eq(TdwTable.bk_biz_id,{})".format(str(bk_biz_id))

    if order_range == 'asc' or order_range == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.range {
                  range_rt_uids as uid
                }
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.range {
                  range_rd_uids as uid
                }
              }
            }
        """
    elif order_heat == 'asc' or order_heat == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.heat {
                  heat_rt_uids as uid
                }
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.heat {
                  heat_rd_uids as uid
                }
              }
            }
        """
    elif order_importance == 'asc' or order_importance == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.importance {
                  importance_rt_uids as uid
                }
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.importance {
                  importance_rd_uids as uid
                }
              }
            }
        """
    elif order_asset_value == 'asc' or order_asset_value == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.asset_value {
                  asset_value_rt_uids as uid
                }
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.asset_value {
                  asset_value_rd_uids as uid
                }
              }
            }
        """
    elif order_storage_capacity == 'asc' or order_storage_capacity == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.cost {
                  Cost.capacity {
                    storage_capacity_rt_uids as uid
                  }
                }
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                LifeCycle.cost {
                  Cost.capacity {
                    storage_capacity_rd_uids as uid
                  }
                }
              }
            }
        """
    elif order_assetvalue_to_cost == 'asc' or order_assetvalue_to_cost == 'desc':
        rt_query_uids += """
            {
              ~LifeCycle.target {
                assetvalue_to_cost_rt_uids as uid
              }
            }
        """
        rd_query_uids += """
            {
              ~LifeCycle.target {
                assetvalue_to_cost_rd_uids as uid
              }
            }
        """
    rt_cal_query = """
        tmp_rt_count as var(func:uid(rt_uids)){
          rt_p as ResultTable.project
          rt_b as ResultTable.bk_biz
        }
        rt_count(func:uid(tmp_rt_count)){
          count(uid)
        }
        p_list(func:uid(rt_p)){
          project_id:ProjectInfo.project_id
        }
    """
    rd_cal_query = """
        tmp_rd_count as var(func:uid(rd_uids)){
          rd_b as AccessRawData.bk_biz
        }
        rd_count(func:uid(tmp_rd_count)){
          count(uid)
        }
    """
    tdw_cal_query = """
        tdw_count(func:uid(tdw_uids)){
          count(uid)
        }
    """
    final_filter_uids_name = ''

    tmp_cal_query = ''
    need_only_uids_query = ''
    bk_biz_id_list_uids = ''
    if TYPE_RESULT_TABLE in data_set_type_map:
        d_query_uids += rt_query_uids
        tmp_cal_query += rt_cal_query
        bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'rt_uids')
        type_uids_dict[D_UID_RESULT_TABLE_KEY] = 'rt_uids'

    if TYPE_RAW_DATA in data_set_type_map:
        d_query_uids += rd_query_uids
        tmp_cal_query += rd_cal_query
        bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rd_b')
        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'rd_uids')
        type_uids_dict[D_UID_RAW_DATA_KEY] = 'rd_uids'

    if TYPE_TDW in data_set_type_map:
        d_query_uids += tdw_query_uids
        tmp_cal_query += tdw_cal_query
        final_filter_uids_name = gen_filter_uids(final_filter_uids_name, 'tdw_uids')
        type_uids_dict[D_UID_TDW_TABLE_KEY] = 'tdw_uids'

    if bk_biz_id_list_uids:
        tmp_cal_query += gen_bk_biz_id_list_query(bk_biz_id_list_uids)

    if need_only_uids:
        d_query_uids += need_only_uids_query
    else:
        d_query_uids, final_filter_uids_name = get_uids_filtered_lifecycle_metric(
            d_query_uids,
            final_filter_uids_name,
            data_set_type,
            storage_type,
            range_operate,
            range_score,
            heat_operate,
            heat_score,
            importance_operate,
            importance_score,
            asset_value_operate,
            asset_value_score,
            assetvalue_to_cost_operate,
            assetvalue_to_cost,
            storage_capacity_operate,
            storage_capacity,
        )
        if (
            storage_type
            or is_filter_by_lifecycle(
                range_operate,
                range_score,
                heat_operate,
                heat_score,
                importance_operate,
                importance_score,
                asset_value_operate,
                asset_value_score,
                assetvalue_to_cost_operate,
                assetvalue_to_cost,
                storage_capacity_operate,
                storage_capacity,
            )
        ) and final_filter_uids_name in d_query_uids:
            tmp_cal_query = tmp_cal_query.replace(
                '(func:uid(rt_uids))', '(func:uid(rt_uids))@filter(uid({}))'.format(final_filter_uids_name)
            )
            tmp_cal_query = tmp_cal_query.replace(
                '(func:uid(rd_uids))', '(func:uid(rd_uids))@filter(uid({}))'.format(final_filter_uids_name)
            )
            tmp_cal_query = tmp_cal_query.replace(
                '(func:uid(tdw_uids))', '(func:uid(tdw_uids))@filter(uid({}))'.format(final_filter_uids_name)
            )
        d_query_uids += tmp_cal_query

    if only_standard or only_not_standard:
        d_query_uids = only_standard_query + d_query_uids

    d_query_uids = d_query_uids.replace('$tag_code', tag_code)
    d_query_uids = d_query_uids.replace('$and_all_st_data_set_uids', and_all_st_data_set_uids)
    d_query_uids = d_query_uids.replace('$final_entitys_name', final_entitys_name)
    d_query_uids = d_query_uids.replace('$param_names', param_names)
    d_query_uids = d_query_uids.replace('$rt_bk_biz_id_cond', rt_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rd_bk_biz_id_cond', rd_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$tdw_bk_biz_id_cond', tdw_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rt_generate_type', rt_generate_type)
    d_query_uids = d_query_uids.replace('$keyword', keyword)

    d_query_uids = d_query_uids.replace(rt_created_by_field_name, rt_created_by)
    d_query_uids = d_query_uids.replace(rd_created_by_field_name, rd_created_by)
    d_query_uids = d_query_uids.replace(tdw_created_by_field_name, tdw_created_by)
    d_query_uids = d_query_uids.replace('$created_at_cond', created_at_cond)

    return d_query_uids, final_filter_uids_name, type_uids_dict


def dgraph_created_by_cond(created_by):
    # 创建者条件拼接
    cond = 'and eq(created_by,{})'.format(created_by)
    return cond


def dgraph_created_at_cond(created_at_start, created_at_end):
    # 创建时间条件拼接
    cond = 'and ge(created_at,"{}") and le(created_at,"{}")'.format(created_at_start, created_at_end)
    return cond


def dgraph_cond_uids(
    bk_biz_id,
    project_id,
    keyword,
    platform,
    business_code_list,
    system_code_list,
    desc_code_list,
    only_standard=False,
    created_by=None,
    created_at_start=None,
    created_at_end=None,
    only_not_standard=False,
):
    # 返回符合条件的uid变量
    """将过滤条件拼接到GrapgQL语句中"""
    rt_generate_type = """eq(ResultTable.generate_type,"user")"""
    rt_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('ResultTable')
    rd_bk_biz_id_cond = '(' + dgraph_exclude_bk_biz_id_cond('AccessRawData') + 'and eq(active, true)' + ')'
    tdw_bk_biz_id_cond = dgraph_exclude_bk_biz_id_cond('TdwTable')
    created_by_cond = dgraph_created_by_cond(created_by) if created_by else ''
    created_at_cond = (
        dgraph_created_at_cond(created_at_start, created_at_end) if created_at_start and created_at_end else ''
    )

    final_entitys_name = 'supported_entitys'

    only_standard_query = get_all_standard_query()

    selected_tag_query, selected_tag_entitys_name = dgraph_get_selected_tags_uids(
        business_code_list, system_code_list, desc_code_list
    )
    all_filter_uids = 'has(~Tag.targets)'
    if selected_tag_entitys_name:
        all_filter_uids = "uid(" + selected_tag_entitys_name + ")"
    and_all_st_data_set_uids = ''
    if only_standard:
        and_all_st_data_set_uids = ' AND uid(all_st_data_set_uids)'
    elif only_not_standard:
        and_all_st_data_set_uids = ' AND not uid(all_st_data_set_uids)'

    d_query_uids = (
        selected_tag_query
        + """
        $final_entitys_name as var(func:$all_filter_uids)
        @filter((($rt_bk_biz_id_cond and $rt_generate_type $created_by_cond $created_at_cond)
        or ($rd_bk_biz_id_cond $created_by_cond $created_at_cond)
        or ($tdw_bk_biz_id_cond $created_by_cond $created_at_cond)) $and_all_st_data_set_uids)
    """
    )

    param_names = ''
    if keyword:
        if project_id is not None:
            with_bk_biz_id_cond = ' AND ' + rt_bk_biz_id_cond
            bk_data_uids, bk_data_param_names = (
                """
                        var(func:regexp(ResultTable.result_table_id,/$keyword/)){
                             meta_uids1 as uid
                            }
                            var(func:regexp(ResultTable.result_table_name_alias,/$keyword/)){
                             meta_uids2 as uid
                            }
                            var(func:regexp(ResultTable.description,/$keyword/)){
                             meta_uids3 as uid
                            }
                        """,
                'meta_uids1,meta_uids2,meta_uids3',
            )
            d_query_uids = bk_data_uids
            d_query_uids += (
                selected_tag_query
                + """
            $final_entitys_name as var(func:uid($param_names))
            @filter($rt_project_id_cond $with_bk_biz_id_cond $created_by_cond $created_at_cond
            $and_all_st_data_set_uids)
            """
            )

            d_query_uids = d_query_uids.replace(
                '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
            )
            if bk_biz_id is not None:
                with_bk_biz_id_cond = " AND eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))

            d_query_uids = d_query_uids.replace('$with_bk_biz_id_cond', with_bk_biz_id_cond)
            param_names = bk_data_param_names

        else:
            bk_data_uids, bk_data_param_names = (
                """
            var(func:regexp(ResultTable.result_table_id,/$keyword/)){
                 meta_uids1 as uid
                }
                var(func:regexp(ResultTable.result_table_name_alias,/$keyword/)){
                 meta_uids2 as uid
                }
                var(func:regexp(ResultTable.description,/$keyword/)){
                 meta_uids3 as uid
                }
                var(func:regexp(AccessRawData.raw_data_name,/$keyword/)){
                 meta_uids4 as uid
                }
                var(func:regexp(AccessRawData.description,/$keyword/)){
                 meta_uids5 as uid
                }
                var(func:regexp(AccessRawData.raw_data_alias,/$keyword/)){
                 meta_uids6 as uid
                }
            """,
                'meta_uids1,meta_uids2,meta_uids3,meta_uids4,meta_uids5,meta_uids6',
            )

            if keyword.isdigit():
                bk_data_uids += """
                var(func:eq(AccessRawData.id,$keyword)){
                 meta_uids10 as uid
                }
                """
                bk_data_param_names += ',meta_uids10'

            tdw_uids, tdw_param_names = (
                """
                var(func:regexp(TdwTable.table_id,/$keyword/)){
                 meta_uids7 as uid
                }
                var(func:regexp(TdwTable.table_name,/$keyword/)){
                 meta_uids8 as uid
                }
                var(func:regexp(TdwTable.table_comment,/$keyword/)){
                 meta_uids9 as uid
                }""",
                'meta_uids7,meta_uids8,meta_uids9',
            )

            if platform == TYPE_ALL:
                d_query_uids = bk_data_uids + tdw_uids
                param_names = bk_data_param_names + ',' + tdw_param_names
            elif platform == TYPE_BK_DATA:
                d_query_uids = bk_data_uids
                param_names = bk_data_param_names
            elif platform == TYPE_TDW:
                d_query_uids = tdw_uids
                param_names = tdw_param_names

            d_query_uids += (
                selected_tag_query
                + """
            $final_entitys_name as var(func:uid($param_names))
            @filter((($rt_bk_biz_id_cond $created_by_cond $created_at_cond)
            or ($rd_bk_biz_id_cond $created_by_cond $created_at_cond)
            or ($tdw_bk_biz_id_cond $created_by_cond $created_at_cond)) $and_all_st_data_set_uids) """
            )

        if selected_tag_entitys_name:
            d_query_uids += """
            $tag_final_entitys_name as var(func:uid($selected_tag_entitys_name)) @filter(uid($final_entitys_name))
            """
            d_query_uids = d_query_uids.replace('$final_entitys_name', final_entitys_name)
            final_entitys_name += '_tag'

    elif project_id is not None:
        with_bk_biz_id_cond = ''
        d_query_uids = (
            selected_tag_query
            + """
        $final_entitys_name as var(func:$all_filter_uids)
        @filter($rt_project_id_cond $with_bk_biz_id_cond $created_by_cond $created_at_cond $and_all_st_data_set_uids)"""
        )
        d_query_uids = d_query_uids.replace(
            '$rt_project_id_cond', "eq(ResultTable.project_id,{})".format(str(project_id))
        )
        if bk_biz_id is not None:
            with_bk_biz_id_cond = " AND eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))

        d_query_uids = d_query_uids.replace('$with_bk_biz_id_cond', with_bk_biz_id_cond)

    if bk_biz_id is not None:
        rt_bk_biz_id_cond = "eq(ResultTable.bk_biz_id,{})".format(str(bk_biz_id))
        rd_bk_biz_id_cond = "( eq(AccessRawData.bk_biz_id,{}) and eq(active, true) )".format(str(bk_biz_id))
        tdw_bk_biz_id_cond = "eq(TdwTable.bk_biz_id,{})".format(str(bk_biz_id))

    d_query_uids = d_query_uids.replace('$all_filter_uids', all_filter_uids)
    d_query_uids = d_query_uids.replace('$final_entitys_name', final_entitys_name)
    d_query_uids = d_query_uids.replace('$tag_final_entitys_name', final_entitys_name)
    d_query_uids = d_query_uids.replace('$selected_tag_entitys_name', selected_tag_entitys_name)
    d_query_uids = d_query_uids.replace('$param_names', param_names)
    d_query_uids = d_query_uids.replace('$rt_bk_biz_id_cond', rt_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rd_bk_biz_id_cond', rd_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$tdw_bk_biz_id_cond', tdw_bk_biz_id_cond)
    d_query_uids = d_query_uids.replace('$rt_generate_type', rt_generate_type)
    d_query_uids = d_query_uids.replace('$keyword', keyword)
    d_query_uids = d_query_uids.replace('$and_all_st_data_set_uids', and_all_st_data_set_uids)
    d_query_uids = d_query_uids.replace('$created_by_cond', created_by_cond)
    d_query_uids = d_query_uids.replace('$created_at_cond', created_at_cond)
    if only_standard or only_not_standard:
        d_query_uids = only_standard_query + d_query_uids

    return d_query_uids, final_entitys_name


def dgraph_tag_type_query(code_list, tag_type, filter_content='@filter(uid(supported_entitys))'):
    query_str = ''
    tag_type_uid_list = []
    uid_tag_type_name = 'uid_tag_type_{}'.format(tag_type)
    if code_list:
        for code in code_list:
            query_str += """\ntag_type_{tag_code} as var(func:eq(Tag.code,"{tag_code}")) @recurse{
                tag_type_parentids_{tag_code} as ~Tag.parent_tag
              }

             var(func:uid(tag_type_parentids_{tag_code},tag_type_{tag_code})){
               tag_type_final_{tag_code} as Tag.targets $filter_content
              }
             """.replace(
                '{tag_code}', code
            )
            tag_type_uid_list.append('tag_type_final_{tag_code}'.format(tag_code=code))
    if query_str:
        query_str += """
        {} as var(func:uid({}))
        """.format(
            uid_tag_type_name, ','.join(tag_type_uid_list)
        )
    query_str = query_str.replace('$filter_content', filter_content)
    return query_str, uid_tag_type_name


def get_single_tag_query(tag_code, final_entites_name, need_me_count=False, need_processing_type=False):
    query_str = """\nmy_{tag_code} as var(func:eq(Tag.code,"{tag_code}")) @recurse{
    parentids_{tag_code} as ~Tag.parent_tag
  }

 var(func:uid(parentids_{tag_code},my_{tag_code})){
   dataset_ids_{tag_code} as Tag.targets @filter(uid({supported_entitys}))
  }

 c_{tag_code}(func:uid(dataset_ids_{tag_code})){
  count(uid)
    }"""
    if need_me_count:
        query_str += """
        var(func:uid(my_{tag_code})){
           dataset_ids_me_{tag_code} as Tag.targets @filter(uid({supported_entitys}))
          }

         me_{tag_code}(func:uid(dataset_ids_me_{tag_code})){
          count(uid)
            }
        """
    if need_processing_type:
        for each_processing_type in processing_type_list:
            if RUN_MODE != 'PRODUCT':
                query_str += """
                c_{tag_code}_{processing_type}(func:eq(ResultTable.processing_type,"{processing_type}"))
                @filter(uid(dataset_ids_{tag_code})){
                  count(uid)
                }
                """
            else:
                query_str += """
                c_{tag_code}_{processing_type}(func:eq(ProcessingTypeConfig.processing_type_name, "{processing_type}")){
                    ~ResultTable.processing_type_obj @filter(uid(dataset_ids_{tag_code})){
                        count(uid)
                    }
                }"""
            query_str = query_str.replace('{processing_type}', each_processing_type)
    query_str = query_str.replace('{tag_code}', tag_code)
    query_str = query_str.replace('{supported_entitys}', final_entites_name)
    return query_str


def get_other_tag_query(tag_list, final_entites_name):
    # 其他节点和各个processing_type之间的对应关系,用于数据盘点的桑基图接口，正式环境和测试环境的processing_type index不一致
    query_str = ''
    for each_processing_type in processing_type_list:
        if RUN_MODE != 'PRODUCT':
            query_str += """
            c_other_{processing_type}(func:uid({supported_entitys}))
            @filter( (not uid({first_level_tag_list})) and eq(ResultTable.processing_type,"{processing_type}") ){
              count(uid)
            }
            """
        else:
            query_str += """
            c_other_{processing_type}(func:eq(ProcessingTypeConfig.processing_type_name,"{processing_type}")){
                ~ResultTable.processing_type_obj
                @filter( (uid({supported_entitys})) and (not uid({first_level_tag_list})) ){
                    count(uid)
                }
            }
            """
        query_str = query_str.replace('{processing_type}', each_processing_type)
    first_level_tag_list = []
    for each_tag in tag_list:
        first_level_tag_list.append('dataset_ids_%s' % each_tag.get('tag_code'))
    first_level_tag_str = ','.join(first_level_tag_list)
    query_str = query_str.replace('{first_level_tag_list}', first_level_tag_str)
    query_str = query_str.replace('{supported_entitys}', final_entites_name)
    return query_str


def get_single_standard_query2(
    standard_version_id_str,
    final_entites_name,
    is_singel_detail=False,
    order_range=None,
    order_heat=None,
    range_operate=None,
    range_score=None,
    heat_operate=None,
    heat_score=None,
    importance_operate=None,
    importance_score=None,
    asset_value_operate=None,
    asset_value_score=None,
    assetvalue_to_cost_operate=None,
    assetvalue_to_cost=None,
    storage_capacity_operate=None,
    storage_capacity=None,
    standard_content_id=None,
):
    if is_singel_detail:  # 悬浮框查询接口的
        query_str = """
         var(func:eq(DmTaskDetail.standard_version_id,$standard_version_id_str)){
                tmp_rt_count as DmTaskDetail.data_set @filter(uid($supported_entitys)){
                rt_p as ResultTable.project
                rt_b as ResultTable.bk_biz
                }
            }
                rt_count(func:uid(tmp_rt_count)){
                  count(uid)
                }
                p_list(func:uid(rt_p)){
                project_id:ProjectInfo.project_id
                }
                bk_biz_id_list(func:uid(rt_b)){
                bk_biz_id : BKBiz.id
                }
        """
        if order_range:
            query_str = """
                 var(func:eq(DmTaskDetail.standard_version_id,$standard_version_id_str)){
                        tmp_rt_count as DmTaskDetail.data_set @filter(uid($supported_entitys)){
                        ~LifeCycle.target {
                          LifeCycle.range {
                            range_rt_uids as uid
                          }
                        }
                        rt_p as ResultTable.project
                        rt_b as ResultTable.bk_biz
                        }
                    }
                        rt_count(func:uid(tmp_rt_count)){
                          count(uid)
                        }
                        p_list(func:uid(rt_p)){
                        project_id:ProjectInfo.project_id
                        }
                        bk_biz_id_list(func:uid(rt_b)){
                        bk_biz_id : BKBiz.id
                        }
            """
        elif order_heat:
            query_str = """
                 var(func:eq(DmTaskDetail.standard_version_id,$standard_version_id_str)){
                        tmp_rt_count as DmTaskDetail.data_set @filter(uid($supported_entitys)){
                        ~LifeCycle.target {
                          LifeCycle.heat {
                            heat_rt_uids as uid
                          }
                        }
                        rt_p as ResultTable.project
                        rt_b as ResultTable.bk_biz
                        }
                    }
                        rt_count(func:uid(tmp_rt_count)){
                          count(uid)
                        }
                        p_list(func:uid(rt_p)){
                        project_id:ProjectInfo.project_id
                        }
                        bk_biz_id_list(func:uid(rt_b)){
                        bk_biz_id : BKBiz.id
                        }
            """
    else:
        query_str = """
            var(func:eq(DmTaskDetail.standard_version_id,$standard_version_id_str)){
              var_$standard_version_id_str as DmTaskDetail.data_set @filter(uid($supported_entitys))
            }
            c_$standard_version_id_str(func:uid(var_$standard_version_id_str)){
              count(uid)
            }
        """
    if standard_content_id:
        query_str = query_str.replace(
            'DmTaskDetail.standard_version_id,$standard_version_id_str',
            'DmTaskDetail.standard_content_id,$standard_content_id',
        )
    query_str = query_str.replace('$standard_version_id_str', standard_version_id_str)
    query_str = query_str.replace('$supported_entitys', final_entites_name)
    query_str = query_str.replace('$standard_content_id', str(standard_content_id))
    return query_str


def get_day_list_dgraph():
    day_list = []
    now = datetime.date.today()
    dgraph_day_list = []
    # dgraph_format = '%Y-%m-%dT00:00:00'
    yesterday_dgraph_format = '%Y-%m-%dT23:59:59'
    for i in range(7):
        td = now - datetime.timedelta(days=i)
        # tomorrow = td + datetime.timedelta(days=1)
        yesterday = td + datetime.timedelta(days=-1)
        day = td.strftime('%Y%m%d')
        day_list.append(day)

        dgraph_day_list.append([day, yesterday.strftime(yesterday_dgraph_format), td.strftime(yesterday_dgraph_format)])

    day_list.reverse()
    dgraph_day_list.reverse()
    return day_list, dgraph_day_list


def parse_basic_list_dgraph_result(result_dict):
    """是否是标准化的数据
    data_set_id转化为unicode
    """
    # 转换数据字典列表的数据
    data_set_list = result_dict.pop('data_set_list', [])  # 函数兼容other节点不存在data_set_list字段
    result_dict['count'] = result_dict['dataset_count'] + result_dict['data_source_count']
    results = []
    if data_set_list:
        for data_set_dict in data_set_list:
            target_type = data_set_dict['data_set_type']
            target_id = data_set_dict['data_set_id']
            is_standard = data_set_dict.get('is_standard', 1)  # 不存在is_standard字段,说明查询参数有only_standard,只查标准表
            platform = 'bk_data'
            if target_type == 'table':
                target_type = 'result_table'
            elif target_type == 'data_id':
                target_type = 'raw_data'

            if target_type == 'tdw_table':
                platform = 'tdw'
            results.append(
                {
                    'data_set_type': target_type,
                    'data_set_id': str(target_id),
                    'is_standard': is_standard,
                    'platform': platform,
                }
            )

    result_dict['results'] = results


def search_summary_dgraph(params, conn):  # 数据地图右侧展示汇总数据接口,注意发挥source_tag_code的作用
    params['tag_code'] = VIRTUAL_DATA_MART_ROOT_NAME
    result_dict = floating_window_query_dgraph(params, conn, NEED_RECENT_DETAIL, need_recent_detail_list=True)
    return result_dict


def datamap_summary(params, conn):
    if not params['tag_code']:
        params['tag_code'] = VIRTUAL_DATA_MART_ROOT_NAME
    result_dict = floating_window_query_dgraph(params, conn, NEED_RECENT_DETAIL)
    if 'standard' in params['cal_type'] and 'only_standard' in params['cal_type'] and params['tag_code']:
        if 'standard_dataset_count' not in result_dict:
            result_dict['standard_dataset_count'] = result_dict['dataset_count']
    return result_dict


def datamap_recent_summary(params, conn):
    params['tag_code'] = VIRTUAL_DATA_MART_ROOT_NAME
    result_dict = floating_window_query_dgraph(
        params, conn, NEED_RECENT_DETAIL, need_only_uids=True, need_recent_detail_list=True
    )
    clear_redundant_fields(result_dict)
    return result_dict


def clear_redundant_fields(result_dict):
    result_dict.pop('project_count', None)
    result_dict.pop('standard_dataset_count', None)
    result_dict.pop('bk_biz_count', None)
    result_dict.pop('data_source_count', None)
    result_dict.pop('dataset_count', None)
    result_dict.pop('project_list', None)


def datamap_search_dgraph(params, conn, index_name_list):  # dgraph查询实现统计
    bk_biz_id = params.get('bk_biz_id')
    project_id = params.get('project_id')
    tag_codes_list = params.get('tag_ids')
    tag_codes_list = parse_tag_codes_list(tag_codes_list)
    keyword = params.get('keyword')
    cal_type_list = params.get('cal_type')
    cal_type_list = [] if cal_type_list is None else cal_type_list
    only_standard = True if CAL_TYPE_ONLY_STANDARD in cal_type_list else False
    platform = params.get('platform', TYPE_ALL)
    token_pkey = params.get('token_pkey', None)

    tree_list, tag_dimension_list = build_data_mart_tree(conn)

    business_code_list, system_code_list, desc_code_list = judge_tag_type(conn, tag_codes_list)

    d_query_uids, final_entites_name = dgraph_cond_uids(
        bk_biz_id,
        project_id,
        keyword,
        platform,
        business_code_list,
        system_code_list,
        desc_code_list,
        only_standard=only_standard,
    )  # 要过滤的数据集列表
    d_query_statement = '{' + d_query_uids
    tag_sql = "select code from tag where tag_type='business' and active=1"
    tag_list = tagaction.query_direct_sql_to_map_list(conn, tag_sql)
    for tag_dict in tag_list:
        tag_code = tag_dict['code']
        d_query_statement += get_single_tag_query(tag_code, final_entites_name)

    # 加入统计数据标准指标
    if CAL_TYPE_STANDARD in cal_type_list:
        dgraph_standard_query_uids, standard_entitys_name = dgraph_standard_uids2(
            bk_biz_id,
            project_id,
            keyword,
            platform,
            business_code_list,
            system_code_list,
            desc_code_list,
            final_entites_name,
        )
        st_cal_final_sql = get_standard_node_cal_sql_v2(
            keyword, bk_biz_id, project_id, '', '', '', '', '', platform=platform
        )
        # print ('---st_cal_final_sql:', st_cal_final_sql)
        st_cal_result_list = tagaction.query_direct_sql_to_map_list(conn, st_cal_final_sql)
        standard_version_id_list = [obj['category_name'] for obj in st_cal_result_list]
        standard_version_id_list = list(set(standard_version_id_list))

        d_query_statement += dgraph_standard_query_uids
        for standard_version_id_str in standard_version_id_list:
            d_query_statement += get_single_standard_query2(standard_version_id_str, standard_entitys_name)

    if not business_code_list and not system_code_list and not desc_code_list:  # 用户没有选择具体标签的情况下,计算数据集市节点
        if keyword:
            data_mart_dgraph_query = """
                        virtual_data_mart(func:uid($final_entitys_name)){
                          count(uid)
                        }
                        """
            data_mart_dgraph_query = data_mart_dgraph_query.replace('$final_entitys_name', final_entites_name)
            d_query_statement += data_mart_dgraph_query
        else:  # 查询关键字为空
            data_mart_dgraph_query, data_mart_final_filter_uids_name, _ = dgraph_data_mart_uids2(
                bk_biz_id, project_id, keyword, platform, only_standard=only_standard
            )
            d_query_statement += data_mart_dgraph_query
    else:  # 选择了标签
        data_mart_dgraph_query = """
                                    virtual_data_mart(func:uid($final_entitys_name)){
                                      count(uid)
                                    }
                                    """
        data_mart_dgraph_query = data_mart_dgraph_query.replace('$final_entitys_name', final_entites_name)
        d_query_statement += data_mart_dgraph_query

    d_query_statement += '}'
    # print(d_query_statement)

    dgraph_result = meta_dgraph_complex_search(d_query_statement, 'c_', token_pkey=token_pkey)

    for index_name in index_name_list:
        index_count_dict = dgraph_result
        # print ('index_count_dict:', index_count_dict)
        for node_dict in tree_list:
            assign_node_counts(node_dict, index_count_dict, 'category_name', index_name)

    # 添加all_count字段
    index_count_dict1 = dgraph_result
    for node_dict in tree_list:
        assign_node_counts(node_dict, index_count_dict1, 'category_name', 'all_count')

    # 加入统计数据标准指标
    if CAL_TYPE_STANDARD in cal_type_list:
        for st_cal_dict in st_cal_result_list:
            st_count = dgraph_result.get(st_cal_dict['category_name'], 0)
            st_cal_dict['dataset_count'] = st_count
            st_cal_dict['all_count'] = st_count

        standard_parent_code_dict = parse_tag_config_result(st_cal_result_list)
        # print ('standard_parent_code_dict:', standard_parent_code_dict)
        for node_dict in tree_list:
            hang_to_tree_node(node_dict, standard_parent_code_dict, has_standard=True, tree_list=tree_list)

    # 取出[数据域相关]节点的值
    metric_domain_node = tree_list[0]

    # 计算及构造数据集市节点/其他节点的统计值
    virtual_data_mart_node = {
        "tag_type": "business",
        "category_alias": DATA_MART_STR,
        "description": DATA_MART_STR,
        "icon": None,
        "is_selected": 0,
        "kpath": 1,
        "sync": 1,
        "sub_list": [],
        "parent_id": -1,
        "parent_code": "virtual_data_node",
        "me_type": "tag",
        "dataset_count": 0,
        "category_id": -1,
        "seq_index": 1,
        "category_name": VIRTUAL_DATA_MART_ROOT_NAME,
    }
    other_node = {
        "tag_type": "business",
        "category_alias": OTHER_STR,
        "description": OTHER_STR,
        "icon": None,
        "is_selected": 0,
        "kpath": 1,
        "sync": 1,
        "sub_list": [],
        "parent_id": 2,
        "parent_code": VIRTUAL_DATA_MART_ROOT_NAME,
        "me_type": "tag",
        "dataset_count": 0,
        "category_id": -2,
        "seq_index": 100,
        "category_name": DATA_MART_OTHER_NAME,
    }

    # 得到[数据集市]节点和[其他]节点的值
    data_mart_dataset_count = dgraph_result[VIRTUAL_DATA_MART_ROOT_NAME]
    virtual_data_mart_node['dataset_count'] = data_mart_dataset_count
    other_dataset_count_val = data_mart_dataset_count - metric_domain_node['dataset_count']
    if other_dataset_count_val < 0:
        other_dataset_count_val = 0
    other_node['dataset_count'] = other_dataset_count_val

    return tree_list, virtual_data_mart_node, other_node


def parse_dgraph_result(d_result_dict):
    result_dict = {}
    if 'rt_count' in d_result_dict:
        result_dict['rt_count'] = d_result_dict['rt_count'][0]['count']
    if 'rd_count' in d_result_dict:
        result_dict['rd_count'] = d_result_dict['rd_count'][0]['count']
    if 'tdw_count' in d_result_dict:
        result_dict['tdw_count'] = d_result_dict['tdw_count'][0]['count']
    if 'standard_count' in d_result_dict:
        result_dict['standard_count'] = d_result_dict['standard_count'][0]['count']
    if 'p_list' in d_result_dict:
        d_p_list = d_result_dict['p_list']
        if len(d_p_list) == 1 and 'count' in d_p_list[0]:  # 说明只是统计project的数量
            result_dict['p_list'] = d_p_list[0]['count']  # 只是数量
        else:
            p_list = [p['project_id'] for p in d_p_list]
            result_dict['p_list'] = p_list

    result_bk_biz_id_list = []
    if 'bk_biz_id_list' in d_result_dict:
        bk_biz_id_list = d_result_dict['bk_biz_id_list']
        if bk_biz_id_list:
            for bk_biz_id_dict in bk_biz_id_list:
                result_bk_biz_id_list.append(bk_biz_id_dict['bk_biz_id'])
            result_dict['bk_biz_id_list'] = result_bk_biz_id_list

    if 'data_set_id_list' in d_result_dict:
        data_set_id_list = d_result_dict['data_set_id_list']
        data_set_id_list_final = []
        for data_set_dict in data_set_id_list:
            data_set_dict_final = {'is_standard': 0}
            if 'ResultTable.result_table_id' in data_set_dict:
                data_set_dict_final['data_set_id'] = data_set_dict['ResultTable.result_table_id']
                data_set_dict_final['data_set_type'] = 'result_table'
                data_set_dict_final['platform'] = 'bk_data'
            elif 'AccessRawData.id' in data_set_dict:
                data_set_dict_final['data_set_id'] = data_set_dict['AccessRawData.id']
                data_set_dict_final['data_set_type'] = 'raw_data'
                data_set_dict_final['platform'] = 'bk_data'
            elif 'TdwTable.table_id' in data_set_dict:
                data_set_dict_final['data_set_id'] = data_set_dict['TdwTable.table_id']
                data_set_dict_final['data_set_type'] = 'tdw_table'
                data_set_dict_final['platform'] = 'tdw'
            data_set_id_list_final.append(data_set_dict_final)
        result_dict['data_set_id_list'] = data_set_id_list_final
    return result_dict


def get_floating_dgraph_result(
    data_mart_dgraph_query,
    result_dict,
    platform,
    need_detail_type=NEED_FLOATING_DETAIL,
    page=None,
    page_size=None,
    token_pkey=None,
    token_msg=None,
):
    if need_detail_type == NEED_DATA_SET_ID_DETAIL:
        data_mart_dgraph_query = dgraph_get_project_count(data_mart_dgraph_query)

    data_mart_dgraph_result = meta_dgraph_complex_search(
        data_mart_dgraph_query, return_original=True, token_pkey=token_pkey, token_msg=token_msg
    )
    if 'data_set_id_list' in data_mart_dgraph_result:
        data_mart_dgraph_result = format_search_res(data_mart_dgraph_result, page, page_size)

    data_mart_dgraph_result_final = parse_dgraph_result(data_mart_dgraph_result)

    result_dict['data_source_count'] = data_mart_dgraph_result_final.get('rd_count', 0)
    result_dict['dataset_count'] = data_mart_dgraph_result_final.get('rt_count', 0)
    project_list = data_mart_dgraph_result_final.get('p_list', [])
    bk_biz_list = data_mart_dgraph_result_final.get('bk_biz_id_list', [])
    if isinstance(project_list, list):
        result_dict['project_list'] = project_list
        result_dict['project_count'] = len(project_list)
    else:
        result_dict['project_count'] = project_list

    result_dict['bk_biz_list'] = bk_biz_list
    result_dict['bk_biz_count'] = len(bk_biz_list)
    tdw_count = data_mart_dgraph_result_final.get('tdw_count', 0)
    if 'standard_count' in data_mart_dgraph_result_final:
        result_dict['standard_dataset_count'] = data_mart_dgraph_result_final['standard_count']

    if platform == TYPE_ALL or platform == TYPE_TDW:
        result_dict['dataset_count'] += tdw_count
        if tdw_count > 0:
            result_dict['bk_biz_count'] += 1
            if TDW_BK_BIZ_ID not in bk_biz_list:
                bk_biz_list.append(TDW_BK_BIZ_ID)

    if need_detail_type == NEED_DATA_SET_ID_DETAIL:  # 数据字典
        data_set_id_list = data_mart_dgraph_result_final.get('data_set_id_list', [])
        result_dict['data_set_list'] = data_set_id_list
    return data_mart_dgraph_result


def get_recent_dgraph_result(
    data_mart_dgraph_query,
    result_dict,
    platform,
    need_detail_type=NEED_FLOATING_DETAIL,
    rt_key_name_list=None,
    rd_key_name_list=None,
    tdw_key_name_list=None,
    standard_key_name_list=None,
    token_pkey=None,
    token_msg=None,
):
    data_dgraph_result = get_floating_dgraph_result(
        data_mart_dgraph_query,
        result_dict,
        platform,
        need_detail_type=need_detail_type,
        token_pkey=token_pkey,
        token_msg=token_msg,
    )
    rt_recent_result_list = parse_recent_type_result(data_dgraph_result, rt_key_name_list)
    rd_recent_result_list = parse_recent_type_result(data_dgraph_result, rd_key_name_list)
    tdw_recent_result_list = parse_recent_type_result(data_dgraph_result, tdw_key_name_list)
    standard_recent_result_list = parse_recent_type_result(data_dgraph_result, standard_key_name_list)
    standard_count = 0
    if 'standard_count' in data_dgraph_result:
        standard_count = data_dgraph_result['standard_count'][0]['count']
    return (
        rt_recent_result_list,
        rd_recent_result_list,
        tdw_recent_result_list,
        standard_recent_result_list,
        standard_count,
    )


def parse_recent_type_result(data_dgraph_result, key_name_list):
    recent_result_list = []
    if key_name_list:
        for key_name in key_name_list:
            created_day = key_name.rsplit('_', 1)[1]
            recent_result_list.append(
                {'created_day': created_day, 'index_count': data_dgraph_result[key_name][0]['count']}
            )
    return recent_result_list


def dgraph_get_project_count(dgraph_query):
    dgraph_query = dgraph_query.replace('project_id:ProjectInfo.project_id', 'count(uid)')
    return dgraph_query


def gen_zero_data_result():
    return {
        "count": 0,
        "project_list": [],
        "bk_biz_count": 0,
        "data_source_count": 0,
        "results": [],
        "dataset_count": 0,
        "project_count": 0,
    }


def floating_window_query_dgraph(
    params,
    conn,
    need_detail_type,
    need_only_uids=False,
    need_all_data_dict_list=False,
    need_recent_detail_list=False,
    data_source_distribute=False,
    data_source_detail_distribute=False,
    data_type_distribute=False,
    sankey_diagram_distribute=False,
):  # 悬浮框查询接口,dgraph实现
    # 以下 为输入参数的处理
    bk_biz_id = params.get('bk_biz_id')
    project_id = params.get('project_id')
    tag_codes_list = params.get('tag_ids')
    tag_codes_list = parse_tag_codes_list(tag_codes_list)
    keyword = params.get('keyword')
    keyword = keyword.strip()
    if keyword and len(keyword) < 3:
        raise ValidationError
    single_tag_code = params.get('tag_code')
    cal_type_list = params.get('cal_type')
    cal_type_list = [] if cal_type_list is None else cal_type_list
    has_standard = params.get('has_standard')  # 是否是tag节点并且下面有标准的
    me_type = params.get('me_type')
    only_standard = True if CAL_TYPE_ONLY_STANDARD in cal_type_list else False
    only_not_standard = True if CAL_TYPE_ONLY_NOT_STANDARD in cal_type_list else False
    platform = params.get('platform', TYPE_ALL)
    data_set_type = params.get('data_set_type', TYPE_ALL)
    created_by = params.get('created_by', None)
    # 其实创建时间和终止创建时间两个参数要同时存在
    if params.get('created_at_start') and params.get('created_at_end'):
        created_at_start = str(arrow.get(params.get('created_at_start')).format('YYYY-MM-DDTHH:mm:ss+08:00'))
        created_at_end = str(arrow.get(params.get('created_at_end')).format('YYYY-MM-DDTHH:mm:ss+08:00'))
    else:
        created_at_start = None
        created_at_end = None
    # 按照存储类型过滤
    storage_type = params.get('storage_type', None)
    # 按照广度、热度排序
    range_operate = params.get('range_operate', None)
    range_score = params.get('range_score', None)
    heat_operate = params.get('heat_operate', None)
    heat_score = params.get('heat_score', None)
    importance_operate = params.get('importance_operate', None)
    importance_score = params.get('importance_score', None)
    asset_value_operate = params.get('asset_value_operate', None)
    asset_value_score = params.get('asset_value_score', None)
    assetvalue_to_cost_operate = params.get('assetvalue_to_cost_operate', None)
    assetvalue_to_cost = params.get('assetvalue_to_cost', None)
    storage_capacity_operate = params.get('storage_capacity_operate', None)
    # 默认单位为byte，前端传递的单位为MB
    storage_capacity = params['storage_capacity'] * 1024 * 1024 if params.get('storage_capacity', None) else None
    order_time = params.get('order_time', None) if need_only_uids and (not platform == 'tdw') else None
    order_heat = params.get('order_heat', None) if need_only_uids and (not platform == 'tdw') else None
    order_range = params.get('order_range', None) if need_only_uids and (not platform == 'tdw') else None
    order_importance = params.get('order_importance', None) if need_only_uids and (not platform == 'tdw') else None
    order_asset_value = params.get('order_asset_value', None) if need_only_uids and (not platform == 'tdw') else None
    order_assetvalue_to_cost = (
        params.get('order_assetvalue_to_cost', None) if need_only_uids and (not platform == 'tdw') else None
    )
    order_storage_capacity = (
        params.get('order_storage_capacity', None) if need_only_uids and (not platform == 'tdw') else None
    )
    parent_tag_code = params.get('parent_tag_code', 'all')

    page_size = params.get('page_size', None)
    page = params.get('page', None)

    if platform == 'tdw' and project_id:
        project_id = None
    if data_set_type == 'raw_data' and project_id:
        project_id = None
    standard_content_id = params.get('standard_content_id', None)
    token_pkey = params.get('token_pkey', None)
    token_msg = params.get('token_msg', None)
    # 以上 为输入参数的处理

    if need_detail_type == NEED_DATA_SET_ID_DETAIL:  # 数据字典接口
        # 下面过滤条件的组合，搜索结果为空
        if (
            (platform == TYPE_TDW and data_set_type == TYPE_RAW_DATA)
            or (only_standard and platform == TYPE_TDW)
            or (only_standard and data_set_type == TYPE_RAW_DATA)
            or (only_not_standard and platform == TYPE_TDW)
            or (only_not_standard and data_set_type == TYPE_RAW_DATA)
            or (storage_type == 'tdw' and platform == 'bk_data')
            or (storage_type and data_set_type == TYPE_RAW_DATA)
            or (range_operate and range_score and platform == TYPE_TDW)
            or (heat_operate and heat_score and platform == TYPE_TDW)
            or (importance_operate and importance_score and platform == TYPE_TDW)
            or (asset_value_operate and asset_value_score and platform == TYPE_TDW)
            or (assetvalue_to_cost_operate and assetvalue_to_cost and platform == TYPE_TDW)
            or (storage_capacity_operate and storage_capacity and platform == TYPE_TDW)
        ):
            return gen_zero_data_result()

    is_virtual_other_node_pre = False
    is_virtual_other_not_standard_node_pre = False
    if single_tag_code.startswith(VIRTUAL_OTHER_NODE_PRE):
        is_virtual_other_node_pre = True
        single_tag_code = single_tag_code.replace(VIRTUAL_OTHER_NODE_PRE, '', 1)
    elif single_tag_code.startswith(VIRTUAL_OTHER_NOT_STANDARD_NODE_PRE):
        is_virtual_other_not_standard_node_pre = True
        single_tag_code = single_tag_code.replace(VIRTUAL_OTHER_NOT_STANDARD_NODE_PRE, '', 1)

    business_code_list, system_code_list, desc_code_list = judge_tag_type(conn, tag_codes_list)

    result_dict = {}  # 返回的结果
    # 将过滤条件拼接到GrapgQL语句中,目前看到只用于"选了标准以后的数据字典列表"和"数据地图右侧统计汇总指标"
    # 需要过滤的数据集列表
    d_query_uids, final_entites_name = dgraph_cond_uids(
        bk_biz_id,
        project_id,
        keyword,
        platform,
        business_code_list,
        system_code_list,
        desc_code_list,
        only_standard=only_standard,
        created_by=created_by,
        created_at_start=created_at_start,
        created_at_end=created_at_end,
        only_not_standard=only_not_standard,
    )
    if me_type == CAL_TYPE_STANDARD:  # 统计数据标准节点的相关指标
        if platform == TYPE_TDW:  # TDW平台
            floating_zero_result(result_dict)
            return result_dict
        else:
            # 在DmTaskDetail中的数据集合
            dgraph_standard_query_uids, standard_entitys_name = dgraph_standard_uids2(
                bk_biz_id,
                project_id,
                keyword,
                platform,
                business_code_list,
                system_code_list,
                desc_code_list,
                final_entites_name,
                order_range,
                order_heat,
            )
            dgraph_standard_query_statement = '{' + d_query_uids + dgraph_standard_query_uids
            # 按照存储类型过滤
            if storage_type:
                dgraph_standard_query_statement, standard_entitys_name = filter_dataset_by_storage_type(
                    dgraph_standard_query_statement, params, standard_entitys_name, storage_type=storage_type
                )
            # 按照热度、广度过滤
            if (
                (range_operate is not None and range_score is not None)
                or (heat_operate is not None and heat_score is not None)
                or (importance_operate is not None and importance_score is not None)
                or (asset_value_operate is not None and asset_value_score is not None)
                or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
                or (storage_capacity_operate is not None and storage_capacity is not None)
            ):
                dgraph_standard_query_statement, standard_entitys_name = filter_dataset_by_lifecycle(
                    dgraph_standard_query_statement,
                    params,
                    standard_entitys_name,
                    range_operate=range_operate,
                    range_score=range_score,
                    heat_operate=heat_operate,
                    heat_score=heat_score,
                    importance_operate=importance_operate,
                    importance_score=importance_score,
                    asset_value_operate=asset_value_operate,
                    asset_value_score=asset_value_score,
                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                    assetvalue_to_cost=assetvalue_to_cost,
                    storage_capacity_operate=storage_capacity_operate,
                    storage_capacity=storage_capacity,
                )
            # 统计数据量
            dgraph_standard_query_statement += get_single_standard_query2(
                single_tag_code,
                standard_entitys_name,
                is_singel_detail=True,
                order_range=order_range,
                order_heat=order_heat,
                range_operate=range_operate,
                range_score=range_score,
                heat_operate=heat_operate,
                heat_score=heat_score,
                importance_operate=importance_operate,
                importance_score=importance_score,
                asset_value_operate=asset_value_operate,
                asset_value_score=asset_value_score,
                assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                assetvalue_to_cost=assetvalue_to_cost,
                storage_capacity_operate=storage_capacity_operate,
                storage_capacity=storage_capacity,
                standard_content_id=standard_content_id,
            )
            if 'tmp_rt_count as DmTaskDetail.data_set' in dgraph_standard_query_statement:
                standard_entitys_name = 'tmp_rt_count'

            if (need_detail_type == NEED_DATA_SET_ID_DETAIL and need_only_uids) or need_all_data_dict_list:

                # 修改排序逻辑
                dgraph_standard_query_statement = gen_data_set_details_query(
                    dgraph_standard_query_statement,
                    params,
                    standard_entitys_name,
                    order_time,
                    order_heat,
                    order_range,
                    order_assetvalue_to_cost,
                    order_importance,
                    order_asset_value,
                    order_storage_capacity,
                )
            # 数据盘点接口
            if need_detail_type == NEED_DATA_SET_ID_DETAIL and (
                data_source_distribute or data_source_detail_distribute or data_type_distribute
            ):
                dgraph_standard_query_ret = gen_data_source_type_distr_query(
                    dgraph_standard_query_statement,
                    params,
                    standard_entitys_name,
                    data_source_distribute,
                    data_source_detail_distribute,
                    data_type_distribute,
                    parent_tag_code,
                )
                return dgraph_standard_query_ret
            # 桑基图
            if need_detail_type == NEED_DATA_SET_ID_DETAIL and sankey_diagram_distribute:
                dgraph_query_ret = gen_sankey_diagram_distribute_query(
                    dgraph_standard_query_statement, params, standard_entitys_name
                )
                return dgraph_query_ret

            dgraph_standard_query_statement += '}'
            get_floating_dgraph_result(
                dgraph_standard_query_statement,
                result_dict,
                platform,
                need_detail_type=need_detail_type,
                page=page,
                page_size=page_size,
                token_pkey=token_pkey,
                token_msg=token_msg,
            )
            return result_dict
    lifecycle_metric_dict = dict(
        data_set_type=data_set_type,
        only_standard=only_standard,
        need_only_uids=need_only_uids,
        order_range=order_range,
        order_heat=order_heat,
        order_assetvalue_to_cost=order_assetvalue_to_cost,
        order_importance=order_importance,
        order_asset_value=order_asset_value,
        order_storage_capacity=order_storage_capacity,
        created_at_start=created_at_start,
        created_at_end=created_at_end,
        range_operate=range_operate,
        range_score=range_score,
        heat_operate=heat_operate,
        heat_score=heat_score,
        importance_operate=importance_operate,
        importance_score=importance_score,
        asset_value_operate=asset_value_operate,
        asset_value_score=asset_value_score,
        assetvalue_to_cost_operate=assetvalue_to_cost_operate,
        assetvalue_to_cost=assetvalue_to_cost,
        storage_capacity_operate=storage_capacity_operate,
        storage_capacity=storage_capacity,
        storage_type=storage_type,
        only_not_standard=only_not_standard,
    )
    if (single_tag_code == VIRTUAL_DATA_MART_ROOT_NAME) and (
        not business_code_list and not system_code_list and not desc_code_list
    ):  # [数据集市]节点,没有选中标签的情况下
        # 用户没有选择具体标签的情况下,计算[数据集市]节点
        data_mart_dgraph_query, final_filter_uids_name, type_uids_dict = dgraph_data_mart_uids2(
            bk_biz_id,
            project_id,
            keyword,
            platform,
            dgrah_cal_type=DGRAPH_FLOATING,
            need_detail_type=need_detail_type,
            created_by=created_by,
            **lifecycle_metric_dict,
        )
        data_mart_dgraph_query_final = '{\n' + data_mart_dgraph_query
        # 如果是数据字典列表接口
        if (need_detail_type == NEED_DATA_SET_ID_DETAIL and need_only_uids) or need_all_data_dict_list:
            # 按照存储类型过滤
            if storage_type:
                data_mart_dgraph_query_final, final_filter_uids_name = filter_dataset_by_storage_type(
                    data_mart_dgraph_query_final, params, final_filter_uids_name, storage_type=storage_type
                )
            # 按照热度、广度过滤
            if (
                (range_operate is not None and range_score is not None)
                or (heat_operate is not None and heat_score is not None)
                or (importance_operate is not None and importance_score is not None)
                or (asset_value_operate is not None and asset_value_score is not None)
                or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
                or (storage_capacity_operate is not None and storage_capacity is not None)
            ):
                data_mart_dgraph_query_final, final_filter_uids_name = filter_dataset_by_lifecycle(
                    data_mart_dgraph_query_final,
                    params,
                    final_filter_uids_name,
                    range_operate=range_operate,
                    range_score=range_score,
                    heat_operate=heat_operate,
                    heat_score=heat_score,
                    importance_operate=importance_operate,
                    importance_score=importance_score,
                    asset_value_operate=asset_value_operate,
                    asset_value_score=asset_value_score,
                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                    assetvalue_to_cost=assetvalue_to_cost,
                    storage_capacity_operate=storage_capacity_operate,
                    storage_capacity=storage_capacity,
                )
            # 修改排序逻辑的地方
            data_mart_dgraph_query_final = gen_data_set_details_query(
                data_mart_dgraph_query_final,
                params,
                final_filter_uids_name,
                order_time,
                order_heat,
                order_range,
                order_assetvalue_to_cost,
                order_importance,
                order_asset_value,
                order_storage_capacity,
            )
        # 数据盘点接口
        if need_detail_type == NEED_DATA_SET_ID_DETAIL and (
            data_source_distribute or data_source_detail_distribute or data_type_distribute
        ):
            data_mart_dgraph_query_final_ret = gen_data_source_type_distr_query(
                data_mart_dgraph_query_final,
                params,
                final_filter_uids_name,
                data_source_distribute,
                data_source_detail_distribute,
                data_type_distribute,
                parent_tag_code,
            )
            return data_mart_dgraph_query_final_ret
        # 桑基图接口
        if need_detail_type == NEED_DATA_SET_ID_DETAIL and sankey_diagram_distribute:
            dgraph_query_ret = gen_sankey_diagram_distribute_query(
                data_mart_dgraph_query_final, params, final_filter_uids_name
            )
            return dgraph_query_ret
        data_mart_dgraph_query_final += '\n}'
        if need_detail_type != NEED_RECENT_DETAIL:
            get_floating_dgraph_result(
                data_mart_dgraph_query_final,
                result_dict,
                platform,
                need_detail_type=need_detail_type,
                page=page,
                page_size=page_size,
                token_pkey=token_pkey,
                token_msg=token_msg,
            )
    elif single_tag_code == DATA_MART_OTHER_NAME:  # [其他]节点
        if only_standard or only_not_standard or business_code_list:  # 选择了只展示标准的数据或选中的标签中有业务标签
            floating_zero_result(result_dict)
        else:
            if not business_code_list and not system_code_list and not desc_code_list:  # 未选择任何标签的情况下
                data_mart_dgraph_uids_query, final_entites_name, type_uids_dict = dgraph_data_mart_uids2(
                    bk_biz_id,
                    project_id,
                    keyword,
                    platform,
                    dgrah_cal_type=DGRAPH_FLOATING,
                    need_detail_type=need_detail_type,
                    created_by=created_by,
                    data_set_type=data_set_type,
                    need_only_uids=True,
                    only_standard=only_standard,
                    order_range=order_range,
                    order_heat=order_heat,
                    order_assetvalue_to_cost=order_assetvalue_to_cost,
                    order_importance=order_importance,
                    order_asset_value=order_asset_value,
                    order_storage_capacity=order_storage_capacity,
                    created_at_start=created_at_start,
                    created_at_end=created_at_end,
                    range_operate=range_operate,
                    range_score=range_score,
                    heat_operate=heat_operate,
                    heat_score=heat_score,
                    importance_operate=importance_operate,
                    importance_score=importance_score,
                    asset_value_operate=asset_value_operate,
                    asset_value_score=asset_value_score,
                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                    assetvalue_to_cost=assetvalue_to_cost,
                    storage_capacity_operate=storage_capacity_operate,
                    storage_capacity=storage_capacity,
                    storage_type=storage_type,
                    only_not_standard=only_not_standard,
                )
            else:  # 若用户选择了具体标签
                data_mart_dgraph_uids_query, final_entites_name, type_uids_dict = dgraph_cond_uids2(
                    single_tag_code,
                    bk_biz_id,
                    project_id,
                    keyword,
                    platform,
                    business_code_list,
                    system_code_list,
                    desc_code_list,
                    created_by=created_by,
                    is_virtual_other_node_pre=is_virtual_other_node_pre,
                    is_virtual_other_not_standard_node_pre=is_virtual_other_not_standard_node_pre,
                    data_set_type=data_set_type,
                    only_standard=only_standard,
                    need_only_uids=True,
                    order_range=order_range,
                    order_heat=order_heat,
                    order_assetvalue_to_cost=order_assetvalue_to_cost,
                    order_storage_capacity=order_storage_capacity,
                    order_importance=order_importance,
                    order_asset_value=order_asset_value,
                    created_at_start=created_at_start,
                    created_at_end=created_at_end,
                    range_operate=range_operate,
                    range_score=range_score,
                    heat_operate=heat_operate,
                    heat_score=heat_score,
                    importance_operate=importance_operate,
                    importance_score=importance_score,
                    asset_value_operate=asset_value_operate,
                    asset_value_score=asset_value_score,
                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                    assetvalue_to_cost=assetvalue_to_cost,
                    storage_capacity_operate=storage_capacity_operate,
                    storage_capacity=storage_capacity,
                    storage_type=storage_type,
                    only_not_standard=only_not_standard,
                )  # 需要过滤的数据集列表

            data_mart_dgraph_uids_query_final = (
                '{\n'
                + data_mart_dgraph_uids_query
                + """
                        my as var(func:eq(Tag.code,"metric_domain")) @recurse{
                        parentids as ~Tag.parent_tag
                        }
                         var(func:uid(parentids,my)){
                           supported_entitys as Tag.targets
                          }
                        """
            )
            data_set_type_map = gen_data_set_map(
                data_set_type,
                only_standard,
                platform=platform,
                project_id=project_id,
                only_not_standard=only_not_standard,
            )
            bk_biz_id_list_uids = ''
            if TYPE_RESULT_TABLE in data_set_type_map:
                data_mart_dgraph_uids_query_final += """
                           tmp_other_rt_uids as var(func:uid(supported_entitys)) @filter(uid($tmp_rt_count))
                           other_rt_uids as var(func:uid($tmp_rt_count)) @filter(not uid(tmp_other_rt_uids)){
                            rt_p as ResultTable.project
                            rt_b as ResultTable.bk_biz
                            }

                            rt_count(func:uid(other_rt_uids)){
                              count(uid)
                            }
                            p_list(func:uid(rt_p)){
                            project_id:ProjectInfo.project_id
                            }
                            """.replace(
                    '$tmp_rt_count', type_uids_dict[D_UID_RESULT_TABLE_KEY]
                )
                bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rt_b')
            if TYPE_RAW_DATA in data_set_type_map:
                data_mart_dgraph_uids_query_final += """
                           tmp_other_rd_uids as var(func:uid(supported_entitys)) @filter(uid($tmp_rd_count))
                           other_rd_uids as var(func:uid($tmp_rd_count)) @filter(not uid(tmp_other_rd_uids)){
                            rd_b as AccessRawData.bk_biz
                            }
                            rd_count(func:uid(other_rd_uids)){
                            count(uid)
                            }
                            """.replace(
                    '$tmp_rd_count', type_uids_dict[D_UID_RAW_DATA_KEY]
                )
                bk_biz_id_list_uids = gen_bk_biz_id_list_uids(bk_biz_id_list_uids, 'rd_b')
            if TYPE_TDW in data_set_type_map:
                data_mart_dgraph_uids_query_final += """
                            tmp_tdw_count as var(func:uid(supported_entitys)) @filter(uid($tdw_count))
                            other_tdw_uids as var(func:uid($tdw_count)) @filter(not uid(tmp_tdw_count))
                            tdw_count(func:uid(other_tdw_uids)){
                            count(uid)
                            }
                            """.replace(
                    '$tdw_count', type_uids_dict[D_UID_TDW_TABLE_KEY]
                )

            if bk_biz_id_list_uids:
                data_mart_dgraph_uids_query_final += gen_bk_biz_id_list_query(bk_biz_id_list_uids)

            if (need_detail_type == NEED_DATA_SET_ID_DETAIL and need_only_uids) or need_all_data_dict_list:
                # 按照存储类型过滤
                if storage_type:
                    data_mart_dgraph_uids_query_final, final_entites_name = filter_dataset_by_storage_type(
                        data_mart_dgraph_uids_query_final, params, final_entites_name, storage_type=storage_type
                    )
                # 按照热度、广度过滤
                if (
                    (range_operate is not None and range_score is not None)
                    or (heat_operate is not None and heat_score is not None)
                    or (importance_operate is not None and importance_score is not None)
                    or (asset_value_operate is not None and asset_value_score is not None)
                    or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
                    or (storage_capacity_operate is not None and storage_capacity is not None)
                ):
                    data_mart_dgraph_uids_query_final, final_entites_name = filter_dataset_by_lifecycle(
                        data_mart_dgraph_uids_query_final,
                        params,
                        final_entites_name,
                        range_operate=range_operate,
                        range_score=range_score,
                        heat_operate=heat_operate,
                        heat_score=heat_score,
                        importance_operate=importance_operate,
                        importance_score=importance_score,
                        asset_value_operate=asset_value_operate,
                        asset_value_score=asset_value_score,
                        assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                        assetvalue_to_cost=assetvalue_to_cost,
                        storage_capacity_operate=storage_capacity_operate,
                        storage_capacity=storage_capacity,
                    )
                # 修改排序逻辑
                data_mart_dgraph_uids_query_final = gen_data_set_details_query(
                    data_mart_dgraph_uids_query_final,
                    params,
                    final_entites_name,
                    order_time,
                    order_heat,
                    order_range,
                    order_assetvalue_to_cost,
                    order_importance,
                    order_asset_value,
                    order_storage_capacity,
                )
            # 数据盘点接口
            if need_detail_type == NEED_DATA_SET_ID_DETAIL and (
                data_source_distribute or data_source_detail_distribute or data_type_distribute
            ):
                data_mart_dgraph_uids_query_final_ret = gen_data_source_type_distr_query(
                    data_mart_dgraph_uids_query_final,
                    params,
                    final_entites_name,
                    data_source_distribute,
                    data_source_detail_distribute,
                    data_type_distribute,
                    parent_tag_code,
                )
                return data_mart_dgraph_uids_query_final_ret
            # 桑基图接口
            if need_detail_type == NEED_DATA_SET_ID_DETAIL and sankey_diagram_distribute:
                dgraph_query_ret = gen_sankey_diagram_distribute_query(
                    data_mart_dgraph_uids_query_final, params, final_entites_name
                )
                return dgraph_query_ret
            data_mart_dgraph_uids_query_final += '\n}'

            get_floating_dgraph_result(
                data_mart_dgraph_uids_query_final,
                result_dict,
                platform,
                need_detail_type=need_detail_type,
                page=page,
                page_size=page_size,
                token_pkey=token_pkey,
                token_msg=token_msg,
            )
    else:
        d_query_uids, final_entites_name, type_uids_dict = dgraph_cond_uids2(
            single_tag_code,
            bk_biz_id,
            project_id,
            keyword,
            platform,
            business_code_list,
            system_code_list,
            desc_code_list,
            created_by=created_by,
            is_virtual_other_node_pre=is_virtual_other_node_pre,
            is_virtual_other_not_standard_node_pre=is_virtual_other_not_standard_node_pre,
            **lifecycle_metric_dict,
        )  # 需要过滤的数据集列表
        d_tag_query_statement = '{' + d_query_uids
        if only_standard or only_not_standard:
            pass
        elif CAL_TYPE_STANDARD in cal_type_list and has_standard == 1:
            standard_count_query = gen_dgraph_standard_count(conn, type_uids_dict[D_UID_RESULT_TABLE_KEY])
            if standard_count_query:
                d_tag_query_statement += standard_count_query

        if (need_detail_type == NEED_DATA_SET_ID_DETAIL and need_only_uids) or need_all_data_dict_list:
            # 按照存储类型过滤
            if storage_type:
                d_tag_query_statement, final_entites_name = filter_dataset_by_storage_type(
                    d_tag_query_statement, params, final_entites_name, storage_type=storage_type
                )
            # 按照热度、广度过滤
            if (
                (range_operate is not None and range_score is not None)
                or (heat_operate is not None and heat_score is not None)
                or (importance_operate is not None and importance_score is not None)
                or (asset_value_operate is not None and asset_value_score is not None)
                or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
                or (storage_capacity_operate is not None and storage_capacity is not storage_capacity)
            ):
                d_tag_query_statement, final_entites_name = filter_dataset_by_lifecycle(
                    d_tag_query_statement,
                    params,
                    final_entites_name,
                    range_operate=range_operate,
                    range_score=range_score,
                    heat_operate=heat_operate,
                    heat_score=heat_score,
                    importance_operate=importance_operate,
                    importance_score=importance_score,
                    asset_value_operate=asset_value_operate,
                    asset_value_score=asset_value_score,
                    assetvalue_to_cost_operate=assetvalue_to_cost_operate,
                    assetvalue_to_cost=assetvalue_to_cost,
                    storage_capacity_operate=storage_capacity_operate,
                    storage_capacity=storage_capacity,
                )
            d_tag_query_statement = gen_data_set_details_query(
                d_tag_query_statement,
                params,
                final_entites_name,
                order_time,
                order_heat,
                order_range,
                order_assetvalue_to_cost,
                order_importance,
                order_asset_value,
                order_storage_capacity,
            )
        # 数据盘点接口
        if need_detail_type == NEED_DATA_SET_ID_DETAIL and (
            data_source_distribute or data_source_detail_distribute or data_type_distribute
        ):
            d_tag_query_statement_ret = gen_data_source_type_distr_query(
                d_tag_query_statement,
                params,
                final_entites_name,
                data_source_distribute,
                data_source_detail_distribute,
                data_type_distribute,
                parent_tag_code,
            )
            return d_tag_query_statement_ret
        # 桑基图接口
        if need_detail_type == NEED_DATA_SET_ID_DETAIL and sankey_diagram_distribute:
            dgraph_query_ret = gen_sankey_diagram_distribute_query(d_tag_query_statement, params, final_entites_name)
            return dgraph_query_ret
        d_tag_query_statement += '}'

        if need_detail_type != NEED_RECENT_DETAIL:
            get_floating_dgraph_result(
                d_tag_query_statement,
                result_dict,
                platform,
                need_detail_type=need_detail_type,
                page=page,
                page_size=page_size,
                token_pkey=token_pkey,
                token_msg=token_msg,
            )
            if only_standard:
                result_dict['standard_dataset_count'] = result_dict['dataset_count']
            elif only_not_standard:
                result_dict['standard_dataset_count'] = 0

    # ======以下是数据地图右侧统计汇总数据指标======
    if need_detail_type == NEED_RECENT_DETAIL:  # 等于[数据集市]节点的值
        day_list, day_list_dgraph = get_day_list_dgraph()
        recent_dgraph_uids = ''
        recent_type_uids_dict = {}
        if not business_code_list and not system_code_list and not desc_code_list:
            recent_dgraph_uids = data_mart_dgraph_query
            recent_type_uids_dict = type_uids_dict
        else:
            recent_dgraph_uids = d_query_uids
            recent_type_uids_dict = type_uids_dict

        recent_dgraph_uids = dgraph_get_project_count(recent_dgraph_uids)

        recent_dgraph_query = '{\n' + recent_dgraph_uids

        if not only_standard:
            standard_count_query = gen_dgraph_standard_count(conn, type_uids_dict[D_UID_RESULT_TABLE_KEY])
            if standard_count_query:
                recent_dgraph_query += standard_count_query

        rt_key_name_list, rd_key_name_list, tdw_key_name_list, standard_key_name_list = None, None, None, None
        if need_recent_detail_list and recent_type_uids_dict:
            if D_UID_RAW_DATA_KEY in recent_type_uids_dict:
                tmp_day_count, rd_key_name_list = gen_recent_dgraph_query(
                    day_list_dgraph, 'rd', type_uids_dict[D_UID_RAW_DATA_KEY], 'created_at'
                )
                recent_dgraph_query += tmp_day_count

            if D_UID_TDW_TABLE_KEY in recent_type_uids_dict:
                tmp_day_count, tdw_key_name_list = gen_recent_dgraph_query(
                    day_list_dgraph, 'tdw', type_uids_dict[D_UID_TDW_TABLE_KEY], 'created_at'
                )
                recent_dgraph_query += tmp_day_count

            if D_UID_RESULT_TABLE_KEY in recent_type_uids_dict:
                recent_rt_query, rt_key_name = get_recent_rt_query(
                    day_list_dgraph, type_uids_dict[D_UID_RESULT_TABLE_KEY]
                )
                recent_dgraph_query += recent_rt_query

                tmp_day_count, rt_key_name_list = gen_recent_dgraph_query(
                    day_list_dgraph, 'rt', rt_key_name, 'created_at'
                )
                recent_dgraph_query += tmp_day_count

                if not only_standard:
                    tmp_day_count, standard_key_name_list = gen_recent_dgraph_query(
                        day_list_dgraph, 'standard', 'supported_entitys_st', 'created_at'
                    )
                    recent_dgraph_query += tmp_day_count

        recent_dgraph_query += '\n}'
        (
            rt_recent_result_list,
            rd_recent_result_list,
            tdw_recent_result_list,
            standard_recent_result_list,
            standard_count,
        ) = get_recent_dgraph_result(
            recent_dgraph_query,
            result_dict,
            platform,
            need_detail_type=need_detail_type,
            rt_key_name_list=rt_key_name_list,
            rd_key_name_list=rd_key_name_list,
            tdw_key_name_list=tdw_key_name_list,
            standard_key_name_list=standard_key_name_list,
            token_pkey=token_pkey,
            token_msg=token_msg,
        )

        result_dict['standard_dataset_count'] = result_dict['dataset_count'] if only_standard else standard_count
        if need_recent_detail_list:
            parse_recent_rt_result(result_dict, rt_recent_result_list, day_list)
            parse_recent_rt_result(result_dict, tdw_recent_result_list, day_list)
            parse_recent_ds_result(result_dict, rd_recent_result_list, day_list)
            if only_standard:  # 仅统计标准数据
                parse_recent_standard_result(result_dict, rt_recent_result_list, day_list)
            else:
                parse_recent_standard_result(result_dict, standard_recent_result_list, day_list)

    return result_dict


def get_recent_rt_query(day_list_dgraph, uids_name):
    """
    整体/满足搜索条件近n天rt查询语句
    :param day_list_dgraph: {List} 最近n天时间列表
    :param uids_name: {String} 满足条件的rt对应的变量
    :return: dgraph_query {String}:近n天rt查询语句, key_name {String}:近n天满足条件的rt对应的变量
    """
    # 1) 近n天的启始时间
    start_time = day_list_dgraph[0][1]
    end_time = day_list_dgraph[-1][2]

    # 2) 拼接近n天创建rt的查询语句
    key_name = 'recent_rt_uids_dm'
    dgraph_query = """
        $key_name as var(func:uid($filter_uids)) @filter( gt(created_at,"$day1") AND le(created_at,"$day2"))
    """
    dgraph_query = dgraph_query.replace('$key_name', key_name)
    dgraph_query = dgraph_query.replace('$filter_uids', uids_name)
    dgraph_query = dgraph_query.replace('$day1', start_time)
    dgraph_query = dgraph_query.replace('$day2', end_time)
    return dgraph_query, key_name


def gen_data_set_details_query(
    query_statement,
    params,
    final_entites_name,
    order_time=None,
    order_heat=None,
    order_range=None,
    order_assetvalue_to_cost=None,
    order_importance=None,
    order_asset_value=None,
    order_storage_capacity=None,
):
    """
    排序修改逻辑的地方
    :param query_statement:
    :param params:
    :param final_entites_name:
    :return:
    """
    query_statement = dgraph_get_project_count(query_statement)
    page_size = params['page_size']
    offset = (params['page'] - 1) * page_size
    # 如果按照热度/广度排序的话，修改query_statement

    if order_time == 'asc':
        query_statement += """
            data_set_id_list(func:uid($final_filter_uids_name),orderasc:updated_at,first:$page_size,offset:$offset){
              ResultTable.result_table_id
              AccessRawData.id
             }
        """
    elif (
        order_range == 'asc'
        or order_range == 'desc'
        or order_heat == 'asc'
        or order_heat == 'desc'
        or order_importance == 'asc'
        or order_importance == 'desc'
        or order_asset_value == 'asc'
        or order_asset_value == 'desc'
    ):
        query_statement += """
            lifecycle_uids_filter as var (func:uid($lifecycle_uids_name)) @cascade {
              $metric {
                LifeCycle.target @filter(uid($final_filter_uids_name))
              }
            }
            data_set_id_list(func:uid($lifecycle_uids_name),$asc_or_desc:$score,first:$page_size,offset:$offset)
            @filter(uid(lifecycle_uids_filter)){
              $metric {
                LifeCycle.target @filter(uid($final_filter_uids_name)) {
                  ResultTable.result_table_id
                  AccessRawData.id
                }
              }
            }
        """
        query_statement = lifecycle_order_format(
            order_range,
            order_heat,
            order_assetvalue_to_cost,
            order_importance,
            order_asset_value,
            order_storage_capacity,
            query_statement,
            params.get('data_set_type'),
        )
    elif order_assetvalue_to_cost == 'asc' or order_assetvalue_to_cost == 'desc':
        query_statement += """
            lifecycle_uids_filter as var (func:uid($lifecycle_uids_name)) @cascade {
              LifeCycle.target @filter(uid($final_filter_uids_name))
            }
            data_set_id_list(func:uid($lifecycle_uids_name),$asc_or_desc:$score,first:$page_size,offset:$offset)
            @filter(uid(lifecycle_uids_filter)) {
              LifeCycle.target @filter(uid($final_filter_uids_name)) {
                ResultTable.result_table_id
                AccessRawData.id
              }
            }
        """
        query_statement = lifecycle_order_format(
            order_range,
            order_heat,
            order_assetvalue_to_cost,
            order_importance,
            order_asset_value,
            order_storage_capacity,
            query_statement,
            params.get('data_set_type'),
        )
    elif order_storage_capacity == 'asc' or order_storage_capacity == 'desc':
        query_statement += """
            lifecycle_uids_filter as var (func:uid($lifecycle_uids_name)) @cascade {
              ~Cost.capacity {
                ~LifeCycle.cost {
                  LifeCycle.target @filter(uid($final_filter_uids_name))
                }
              }
            }
            data_set_id_list(func:uid($lifecycle_uids_name),$asc_or_desc:$score,first:$page_size,offset:$offset)
            @filter(uid(lifecycle_uids_filter)){
              ~Cost.capacity {
                ~LifeCycle.cost {
                  LifeCycle.target @filter(uid($final_filter_uids_name)) {
                    ResultTable.result_table_id
                    AccessRawData.id
                  }
                }
              }
            }
        """
        query_statement = lifecycle_order_format(
            order_range,
            order_heat,
            order_assetvalue_to_cost,
            order_importance,
            order_asset_value,
            order_storage_capacity,
            query_statement,
            params.get('data_set_type'),
        )
    # 默认按照时间倒序去查
    else:
        query_statement += """
            data_set_id_list(func:uid($final_filter_uids_name),orderdesc:updated_at,first:$page_size,offset:$offset){
              ResultTable.result_table_id
              TdwTable.table_id
              AccessRawData.id
            }
        """

    query_statement = query_statement.replace('$final_filter_uids_name', final_entites_name)
    query_statement = query_statement.replace('$page_size', str(page_size))
    query_statement = query_statement.replace('$offset', str(offset))
    return query_statement


def gen_data_source_type_distr_query(
    query_statement,
    params,
    final_entites_name,
    data_source_distribute=False,
    data_source_detail_distribute=False,
    data_type_distribute=False,
    parent_tag=None,
):
    """
    数据来源/数据类型分布查询
    :param query_statement:
    :param params:
    :param final_entites_name:
    :return:
    """
    query_statement = dgraph_get_project_count(query_statement)
    data_source_tag_list = [
        {'tag_code': 'components', 'tag_alias': _('组件')},
        {'tag_code': 'sys_host', 'tag_alias': _('设备')},
        {'tag_code': 'system', 'tag_alias': _('系统')},
    ]

    if data_source_detail_distribute:
        # 数据来源分布的详情
        if parent_tag == 'all':
            sql = """select code as tag_code, alias as tag_alias from tag where parent_id in (select id from tag
                      where code in ('components', 'sys_host', 'system'))"""
        else:
            sql = """select code as tag_code, alias as tag_alias from tag where parent_id in (select id from tag
                     where code = '{}') or code = '{}'""".format(
                parent_tag, parent_tag
            )
        data_source_tag_list = tagaction.query_direct_sql_to_map_list(connections['bkdata_basic_slave'], sql)
    elif data_type_distribute:
        sql = """select code as tag_code, alias as tag_alias from tag where parent_id in
                (select id from tag where code = 'data_type')"""
        data_source_tag_list = tagaction.query_direct_sql_to_map_list(connections['bkdata_basic_slave'], sql)

    for each_tag in data_source_tag_list:
        query_statement += get_single_tag_query(
            each_tag.get('tag_code'), '$final_filter_uids_name', need_me_count=False
        )

    query_statement = query_statement.replace('$final_filter_uids_name', final_entites_name)
    query_statement += '\n}'
    dgraph_result = meta_dgraph_complex_search(query_statement, return_original=True)

    return {'data_source_tag_list': data_source_tag_list, 'dgraph_result': dgraph_result}


def gen_sankey_diagram_distribute_query(query_statement, params, final_entites_name):
    """
    桑基图数据分布查询
    :param query_statement:
    :param params:
    :param final_entites_name:
    :return:
    """
    query_statement = dgraph_get_project_count(query_statement)

    # 第一层的节点
    first_level_sql = """select a.code as tag_code, a.alias as tag_alias, a.id as tag_id, b.code as parent_code
        from tag as a, tag as b where a.parent_id in (select id from tag where code in ('techops', 'bissness'))
        and a.parent_id = b.id and a.kpath = 1;"""
    first_level_list = tagaction.query_direct_sql_to_map_list(connections['bkdata_basic_slave'], first_level_sql)

    # 第二层的节点
    first_level_id_tuple = tuple([each_tag['tag_id'] for each_tag in first_level_list])
    second_level_sql = """select a.code as tag_code, a.alias as tag_alias, a.id as tag_id, b.code as parent_code
        from tag as a, tag as b where a.parent_id in {} and a.parent_id = b.id and a.kpath = 1;""".format(
        first_level_id_tuple
    )
    second_level_list = tagaction.query_direct_sql_to_map_list(connections['bkdata_basic_slave'], second_level_sql)

    # graphQL
    for each_tag in first_level_list:
        query_statement += get_single_tag_query(
            each_tag.get('tag_code'), '$final_filter_uids_name', need_me_count=False
        )
    # 其他节点和processing_type之间的对应关系
    query_statement += get_other_tag_query(first_level_list, '$final_filter_uids_name')

    for each_tag in second_level_list:
        query_statement += get_single_tag_query(
            each_tag.get('tag_code'), '$final_filter_uids_name', need_me_count=False, need_processing_type=True
        )

    query_statement = query_statement.replace('$final_filter_uids_name', final_entites_name)
    query_statement += '\n}'
    dgraph_result = meta_dgraph_complex_search(query_statement, return_original=True)
    return {
        'first_level_tag_list': first_level_list,
        'second_level_tag_list': second_level_list,
        'dgraph_result': dgraph_result,
    }


def gen_dgraph_standard_count(conn, standard_filter_uids):
    standard_count_query = get_all_standard_query()
    standard_count_query += """
    supported_entitys_st as var(func:uid(all_st_data_set_uids)) @filter(uid($standard_filter_uids))
    standard_count(func:uid(supported_entitys_st)){
    count(uid)
    }
    """
    standard_count_query = standard_count_query.replace('$standard_filter_uids', standard_filter_uids)
    return standard_count_query


def gen_recent_dgraph_query(day_list_dgraph, type_name_pre, uids_name, field_name):
    dgraph_query = ''
    key_name_list = []
    for dgrah_day in day_list_dgraph:
        key_name = 'recent_' + type_name_pre + '_' + dgrah_day[0]
        tmp_day_count = """
          $key_name(func:uid($filter_uids)) @filter( gt($field_name,"$day1") AND le($field_name,"$day2")){
          count(uid)
         }
        """
        tmp_day_count = tmp_day_count.replace('$key_name', key_name)
        tmp_day_count = tmp_day_count.replace('$filter_uids', uids_name)
        tmp_day_count = tmp_day_count.replace('$day1', dgrah_day[1])
        tmp_day_count = tmp_day_count.replace('$day2', dgrah_day[2])
        tmp_day_count = tmp_day_count.replace('$field_name', field_name)
        dgraph_query += tmp_day_count
        key_name_list.append(key_name)
    return dgraph_query, key_name_list


def clear_bk_biz_id_list(result_dict):
    if 'bk_biz_list' in result_dict:
        del result_dict['bk_biz_list']
