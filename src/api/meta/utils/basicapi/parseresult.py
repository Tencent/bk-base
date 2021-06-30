# -*- coding: utf-8 -*-
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


import json
import time

import pymysql
from common.meta.common import create_tag_to_target, get_default_geog_tag
from django.conf import settings
from meta.public.common import field_allowed_roles
from meta.utils.basicapi import BasicApi
from meta.utils.common import paged_sql

MYSQL_BACKEND_TYPE = "mysql"
ATLAS_BACKEND_TYPE = "atlas"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%i:%s"

TAG_TYPE_RESULT_TABLE = "result_table"
TAG_TYPE_RAW_DATA = "raw_data"
TAG_TYPE_PROJECT = "project"
TAG_TYPE_DATA_PROCESSING = "data_processing"
TAG_TYPE_DATA_PROCESSING_RELATION = "data_processing_relation"
TAG_TYPE_DATA_TRANSFERRING = "data_transferring"
TAG_TYPE_CLUSTER_GROUP = "cluster_group"

VIRTUAL_BELONG_TO_FIELD = "belong_to"


def get_tag_params(params):
    tags = params.pop("tags", [])
    return tags


def create_tag_to_result_table(tags, bk_biz_id, project_id, *result_table_ids):
    tag_to_target(TAG_TYPE_RESULT_TABLE, tags, bk_biz_id, project_id, *result_table_ids)


def create_tag_to_raw_data(tags, bk_biz_id, *raw_data_ids):
    tag_to_target(TAG_TYPE_RAW_DATA, tags, bk_biz_id, None, *raw_data_ids)


def create_tag_to_data_processing(tags, *raw_data_ids):
    tag_to_target(TAG_TYPE_DATA_PROCESSING, tags, None, None, *raw_data_ids)


def create_tag_to_dpr(tags, *data_processing_ids):
    tag_to_target(TAG_TYPE_DATA_PROCESSING_RELATION, tags, None, None, *data_processing_ids)


def create_tag_to_data_transferring(tags, *raw_data_ids):
    tag_to_target(TAG_TYPE_DATA_TRANSFERRING, tags, None, None, *raw_data_ids)


def create_tag_to_project(tags, *project_id_ids):
    tag_to_target(TAG_TYPE_PROJECT, tags, None, None, *project_id_ids)


def create_tag_to_cluster_group(tags, *cluster_group_ids):
    tag_to_target(TAG_TYPE_CLUSTER_GROUP, tags, None, None, *cluster_group_ids)


def tag_to_target(target_type, tags, bk_biz_id, project_id, *target_ids):
    targets = []
    for target_id in target_ids:
        targets.append((target_type, target_id))
    create_tag_to_target(targets, tags, target_exists=False, bk_biz_id=bk_biz_id, project_id=project_id)


def add_manage_tag_to_project(result):
    return manage_tag_info(TAG_TYPE_PROJECT, result, "project_id")


def add_manage_tag_to_result_table(result):
    return manage_tag_info(TAG_TYPE_RESULT_TABLE, result, "result_table_id")


def add_manage_tag_to_data_processing(result):
    return manage_tag_info(TAG_TYPE_DATA_PROCESSING, result, "processing_id")


def add_manage_tag_to_data_transferring(result):
    return manage_tag_info(TAG_TYPE_DATA_TRANSFERRING, result, "transferring_id")


def add_manage_tag_to_cluster_group(result):
    return manage_tag_info(TAG_TYPE_CLUSTER_GROUP, result, "cluster_group_id")


def gen_managed_tag_dict(sql):
    tag_dict = {}
    tag_id_obj_dict = {}
    tag_list = BasicApi.get_complex_search(sql)
    # 转换标签,为每一个tag添加它所属的大类,并虚拟命名为:belong_to字段
    if tag_list:
        # 构建标签关系树
        for tag_obj in tag_list:
            tag_id = tag_obj["id"]
            parent_id = tag_obj["parent_id"]
            tag_code = tag_obj["code"]
            if parent_id == 0:  # 根节点
                tag_obj[VIRTUAL_BELONG_TO_FIELD] = tag_code
            tag_id_obj_dict[tag_id] = tag_obj
        # 从树中检索继承关系
        for tag_obj in tag_list:
            if VIRTUAL_BELONG_TO_FIELD not in tag_obj:
                parent_id = tag_obj["parent_id"]
                while True:
                    parent_dict = tag_id_obj_dict.get(parent_id)
                    if not parent_dict:
                        break
                    if VIRTUAL_BELONG_TO_FIELD in parent_dict:
                        tag_obj[VIRTUAL_BELONG_TO_FIELD] = parent_dict[VIRTUAL_BELONG_TO_FIELD]
                        break
                    parent_id = parent_dict["parent_id"]
        # 更新标签信息
        for tag_obj in tag_list:
            tag_dict[tag_obj["code"]] = {
                "code": tag_obj["code"],
                "alias": tag_obj["alias"],
                VIRTUAL_BELONG_TO_FIELD: tag_obj.get(VIRTUAL_BELONG_TO_FIELD),
            }
    return tag_dict


def gen_managed_tag_target_dict(sql, tag_dict):
    tag_target_dict = {}
    tag_target_list = BasicApi.get_complex_search(sql)
    if tag_target_list:
        for tag_target_obj in tag_target_list:
            target_id = tag_target_obj["target_id"]
            tag_code = tag_target_obj["tag_code"]
            ttt_dict = tag_dict.get(tag_code)
            if ttt_dict:
                belong_to = ttt_dict[VIRTUAL_BELONG_TO_FIELD]
                cp_ttt_dict = ttt_dict.copy()
                cp_ttt_dict.pop(VIRTUAL_BELONG_TO_FIELD)
                tg_dict = tag_target_dict.get(target_id)
                if not tg_dict:
                    tag_target_dict[target_id] = {belong_to: [cp_ttt_dict]}
                elif belong_to in tg_dict:
                    tg_dict[belong_to].append(cp_ttt_dict)
                else:
                    tg_dict[belong_to] = [cp_ttt_dict]
    return tag_target_dict


def manage_tag_info(target_type, result, data_set_name):  # 给返回结果添加上管理标签信息
    if not result:
        return

    if settings.ENABLED_TDW:
        from meta.extend.tencent.lol.utils import mixin_lol_manage_tag_info

    default_geog_area_dict = None
    if not settings.MULTI_GEOG_AREA:  # 非多地域,直接返回默认标签
        dgt_dict = get_default_geog_tag()
        if dgt_dict:
            default_geog_area_dict = {"code": dgt_dict["code"], "alias": dgt_dict["alias"]}
        if isinstance(result, list):
            for obj in result:
                obj["tags"] = {"manage": {"geog_area": [default_geog_area_dict]}}
                if (
                    settings.ENABLED_TDW
                    and target_type == TAG_TYPE_PROJECT
                    and obj.get("project_id", 0) == settings.LOL_BILLING_PRJ_ID
                ):
                    mixin_lol_manage_tag_info(obj)
        elif isinstance(result, dict):
            result["tags"] = {"manage": {"geog_area": [default_geog_area_dict]}}
            if (
                settings.ENABLED_TDW
                and target_type == TAG_TYPE_PROJECT
                and result.get("project_id", 0) == settings.LOL_BILLING_PRJ_ID
            ):
                mixin_lol_manage_tag_info(result)
        return

    target_id_list = []
    if isinstance(result, list):
        for obj in result:
            if data_set_name in obj:
                target_id_list.append(obj[data_set_name])
    elif isinstance(result, dict) and data_set_name in result:
        target_id_list.append(result[data_set_name])
    if not target_id_list:
        return

    # 生成tag字典
    tag_sql = "select id,code,alias,parent_id from tag where active=1 and tag_type='manage' order by parent_id asc"
    tag_dict = gen_managed_tag_dict(tag_sql)
    # 生成tag_target字典 {target_id:{"geog_area":[{"tag_code":"xxx","tag_name":"xxx"}]}}
    where_cond = ",".join("'" + escape_string(str(target_id)) + "'" for target_id in target_id_list)
    tag_target_sql = (
        "select id,target_id,target_type,tag_code from tag_target where active=1 "
        " and tag_type='manage' and tag_code=source_tag_code and target_type='{}' and target_id in({})".format(
            escape_string(target_type), where_cond
        )
    )
    tag_target_dict = gen_managed_tag_target_dict(tag_target_sql, tag_dict)

    # 添加标签信息到结果中
    if isinstance(result, list):
        for obj in result:
            data_set_id = str(obj[data_set_name])
            geog_area_dict = tag_target_dict.get(data_set_id)
            if geog_area_dict:
                if "geog_area" not in geog_area_dict:
                    geog_area_dict["geog_area"] = [default_geog_area_dict]
                obj["tags"] = {"manage": geog_area_dict}
            elif default_geog_area_dict:
                obj["tags"] = {"manage": {"geog_area": [default_geog_area_dict]}}
    elif isinstance(result, dict):
        data_set_id = str(result[data_set_name])
        geog_area_dict = tag_target_dict.get(data_set_id)
        if geog_area_dict:
            if "geog_area" not in geog_area_dict:
                geog_area_dict["geog_area"] = [default_geog_area_dict]
            result["tags"] = {"manage": geog_area_dict}
        elif default_geog_area_dict:
            result["tags"] = {"manage": {"geog_area": [default_geog_area_dict]}}


def escape_string(s):
    return pymysql.converters.escape_string(s)


def cover_storage_detail(cluster_dict, channel_dict, cluster_id_in_cond=None, channel_id_in_cond=None):
    cluster_sql, channel_sql, union_sql = None, None, ""
    if cluster_id_in_cond:
        cluster_id_in_cond = cluster_id_in_cond.lstrip(",").rstrip(",")
        cluster_sql = """select id,cluster_type,'1' as comm from storage_cluster_config where id in ({})""".format(
            cluster_id_in_cond
        )
    if channel_id_in_cond:
        channel_id_in_cond = channel_id_in_cond.lstrip(",").rstrip(",")
        channel_sql = (
            """select id,cluster_type,'2' as comm from databus_channel_cluster_config where id in({})""".format(
                channel_id_in_cond
            )
        )
    if cluster_sql:
        union_sql += cluster_sql
    if channel_sql:
        if union_sql:
            union_sql += " union all "
        union_sql += channel_sql

    if union_sql:
        union_result = BasicApi.get_complex_search(union_sql)
        if union_result:
            for union_obj in union_result:
                comm = union_obj["comm"]
                id_key = union_obj["id"]
                if comm == "1":
                    cluster_dict[id_key] = union_obj
                elif comm == "2":
                    channel_dict[id_key] = union_obj


def get_result_table_storages_v3(in_cond, cluster_type=None, need_storage_detail=True):
    if need_storage_detail and in_cond:
        storage_sql = """select id,physical_table_name,updated_by,generate_type,storage_config,active,priority,
        created_by,data_type,date_format(updated_at,'{0}') updated_at,expires,
        date_format(created_at,'{0}') created_at,description,storage_cluster_config_id,previous_cluster_name,
        storage_channel_id,result_table_id from storage_result_table where active=1
        and result_table_id in ({1})""".format(
            DEFAULT_DATE_FORMAT, in_cond
        )
    elif need_storage_detail and not in_cond:
        storage_sql = """select id,physical_table_name,updated_by,generate_type,storage_config,active,priority,
        created_by,data_type,date_format(updated_at,'{0}') updated_at,expires,
        date_format(created_at,'{0}') created_at,description,storage_cluster_config_id,previous_cluster_name,
        storage_channel_id,result_table_id from storage_result_table where active=1 """.format(
            DEFAULT_DATE_FORMAT
        )
    elif not need_storage_detail and in_cond:  # 不需要storage详情
        storage_sql = """select id,result_table_id,storage_cluster_config_id,storage_channel_id,
        active from storage_result_table where active=1 and result_table_id in ({})""".format(
            in_cond
        )
    else:
        storage_sql = """select id,result_table_id,storage_cluster_config_id,storage_channel_id,
        active from storage_result_table where active=1"""

    storage_rt_id_dict = {}
    cluster_id_in_cond, channel_id_in_cond = "", ""
    storage_result = BasicApi.get_complex_search(storage_sql)
    if not storage_result:
        return storage_rt_id_dict

    # 获取存储cluster id条件列表 & 总线channel id条件列表
    storage_result = parse_field_to_boolean(storage_result, "active")
    for storage_obj in storage_result:
        storage_cluster_config_id = storage_obj["storage_cluster_config_id"]
        storage_channel_id = storage_obj["storage_channel_id"]
        if storage_cluster_config_id is not None:
            if not cluster_id_in_cond:
                cluster_id_in_cond += ","
            if "," + str(storage_cluster_config_id) + "," not in cluster_id_in_cond:  # id去重
                cluster_id_in_cond += str(storage_cluster_config_id) + ","

        if storage_channel_id is not None:
            if not channel_id_in_cond:
                channel_id_in_cond += ","
            if "," + str(storage_channel_id) + "," not in channel_id_in_cond:
                channel_id_in_cond += str(storage_channel_id) + ","

    # 获取存储集群信息
    cluster_dict = {}
    if need_storage_detail and cluster_id_in_cond:
        cluster_id_in_cond = cluster_id_in_cond.lstrip(",").rstrip(",")
        cluster_sql = """select id,id as storage_cluster_config_id,cluster_name,cluster_type,priority,version,
        expires,connection_info,belongs_to,cluster_group from storage_cluster_config where id in ({})""".format(
            cluster_id_in_cond
        )
        if cluster_type:
            cluster_sql += " and cluster_type='" + escape_string(cluster_type) + "'"
        cluster_result = BasicApi.get_complex_search(cluster_sql)
        if cluster_result:
            for cluster_obj in cluster_result:
                storage_cluster_config_id = cluster_obj["storage_cluster_config_id"]
                cluster_dict[storage_cluster_config_id] = cluster_obj

    # 获取总线集群信息
    channel_dict = {}
    if need_storage_detail and channel_id_in_cond:
        channel_id_in_cond = channel_id_in_cond.lstrip(",").rstrip(",")
        channel_sql = """select id,id as channel_cluster_config_id,cluster_backup_ips,attribute,zk_port,
        description,cluster_name,cluster_type,cluster_role,priority,cluster_domain,cluster_port,active,
        zk_domain,zk_root_path from databus_channel_cluster_config where id in ({})""".format(
            channel_id_in_cond
        )
        if cluster_type:
            channel_sql += " and cluster_type='" + escape_string(cluster_type) + "'"
        channel_result = BasicApi.get_complex_search(channel_sql)
        channel_result = parse_field_to_boolean(channel_result, "active")
        if channel_result:
            for channel_obj in channel_result:
                channel_cluster_config_id = channel_obj["channel_cluster_config_id"]
                channel_dict[channel_cluster_config_id] = channel_obj

    if not need_storage_detail:  # 不需要storage详情, 用概要信息覆盖对应结果
        cover_storage_detail(
            cluster_dict, channel_dict, cluster_id_in_cond=cluster_id_in_cond, channel_id_in_cond=channel_id_in_cond
        )

    for storage_obj in storage_result:
        storage_rt_id = storage_obj["result_table_id"]
        storage_cluster_config_id = storage_obj["storage_cluster_config_id"]
        storage_channel_id = storage_obj["storage_channel_id"]
        tmp_storage_rt_id_dict = storage_rt_id_dict.get(storage_rt_id, {})
        tmp_cluster_obj = cluster_dict.get(storage_cluster_config_id, {})
        tmp_channel_obj = channel_dict.get(storage_channel_id, {})
        cluster_type = None

        if tmp_cluster_obj:
            cluster_type = tmp_cluster_obj.get("cluster_type")
        elif tmp_channel_obj:
            cluster_type = tmp_channel_obj.get("cluster_type")

        if cluster_type is None:  # 为了处理有cluster_type参数的情况
            continue

        storage_obj["storage_cluster"] = tmp_cluster_obj
        storage_obj["storage_channel"] = tmp_channel_obj
        if need_storage_detail:
            tmp_storage_rt_id_dict[cluster_type] = storage_obj
        else:  # 不需要storage详情
            tmp_storage_rt_id_dict[cluster_type] = {}
        storage_rt_id_dict[storage_rt_id] = tmp_storage_rt_id_dict
    return storage_rt_id_dict


def get_result_table_fields_v3(in_cond, need_result_table_id=False):
    field_sql = "select "
    if need_result_table_id:
        field_sql += "result_table_id,"
    if in_cond:
        field_sql += """ roles,field_type,description,date_format(created_at,'{0}') created_at,is_dimension,created_by,
        date_format(updated_at,'{0}') updated_at,origins,field_alias,field_name,id,field_index,updated_by
        from result_table_field where result_table_id in ({1}) order by result_table_id asc,field_index asc""".format(
            DEFAULT_DATE_FORMAT, in_cond
        )
    else:
        field_sql += """ roles,field_type,description,date_format(created_at,'{0}') created_at,is_dimension,created_by,
        date_format(updated_at,'{0}') updated_at,origins,field_alias,field_name,id,field_index,updated_by
        from result_table_field order by result_table_id asc,field_index asc""".format(
            DEFAULT_DATE_FORMAT,
        )
    field_result = BasicApi.get_complex_search(field_sql)
    field_result = parse_field_to_boolean(field_result, "is_dimension")
    for item in field_result:
        default_roles = {k: False for k in field_allowed_roles}
        roles = None
        try:
            roles = json.loads(item["roles"])
        except Exception:
            pass
        if roles and isinstance(roles, dict):
            default_roles.update(roles)
        item["roles"] = default_roles
    return field_result


def get_result_table_infos_v3(
    bk_biz_ids=None,
    project_id=None,
    result_table_ids=None,
    related=None,
    processing_type=None,
    page=None,
    page_size=None,
    need_storage_detail=True,
    tags=None,
    only_queryable=True,
):
    sql = """select a.result_table_name,a.bk_biz_id,date_format(a.created_at,'{0}') created_at,a.sensitivity,
    a.result_table_name_alias,a.updated_by,a.created_by,a.result_table_id,a.count_freq,a.description,
    date_format(a.updated_at,'{0}') updated_at,a.generate_type,a.result_table_type,a.processing_type,a.project_id,
    a.platform, a.is_managed,a.count_freq_unit,a.data_category,b.project_name from result_table a
    left join project_info b on a.project_id=b.project_id where 1=1""".format(
        DEFAULT_DATE_FORMAT
    )
    where_cond = ""
    if bk_biz_ids:
        where_cond += " and a.bk_biz_id in ({})".format(",".join(escape_string(bk_biz_id) for bk_biz_id in bk_biz_ids))
    if project_id:
        where_cond += " and a.project_id=" + escape_string(project_id)

    # 只查询可见的rt(queryset不可见)
    if only_queryable:
        if processing_type:
            if processing_type == "queryset":
                return []
            where_cond += " and a.processing_type='" + escape_string(processing_type) + "'"
        else:
            where_cond += " and a.processing_type!='queryset'"
    else:
        if processing_type:
            where_cond += " and a.processing_type='" + escape_string(processing_type) + "'"

    if settings.MULTI_GEOG_AREA and tags:  # 非多地域
        where_cond += (
            " and a.result_table_id in(select target_id from tag_target where tag_code=source_tag_code "
            "and active=1 and target_type='result_table' and tag_type='manage' and source_tag_code in ({}))".format(
                ",".join("'" + escape_string(tag) + "'" for tag in tags)
            )
        )

    if result_table_ids:
        page = None
        page_size = None
        where_cond += " and a.result_table_id in("
        where_cond += "".join("'" + escape_string(rt_id) + "'," for rt_id in result_table_ids)
        where_cond = where_cond.rstrip(",") + ") "
    if where_cond:
        sql += where_cond
    query_result = BasicApi.get_complex_search(paged_sql(sql, page=page, page_size=page_size))
    if only_queryable:
        query_result = [
            rt_obj
            for rt_obj in query_result
            if not (rt_obj["processing_type"] == "queryset" and rt_obj["generate_type"] == "system")
        ]
    if related and query_result:
        if not (result_table_ids or bk_biz_ids or project_id or processing_type):
            in_cond = None
        else:
            in_cond = ",".join("'" + escape_string(rt_obj["result_table_id"]) + "'" for rt_obj in query_result)
        field_rt_id_dict, dp_rt_id_dict, storage_rt_id_dict = {}, {}, None
        if "fields" in related:
            # 不允许拉取fields全表
            if not in_cond:
                pass
            else:
                field_result = get_result_table_fields_v3(in_cond, need_result_table_id=True)
                if field_result:
                    for field_obj in field_result:
                        field_rt_id = field_obj["result_table_id"]
                        del field_obj["result_table_id"]
                        field_list = field_rt_id_dict.get(field_rt_id, [])
                        field_list.append(field_obj)
                        field_rt_id_dict[field_rt_id] = field_list

        if "data_processing" in related:
            if not in_cond:
                dp_sql = """select tmp.*,c.project_name from
                (select a.processing_id,a.project_id,a.processing_alias,a.processing_type,a.description,
                a.generate_type,a.platform,a.created_by,date_format(a.created_at,'{0}') created_at,
                a.updated_by,date_format(a.updated_at,'{0}') updated_at,b.data_set_id
                from data_processing a,data_processing_relation b where a.processing_id=b.processing_id
                and b.data_set_type='result_table' and b.data_directing='output')tmp
                left join project_info c on tmp.project_id=c.project_id""".format(
                    DEFAULT_DATE_FORMAT
                )
            else:
                dp_sql = """select tmp.*,c.project_name from
                (select a.processing_id,a.project_id,a.processing_alias,a.processing_type,a.description,
                a.generate_type,a.platform,a.created_by,date_format(a.created_at,'{0}') created_at,
                a.updated_by,date_format(a.updated_at,'{0}') updated_at,b.data_set_id
                from data_processing a,data_processing_relation b where a.processing_id=b.processing_id
                and b.data_set_type='result_table' and b.data_directing='output' and b.data_set_id in({1}))tmp
                left join project_info c on tmp.project_id=c.project_id""".format(
                    DEFAULT_DATE_FORMAT, in_cond
                )
            dp_result = BasicApi.get_complex_search(dp_sql)
            if dp_result:
                for dp_obj in dp_result:
                    data_set_id = dp_obj["data_set_id"]
                    del dp_obj["data_set_id"]
                    dp_rt_id_dict[data_set_id] = dp_obj

        if "storages" in related:
            storage_rt_id_dict = get_result_table_storages_v3(in_cond, need_storage_detail=need_storage_detail)

        for rt_obj in query_result:
            rt_id = rt_obj["result_table_id"]
            if "fields" in related:
                rt_obj["fields"] = field_rt_id_dict.get(rt_id, [])
            if "data_processing" in related:
                rt_obj["data_processing"] = dp_rt_id_dict.get(rt_id, {})
            if "storages" in related:
                rt_obj["storages"] = storage_rt_id_dict.get(rt_id, {})
    return query_result


def get_result_table_geog_area(result_table_id=None):
    sql = None
    if result_table_id:
        sql = """select id, tag_code, tag_type from tag_target where target_id = {}
        and tag_code in ('inland', 'SEA', 'NA') and target_type = 'result_table'""".format(
            result_table_id
        )
    return BasicApi.get_complex_search(sql)


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


def get_data_transfers_v3(where_cond, page=None, page_size=None):  # backend=mysql
    sql = """
    select a.project_id,b.project_name,a.transferring_id, a.transferring_alias, a.transferring_type, a.generate_type,
    a.created_by, date_format(a.created_at,'{0}') created_at, a.updated_by, date_format(a.updated_at,'{0}') updated_at,
    a.description from data_transferring a left join project_info b on a.project_id=b.project_id where 1=1 """.format(
        DEFAULT_DATE_FORMAT
    )
    if where_cond:
        sql += where_cond
    query_result = BasicApi.get_complex_search(paged_sql(sql, page=page, page_size=page_size))
    if query_result:
        dp_relation_sql = """
        select transferring_id,data_directing,data_set_id,data_set_type,storage_type,channel_cluster_config_id,
        storage_cluster_config_id from data_transferring_relation where transferring_id in(
        """
        for dp_obj in query_result:
            dp_obj["inputs"] = []
            dp_obj["outputs"] = []
        dp_relation_sql += "".join("'" + escape_string(dp_obj["transferring_id"]) + "'," for dp_obj in query_result)
        dp_relation_sql = dp_relation_sql[: len(dp_relation_sql) - 1] + ") order by transferring_id"
        dp_relation_result = BasicApi.get_complex_search(dp_relation_sql)
        dp_relation_dict = {}
        for dp_relation_obj in dp_relation_result:
            dp_id = dp_relation_obj["transferring_id"]
            data_directing = dp_relation_obj["data_directing"]
            del dp_relation_obj["transferring_id"]
            del dp_relation_obj["data_directing"]
            if dp_id in dp_relation_dict:
                if data_directing == "input":
                    dp_relation_dict[dp_id]["inputs"].append(dp_relation_obj)
                elif data_directing == "output":
                    dp_relation_dict[dp_id]["outputs"].append(dp_relation_obj)
            else:
                tmp_dict = {}
                inputs_list, outputs_list = [], []
                if data_directing == "input":
                    inputs_list.append(dp_relation_obj)
                elif data_directing == "output":
                    outputs_list.append(dp_relation_obj)
                tmp_dict["inputs"] = inputs_list
                tmp_dict["outputs"] = outputs_list
                dp_relation_dict[dp_id] = tmp_dict

        for dp_obj in query_result:
            dp_id = dp_obj["transferring_id"]
            dp_relation_obj = dp_relation_dict.get(dp_id)
            if dp_relation_obj:
                dp_obj["inputs"] = dp_relation_obj["inputs"]
                dp_obj["outputs"] = dp_relation_obj["outputs"]

    return query_result


def get_data_processings_v3(where_cond, page=None, page_size=None):  # backend=mysql
    sql = """
    select a.processing_id,a.project_id,b.project_name,a.processing_alias,a.processing_type,a.generate_type,
    a.created_by,date_format(a.created_at,'{0}') created_at,a.updated_by,date_format(a.updated_at,'{0}') updated_at,
    a.description,a.platform from data_processing a left join project_info b on a.project_id=b.project_id
    where 1=1 """.format(
        DEFAULT_DATE_FORMAT
    )
    if where_cond:
        sql += where_cond
    query_result = BasicApi.get_complex_search(paged_sql(sql, page=page, page_size=page_size))
    if query_result:
        dp_relation_sql = """
        select processing_id,data_directing,data_set_id,data_set_type,storage_type,channel_cluster_config_id,
        storage_cluster_config_id from data_processing_relation where processing_id in(
        """
        for dp_obj in query_result:
            dp_obj["inputs"] = []
            dp_obj["outputs"] = []
        dp_relation_sql += "".join("'" + escape_string(dp_obj["processing_id"]) + "'," for dp_obj in query_result)
        dp_relation_sql = dp_relation_sql[: len(dp_relation_sql) - 1] + ") order by processing_id"
        dp_relation_result = BasicApi.get_complex_search(dp_relation_sql)
        dp_relation_dict = {}
        for dp_relation_obj in dp_relation_result:
            dp_id = dp_relation_obj["processing_id"]
            data_directing = dp_relation_obj["data_directing"]
            del dp_relation_obj["processing_id"]
            del dp_relation_obj["data_directing"]
            if dp_id in dp_relation_dict:
                if data_directing == "input":
                    dp_relation_dict[dp_id]["inputs"].append(dp_relation_obj)
                elif data_directing == "output":
                    dp_relation_dict[dp_id]["outputs"].append(dp_relation_obj)
            else:
                tmp_dict = {}
                inputs_list, outputs_list = [], []
                if data_directing == "input":
                    inputs_list.append(dp_relation_obj)
                elif data_directing == "output":
                    outputs_list.append(dp_relation_obj)
                tmp_dict["inputs"] = inputs_list
                tmp_dict["outputs"] = outputs_list
                dp_relation_dict[dp_id] = tmp_dict

        for dp_obj in query_result:
            dp_id = dp_obj["processing_id"]
            dp_relation_obj = dp_relation_dict.get(dp_id)
            if dp_relation_obj:
                dp_obj["inputs"] = dp_relation_obj["inputs"]
                dp_obj["outputs"] = dp_relation_obj["outputs"]

    return query_result


def get_project_info_sql():
    sql = """select project_name,date_format(deleted_at,'{0}') deleted_at,description,
    date_format(created_at,'{0}') created_at,date_format(updated_at,'{0}') updated_at,created_by,deleted_by,bk_app_code,
    active,project_id,updated_by from project_info where 1=1""".format(
        DEFAULT_DATE_FORMAT
    )
    return sql


def parse_data_category_result(ret_result):
    parse_result = []
    tmp_result = []
    not_visible_dict = {}  # visible=0的id与parent_id
    if ret_result:
        ret_result.sort(key=lambda l: (l["parent_id"], l["seq_index"]), reverse=False)
        for ret_dict in ret_result:
            id_val = ret_dict["id"]
            active = ret_dict["active"]
            visible = ret_dict["visible"]
            parent_id = ret_dict["parent_id"]
            if not active:  # false
                continue
            if not visible:  # false
                not_visible_dict[id_val] = parent_id
                continue
            tmp_result.append(ret_dict)

    if tmp_result:
        for ret_dict in tmp_result:
            parent_id = ret_dict["parent_id"]
            if parent_id in not_visible_dict:
                p_parent_id = not_visible_dict[parent_id]
                while p_parent_id in not_visible_dict:
                    p_parent_id = not_visible_dict[p_parent_id]
                ret_dict["parent_id"] = p_parent_id

    if tmp_result:
        parent_id_dict = {}  # parent_id:value_list

        for ret_dict in tmp_result:
            parent_id = ret_dict["parent_id"]
            if parent_id in parent_id_dict:
                parent_id_dict[parent_id].append(ret_dict)
            else:
                parent_id_dict[parent_id] = [ret_dict]

        # for value in parent_id_dict.values():
        #     if value:
        #         value.sort(key=field_index_seq_index_sort, reverse=False)

        for ret_dict in tmp_result:
            id_val = ret_dict["id"]
            parent_id = ret_dict["parent_id"]
            ret_dict["sub_list"] = parent_id_dict[id_val] if id_val in parent_id_dict else []
            if parent_id == 0:
                parse_result.append(ret_dict)

    # if parse_result:
    #     parse_result.sort(key=field_index_seq_index_sort, reverse=False)
    return parse_result


def get_filter_attrs(related_filter, need_type_name):
    filter_attr_name = None
    filter_attr_value = None
    if related_filter:
        filter_obj = json.loads(related_filter)
        filter_type = filter_obj.get("type")
        if filter_type == need_type_name:
            filter_attr_name = filter_obj.get("attr_name", "")
            filter_attr_value = filter_obj.get("attr_value", "")
    return filter_attr_name, filter_attr_value


# Atlas公共字段
ATLAS_FIELD_ID = "id"
ATLAS_FIELD_NAME = "name"
ATLAS_FIELD_QUALIFIED_NAME = "qualifiedName"
ATLAS_FIELD_OWNER = "owner"
ATLAS_FIELD_DESCRIPTION = "description"
ATLAS_FIELD_INPUTS = "inputs"
ATLAS_FIELD_OUTPUTS = "outputs"

# Atlas状态
ATLAS_FIELD_ACTIVE = "ACTIVE"


def get_attrs_v2(obj, batch_guid_dict):
    if isinstance(obj, dict) and obj:
        guid = obj["guid"]
        obj_attrs = batch_guid_dict.get(guid)
        if obj_attrs:
            return obj_attrs
    return {}


def parse_storage_to_dict_v2(storage_list, batch_guid_dict, result_table_id):
    res_dict = {}
    for attrs in storage_list:
        # cluster_type = attrs['cluster_type'] # cluster_type 字段已去掉
        # storage_cluster 和 storage_channel 只会一个有值
        active = attrs.get("active")
        if active is not None and not active:  # 软删除的
            continue

        storage_cluster = attrs["storage_cluster"]
        storage_channel = attrs["storage_channel"]
        cluster_type = None

        if storage_channel:
            cluster_type = "kafka"

        storage_cluster_attrs = get_attrs_v2(storage_cluster, batch_guid_dict)
        if storage_cluster_attrs:
            storage_cluster_attrs["storage_cluster_config_id"] = storage_cluster_attrs.get("id")
        del_redundance_key(storage_cluster_attrs, "storage_configs", "storage_config", "dp_characters", "dt_characters")
        del_common_unuse_fields(storage_cluster_attrs)
        del_common_unuse_fields2(storage_cluster_attrs)
        del_common_unuse_fields3(storage_cluster_attrs)
        attrs["storage_cluster"] = storage_cluster_attrs
        if storage_cluster_attrs:
            cluster_type = storage_cluster_attrs["cluster_type"]

        storage_channel_obj = get_attrs_v2(storage_channel, batch_guid_dict)
        if storage_channel_obj:
            storage_channel_obj["channel_cluster_config_id"] = storage_channel_obj.get("id")
        del_redundance_key(
            storage_channel_obj,
            "storage_config",
            "storage_configs",
            ATLAS_FIELD_NAME,
            ATLAS_FIELD_OWNER,
            "dp_characters",
            "dt_characters",
        )
        del_common_unuse_fields(storage_channel_obj)
        del_common_unuse_fields2(storage_channel_obj)
        attrs["storage_channel"] = storage_channel_obj
        del_redundance_key(attrs, "result_tables", "result_table", ATLAS_FIELD_NAME, ATLAS_FIELD_OWNER)

        # attrs['result_table_id'] = result_table_id
        res_dict[cluster_type] = attrs
    return res_dict


def parse_lineage_attrs(entity_obj):
    type_name = entity_obj["typeName"]
    entity_attrs = entity_obj["attributes"]
    prefix_name = None
    field_name = None
    ret_type_name = None
    if type_name == "ResultTable":
        prefix_name = "result_table_"
        field_name = "result_table_id"
        ret_type_name = "result_table"
    elif type_name == "DataProcess":
        prefix_name = "data_processing_"
        field_name = "processing_id"
        ret_type_name = "data_processing"
    elif type_name == "RawData":
        prefix_name = "raw_data_"
        field_name = "id"
        ret_type_name = "raw_data"
    qualified_name = str(entity_attrs[field_name])
    name = prefix_name + qualified_name
    return name, ret_type_name, qualified_name


def parse_mysql_lineage_attrs(entity_obj):
    type_name = entity_obj["type"]
    qualified_name = entity_obj["qualified_name"]
    return type_name + "_" + qualified_name, type_name, qualified_name


def parse_mysql_lineage_node_attrs(entity_obj):
    type_name = entity_obj["type"]
    qualified_name = entity_obj["qualified_name"]
    extra = entity_obj.get("extra", {})
    return type_name + "_" + qualified_name, type_name, qualified_name, extra


def get_result_table_lineage_info(params):
    res_obj = {}
    res_data = BasicApi.get_lineage(params)
    backend_type = res_data.get("backend_type", None)
    if backend_type and backend_type in ("mysql", "dgraph"):
        lineage = res_data["lineage"]
        if lineage:
            mysql_criteria = lineage["criteria"]
            mysql_nodes = lineage["nodes"]
            mysql_relations = lineage["relations"]
            res_obj["depth"] = mysql_criteria["depth"]
            res_obj["direction"] = mysql_criteria["direction"]
            res_relations = []
            nodes = {}
            if mysql_relations:
                for relation in mysql_relations:
                    fm = relation["from"]
                    to = relation["to"]
                    from_name, _, _ = parse_mysql_lineage_attrs(fm)
                    to_name, _, _ = parse_mysql_lineage_attrs(to)
                    res_relations.append({"from": from_name, "to": to_name, "status": "ACTIVE"})
            if mysql_nodes:
                for node in mysql_nodes:
                    key_name, type_name, qualified_name, extra = parse_mysql_lineage_node_attrs(node)
                    if "extra_retrieve" in params:
                        tmp_node = {"type": type_name, "qualified_name": qualified_name, "extra": extra}
                    else:
                        tmp_node = {"type": type_name, "qualified_name": qualified_name}
                    if type_name == "result_table":
                        tmp_node["result_table_id"] = qualified_name
                    elif type_name == "data_processing":
                        tmp_node["processing_id"] = qualified_name
                    elif type_name == "raw_data":
                        tmp_node["id"] = int(qualified_name)
                    nodes[key_name] = tmp_node
            res_obj["nodes"] = nodes
            res_obj["relations"] = res_relations
    return res_obj


def parse_genealogy_info(rpc_ret):
    res_obj = {}
    genealogy = rpc_ret["genealogy"]
    if genealogy:
        mysql_criteria = genealogy["criteria"]
        mysql_nodes = genealogy["nodes"]
        mysql_relations = genealogy["relations"]
        res_obj["depth"] = mysql_criteria["depth"]
        res_relations = []
        nodes = {}
        if mysql_relations:
            for relation in mysql_relations:
                fm = relation["from"]
                to = relation["to"]
                from_name, _, _ = parse_mysql_lineage_attrs(fm)
                to_name, _, _ = parse_mysql_lineage_attrs(to)
                res_relations.append({"from": from_name, "to": to_name, "status": "ACTIVE"})
        if mysql_nodes:
            for node in mysql_nodes:
                key_name, type_name, qualified_name = parse_mysql_lineage_attrs(node)
                tmp_node = {"type": type_name, "qualified_name": qualified_name}
                if type_name == "result_table":
                    tmp_node["result_table_id"] = qualified_name
                elif type_name == "data_processing":
                    tmp_node["processing_id"] = qualified_name
                elif type_name == "raw_data":
                    tmp_node["id"] = int(qualified_name)
                nodes[key_name] = tmp_node
        res_obj["nodes"] = nodes
        res_obj["relations"] = res_relations
    return res_obj


def get_lineage_node(obj, type_name, qualified_name):
    obj_attrs = get_obj_attrs(obj, add_common=False)
    parse_lineage_nodes_obj(obj_attrs)
    obj_attrs["type"] = type_name
    obj_attrs["qualified_name"] = qualified_name
    return obj_attrs


def parse_lineage_nodes_obj(obj):
    del_redundance_key(obj, "createTime", ATLAS_FIELD_OWNER, ATLAS_FIELD_NAME, ATLAS_FIELD_DESCRIPTION)


def field_index_sort(elem):
    return elem["field_index"]


def get_result_guids(res_dict, field_name):
    result = res_dict["result"]
    ret_attrs = []
    if result:
        res_data = res_dict["data"]
        if res_data:
            if field_name == "entities":
                if "entities" in res_data:
                    entities = res_data[field_name]
                    for entity in entities:
                        status = entity["status"]
                        if status == ATLAS_FIELD_ACTIVE:
                            ret_attrs.append(entity["guid"])
            elif field_name == "entity":
                entity = res_data["entity"]
                status = entity["status"]
                if status == ATLAS_FIELD_ACTIVE:
                    ret_attrs.append(entity["guid"])
    return ret_attrs


def get_result_attrs(res_dict, available_attr=None, complex_attrs=None, need_guid=False):
    result = res_dict["result"]
    ret_attrs = []
    referred_entities = None
    if result:
        res_data = res_dict["data"]
        if res_data:
            if "entities" in res_data:
                entities = res_data["entities"]
                referred_entities = res_data.get("referredEntities")
                for entity in entities:
                    status = entity["status"]
                    if status == ATLAS_FIELD_ACTIVE:
                        attrs = entity["attributes"]
                        handler_common_fields(attrs)
                        ret_entity = {}
                        if need_guid:
                            ret_entity["guid"] = entity["guid"]
                        if complex_attrs:
                            for k, v in list(complex_attrs.items()):
                                field_arr = v.split("|")
                                for def_field_val in field_arr:
                                    show_field, get_field = def_field_val.split("=", 1)
                                    ret_entity[show_field] = None
                                    key_dict = attrs.get(k)
                                    if key_dict:
                                        k_guid = key_dict["guid"]
                                        if referred_entities:
                                            guid_dict = referred_entities.get(k_guid)
                                            if guid_dict:
                                                guid_attrs = guid_dict.get("attributes")
                                                if guid_attrs:
                                                    ret_entity[show_field] = guid_attrs.get(get_field)
                        if available_attr:
                            for field_name in available_attr:
                                if complex_attrs:
                                    if field_name in complex_attrs:
                                        continue
                                ret_entity[field_name] = attrs.get(field_name)
                            ret_attrs.append(ret_entity)
                        else:
                            if need_guid:
                                attrs["guid"] = entity["guid"]
                            ret_attrs.append(attrs)
    return ret_attrs, referred_entities


def get_result_obj_attrs(res_dict, need_guid=False):
    result = res_dict["result"]
    if result:
        res_data = res_dict["data"]
        if res_data:
            referred_entities = res_data["referredEntities"]
            entity = res_data["entity"]
            guid = entity["guid"]
            if guid not in list(referred_entities.keys()):
                # 加入referredEntities中,主要为了处理如:根据Project查询得到了ResultTable,然后在ResultTable中又需要取Project的信息的情况
                referred_entities[guid] = entity
            attrs = get_obj_attrs(entity, need_guid=need_guid)
            return attrs, referred_entities
    return {}, {}


def get_obj_attrs(entity_obj, add_common=True, need_guid=False):
    status = entity_obj["status"]
    if status == ATLAS_FIELD_ACTIVE:
        attrs = entity_obj["attributes"]
        if add_common:
            handler_common_fields(attrs)
        else:
            del_redundance_key(attrs, "created_by", "updated_by", "created_at", "updated_at")
        if need_guid:
            attrs["guid"] = entity_obj["guid"]
        del_common_unuse_fields(attrs)
        return attrs
    return {}


def handler_common_fields(attrs):
    if "deleted_at" in attrs:
        deleted_at = attrs["deleted_at"]
        if deleted_at is not None and deleted_at != 0:
            attrs["deleted_at"] = parse_time_format(deleted_at)
    if "created_by" not in attrs:
        attrs["created_by"] = "admin"
    if "updated_by" not in attrs:
        attrs["updated_by"] = "admin"
    if "created_at" not in attrs:
        attrs["created_at"] = None
    else:
        attrs["created_at"] = parse_time_format(attrs["created_at"])
    if "updated_at" not in attrs:
        attrs["updated_at"] = None
    else:
        attrs["updated_at"] = parse_time_format(attrs["updated_at"])


def del_common_unuse_fields(attrs_dict):
    del_redundance_key(attrs_dict, "qualifiedName", "qualified_name_ref", "name_ref", "qualified_name")
    return attrs_dict


def del_common_unuse_fields2(attrs_dict):
    del_redundance_key(attrs_dict, "created_by", "updated_by", "created_at", "updated_at")
    return attrs_dict


def del_common_unuse_fields3(attrs_dict):
    del_redundance_key(attrs_dict, ATLAS_FIELD_ID, ATLAS_FIELD_NAME, ATLAS_FIELD_OWNER, ATLAS_FIELD_DESCRIPTION)
    return attrs_dict


def del_redundance_key(obj, *keys):
    if isinstance(obj, dict):
        for key in keys:
            if key in list(obj.keys()):
                del obj[key]
    elif isinstance(obj, (list, tuple)):
        for elem in obj:
            if isinstance(elem, dict):
                for key in keys:
                    if key in list(elem.keys()):
                        del elem[key]


def parse_time_format(time_stamp):
    if isinstance(time_stamp, str):
        return time_stamp
    if time_stamp:
        # time_stamp /= 1000.0
        time_arr = time.localtime(time_stamp)
        return time.strftime("%Y-%m-%d %H:%M:%S", time_arr)
    return None
