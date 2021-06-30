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


from pandas import DataFrame
import copy

from django.conf import settings
from django.utils.translation import ugettext as _
from django.core.cache import cache

from datamanage.pro.utils.time import get_date
from datamanage.utils.api.dataquery import DataqueryApi
from datamanage.pro.datastocktake.dataset_process import dataset_filter
from datamanage.pro.datamap import dmaction

minimal_value = 1e-23
prefer_storage = 'tspider'
RUN_MODE = getattr(settings, 'RUN_MODE', 'DEVELOP')


def fetch_value_between_source_target(
    second_level_tag_list, dgraph_result, processing_type_dict, label, alias_list, source, target, value
):
    for each_processing_type in dmaction.processing_type_list:
        # 第二层和第三层之间的value
        for each_tag in second_level_tag_list:
            if dgraph_result.get('c_%s_%s' % (each_tag.get('tag_code'), each_processing_type)):
                if RUN_MODE != 'PRODUCT':
                    count_tmp = dgraph_result.get('c_%s_%s' % (each_tag.get('tag_code'), each_processing_type))[0].get(
                        'count', 0
                    )
                else:
                    count_tmp = (
                        dgraph_result.get('c_%s_%s' % (each_tag.get('tag_code'), each_processing_type))[0]
                        .get('~ResultTable.processing_type_obj')[0]
                        .get('count')
                        if dgraph_result.get('c_%s_%s' % (each_tag.get('tag_code'), each_processing_type))[0].get(
                            '~ResultTable.processing_type_obj'
                        )
                        else 0
                    )
                if count_tmp:
                    # 第三层label
                    if each_processing_type not in label:
                        label.append(each_processing_type)
                        alias_list.append(processing_type_dict.get(each_processing_type))
                    source.append(label.index(each_tag.get('tag_code')))
                    target.append(label.index(each_processing_type))
                    value.append(count_tmp)
        # 第一层和第三层之前的value(其他到processing_type)
        if dgraph_result.get('c_other_%s' % each_processing_type):
            if RUN_MODE != 'PRODUCT':
                count_tmp = dgraph_result.get('c_other_%s' % each_processing_type)[0].get('count')
            else:
                count_tmp = (
                    dgraph_result.get('c_other_%s' % each_processing_type)[0]
                    .get('~ResultTable.processing_type_obj')[0]
                    .get('count')
                    if dgraph_result.get('c_other_%s' % each_processing_type)[0].get('~ResultTable.processing_type_obj')
                    else 0
                )
            if count_tmp:
                # 第三层label
                if each_processing_type not in label:
                    label.append(each_processing_type)
                    alias_list.append(processing_type_dict.get(each_processing_type))
                source.append(label.index('other'))
                target.append(label.index(each_processing_type))
                value.append(count_tmp)


def format_sankey_diagram(params, request, source, target, value, level, label, alias_list, processing_type_dict):
    # 第三层和第四层之前的关联通过表591_dataquery_opdata_biz_rt
    query_and_proc_list = get_query_and_proc_list(params, request)
    real_level = level
    if not query_and_proc_list:
        real_level = 3

    # 排序
    app_top_len = 8 if len(query_and_proc_list) >= 8 else len(query_and_proc_list)
    i = 1
    other_app = _('其他应用')
    app_code_dict = cache.get('app_code_dict', {})
    other_app_code_list = []
    for each_query_and_proc in query_and_proc_list:
        if i <= app_top_len:
            each_query_and_proc['app_code_alias'] = app_code_dict.get(each_query_and_proc['app_code'], '')
            i += 1
            continue
        else:
            other_app_code_list.append(copy.deepcopy(each_query_and_proc))
            each_query_and_proc['app_code'] = other_app
            each_query_and_proc['app_code_alias'] = other_app
        i += 1

    for each_query_and_proc in query_and_proc_list:
        # 第四层label
        if each_query_and_proc.get('processing_type') not in label:
            label.append(each_query_and_proc.get('processing_type'))
            alias_list.append(processing_type_dict.get(each_query_and_proc.get('processing_type')))
        if each_query_and_proc.get('app_code_alias') not in label:
            label.append(each_query_and_proc['app_code_alias'])
            alias_list.append(each_query_and_proc['app_code_alias'])
        source.append(label.index(each_query_and_proc.get('processing_type')))
        target.append(label.index(each_query_and_proc['app_code_alias']))
        if RUN_MODE != 'PRODUCT':
            value.append(int(each_query_and_proc['count']))
        else:
            value.append(each_query_and_proc['count'])
    if other_app not in label:
        label.append(other_app)
        alias_list.append(other_app)
    # 第三层节点和第四层节点之间没有link，导致第三层节点到第四层
    for each_processing_type in dmaction.processing_type_list:
        if each_processing_type in label and label.index(each_processing_type) not in source and other_app in label:
            source.append(label.index(each_processing_type))
            target.append(label.index(other_app))
            value.append(minimal_value)
    return other_app_code_list, real_level


def get_query_and_proc_list(params, request):
    yesterday = get_date()
    bk_biz_id = params.get('bk_biz_id')
    project_id = params.get('project_id')
    bk_biz_id_cond = 'bk_biz_id={} and'.format(bk_biz_id) if bk_biz_id else ''
    project_id_cond = 'project_id={} and'.format(project_id) if project_id else ''

    query_and_proc_list = []

    if not params['has_advanced_params']:
        sql = """SELECT count(result_table_id) as count, app_code, processing_type from
        (SELECT count, app_code, processing_type, result_table_id
        FROM 591_dataquery_processing_type_project_id_one_day WHERE {} {} thedate={} and app_code is not null
        and processing_type in
        ('model', 'batch_model', 'stream', 'clean', 'stream_model', 'view', 'storage', 'batch', 'transform')
        and result_table_id is not null) sankey_rt
        group by app_code, processing_type order by count desc limit 10000""".format(
            bk_biz_id_cond, project_id_cond, yesterday
        )
        # 查询和processing之间的关联
        query_and_proc_dict = DataqueryApi.query({'sql': sql, 'prefer_storage': prefer_storage}).data
        if query_and_proc_dict:
            query_and_proc_list = query_and_proc_dict.get('list', [])
    else:
        sql = """SELECT count, app_code, processing_type, result_table_id
        FROM 591_dataquery_processing_type_project_id_one_day
        WHERE {} {} thedate={} and app_code is not null
        and processing_type in
        ('model', 'batch_model', 'stream', 'clean', 'stream_model', 'view', 'storage', 'batch', 'transform')
        and result_table_id is not null
        order by count desc limit 10000""".format(
            bk_biz_id_cond, project_id_cond, yesterday
        )
        query_rt_tmp_dict = DataqueryApi.query({'sql': sql, 'prefer_storage': prefer_storage}).data
        if query_rt_tmp_dict:
            query_rt_tmp_list = query_rt_tmp_dict.get('list', [])
        else:
            query_rt_tmp_list = []
        params['data_set_type'] = 'result_table'

        has_filter_cond, filter_dataset_dict = dataset_filter(params, request)
        rt_list = list(filter_dataset_dict.keys())
        query_rt_list = [
            each_query_rt for each_query_rt in query_rt_tmp_list if each_query_rt.get('result_table_id') in rt_list
        ]
        if query_rt_list:
            df1 = DataFrame(query_rt_list)
            df2 = (
                df1.groupby(['app_code', 'processing_type'], as_index=False)
                .count()
                .sort_values(['count'], axis=0, ascending=False)
            )
            query_and_proc_list = df2.to_dict(orient='records')
    return query_and_proc_list
