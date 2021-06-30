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


from math import log
import logging

from common.log import ErrorCodeLogger

from datamanage.utils.api.meta import MetaApi
from datamanage.pro.lifecycle.metrics.bfs import get_node_count
from datamanage.pro.lifecycle.metrics.range import get_range_content

logger = ErrorCodeLogger(logging.getLogger('range_task'))

range_metric_dict = {
    'biz_count': 0,
    'project_count': 0,
    'depth': 0,
    'node_count_list': [],
    'node_count': 0,
    'weighted_node_count': 0.0,
    'app_code_count': 0,
    'range_score': 0.0,
    'normalized_range_score': 0.0,
}


def get_range_bfs_metric(dataset_id, dataset_type):
    # 广度优先搜索
    statement = (
        """{every_level(func: eq(ResultTable.result_table_id,\"%s\"))
    @normalize @recurse(depth: 100, loop: false) {all_uid as uid rt_id: ResultTable.result_table_id lineage.descend}
    var(func:uid(all_uid))@filter(has(ResultTable.typed)) {p as ResultTable.project} project_count(func:uid(p))
    {count(uid)} bk_biz_id(func:uid(all_uid)) @filter(has(ResultTable.typed)) @groupby(ResultTable.bk_biz_id)
    {count(uid)}}"""
        % dataset_id
        if dataset_type == 'result_table'
        else """{every_level(func: eq(AccessRawData.id,
    \"%s\")) @normalize @recurse(depth: 100, loop: false) {all_uid as uid id:AccessRawData.id
    rt_id:ResultTable.result_table_id lineage.descend} var(func:uid(all_uid))@filter(has(ResultTable.typed))
    {p as ResultTable.project} project_count(func:uid(p)) {count(uid)} bk_biz_id(func:uid(all_uid))
    @filter(has(ResultTable.typed)) @groupby(ResultTable.bk_biz_id) {count(uid)}}"""
        % dataset_id
    )
    try:
        res = MetaApi.complex_search(
            {
                'statement': statement,
                'backend_type': 'dgraph',
            },
            raise_exception=False,
        ).data
    except Exception as e:
        logger.error('Complex search error: {error}'.format(error=e))
        return range_metric_dict
    if not res.get('data', {}).get('every_level', []):
        logger.error('every_level[] dataset_id: %s' % dataset_id)
        return range_metric_dict
        # raise ParamQueryMetricsError
    biz_count, proj_count = get_range_content(res)

    weighted_node = 0.0

    node_count_list = []
    node_count = 0
    level_count_dict = get_node_count(dataset_id, dataset_type)
    for key, value in list(level_count_dict.items()):
        if key == 0:
            continue
        if key < 30:
            node_count_list.append(len(set(value)))
            weighted_node += (1 - log(key) / log(30)) * len(set(value))
            node_count += len(set(value))
    range_score = round((weighted_node + biz_count + proj_count), 2)
    a = 1 if weighted_node / 32.0 > 1 else weighted_node / 32.0
    b = 1 if biz_count / 4.0 > 1 else biz_count / 4.0
    c = 1 if proj_count / 7.0 > 1 else proj_count / 7.0
    normlized_range_score = round((a + b + c) / 3.0 * 100, 2) if round((a + b + c) / 3.0 * 100, 2) < 100 else 99.99
    return {
        'biz_count': biz_count,
        'project_count': proj_count,
        'depth': len(list(level_count_dict.keys())) - 1 if len(list(level_count_dict.keys())) > 1 else 0,
        'node_count_list': node_count_list,
        'node_count': node_count,
        'weighted_node_count': weighted_node,
        'app_code_count': 0,
        'range_score': range_score,
        'normalized_range_score': normlized_range_score,
    }


def get_range_bfs_metric_for_kafka(dataset_id, dataset_type):
    # 广度优先搜索,用于同步数据到kafka
    statement = (
        """{
        every_level(func: eq(ResultTable.result_table_id,\"%s\")) @normalize @recurse(depth: 100, loop: false) {
            all_uid as uid
            rt_id: ResultTable.result_table_id
            lineage.descend
        }
        var(func:uid(all_uid))@filter(has(ResultTable.typed)) {p as ResultTable.project} project_count(func:uid(p)) {
            count(uid)
        }
        bk_biz_id(func:uid(all_uid)) @filter(has(ResultTable.typed)) @groupby(ResultTable.bk_biz_id) {
            count(uid)
        }
    }"""
        % dataset_id
        if dataset_type == 'result_table'
        else """{
        every_level(func: eq(AccessRawData.id,\"%s\")) @normalize @recurse(depth: 100, loop: false) {
            all_uid as uid id:AccessRawData.id
            rt_id:ResultTable.result_table_id
            lineage.descend
        }
        var(func:uid(all_uid))@filter(has(ResultTable.typed)) {p as ResultTable.project} project_count(func:uid(p)) {
            count(uid)
        }
        bk_biz_id(func:uid(all_uid)) @filter(has(ResultTable.typed)) @groupby(ResultTable.bk_biz_id) {
            count(uid)
        }
    }"""
        % dataset_id
    )

    default_metric_dict = {
        'biz_count': 0,
        'proj_count': 0,
        'node_count': [],
        'range_score': 0.0,
        'weighted_node': 0.0,
        'normlized_range_score': 0.0,
        'depth': 0,
    }
    try:
        res = MetaApi.complex_search(
            {
                'statement': statement,
                'backend_type': 'dgraph',
            },
            raise_exception=False,
        ).data
    except Exception as e:
        logger.error('Complex search error: {error}'.format(error=e))
        return default_metric_dict
    if not res.get('data', {}).get('every_level', []):
        logger.error('every_level[] dataset_id: %s' % dataset_id)
        return default_metric_dict
        # raise ParamQueryMetricsError
    biz_count, proj_count = get_range_content(res)

    weighted_node = 0.0

    # 每一层叶子节点的个数
    node_count = []
    # 统计每一层的节点数（过滤掉dp）
    level_count_dict = get_node_count(dataset_id, dataset_type)
    for key, value in list(level_count_dict.items()):
        if key == 0:
            continue
        if key < 30:
            node_count.append(len(set(value)))
            weighted_node += (1 - log(key) / log(30)) * len(set(value))
    # 未归一化广度评分，暂时不展示页面，非百分制
    range_score = weighted_node + biz_count + proj_count
    a = 1 if weighted_node / 32.0 > 1 else weighted_node / 32.0
    b = 1 if biz_count / 4.0 > 1 else biz_count / 4.0
    c = 1 if proj_count / 7.0 > 1 else proj_count / 7.0
    # 归一化广度评分，展示页面，百分制
    normlized_range_score = round((a + b + c) / 3.0 * 100, 2) if round((a + b + c) / 3.0 * 100, 2) < 100 else 99.99
    return {
        'biz_count': biz_count,
        'proj_count': proj_count,
        'node_count': node_count,
        'range_score': range_score,
        'weighted_node': weighted_node,
        'normlized_range_score': normlized_range_score,
        'depth': len(list(level_count_dict.keys())) - 1 if len(list(level_count_dict.keys())) > 1 else 0,
    }
