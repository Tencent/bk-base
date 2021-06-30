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


import math

from common.log import logger

from datamanage.utils.api.meta import MetaApi
from datamanage.pro.lifecycle.utils import transpose
from datamanage.exceptions import ParamQueryMetricsError


def get_range_content(res):
    biz_count = (
        len(res.get('data', {}).get('bk_biz_id', [])[0].get('@groupby'))
        if len(res.get('data', {}).get('bk_biz_id', [])) > 0
        else 1
    )
    proj_count = (
        res.get('data', {}).get('project_count', [])[0].get('count')
        if res.get('data', {}).get('project_count', []) > 0
        else 1
    )
    if proj_count == 0:
        proj_count = 1
    return biz_count, proj_count


def get_range_metric(dataset_id, dataset_type):
    """
    拿到单个数据集的广度分数和相关指标
    直接用dgraph统计血缘中除去数据处理节点后的每一层的节点数
    目前存在问题，深度优先搜索得到的搜索路径里面的节点是乱序
    :param dataset_id:
    :param dataset_type:
    :return:
    """
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
        logger.error('Complex search error: {error}').format(error=e)
    # 参数错误导致血缘查询结果为空
    if not res.get('data', {}).get('every_level', []):
        raise ParamQueryMetricsError

    biz_count, proj_count = get_range_content(res)

    # 每一层子节点的个数
    level_count = []
    for each_level in res.get('data', {}).get('every_level', []):
        if 'rt_id' in each_level:
            level_count.append(each_level.get('rt_id')) if isinstance(
                each_level.get('rt_id'), list
            ) else level_count.append([each_level.get('rt_id')])

    # 对矩阵进行转置
    level_count = transpose(level_count)

    # 对血缘每一层节点数求加权和
    level = 0 if dataset_type == 'result_table' else 1
    weighted_node = 0
    node_count = []
    for each_level in level_count:
        if level == 0:
            level += 1
            continue
        if level >= 30:
            break
        node_count.append(len(set(each_level)))
        weighted_node += (1 - math.log(level) / math.log(30)) * len(set(each_level))
        level += 1

    # 没进行归一化的广度分数
    weighted_node += biz_count + proj_count
    return {
        'biz_count': biz_count,
        'proj_count': proj_count,
        'node_count': node_count,
        'range_score': weighted_node,
        'depth': level - 1 if level > 1 else 0,
    }
