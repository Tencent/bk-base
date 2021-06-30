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
from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
from math import log

from api import meta_api
from conf.settings import LIFE_CYCLE_TOKEN_PKEY
from lifecycle.metrics.bfs import get_node_count

logger = logging.getLogger(__name__)


def get_range_info(dataset_id, dataset_type):
    """
    get the range related info
    :param dataset_id:
    :param dataset_type:
    :return:
    """
    statement = (
        """
    {
        var(func: eq(ResultTable.result_table_id,\"%s\")) @recurse(depth: 100, loop: false) {
            all_uid as uid
            lineage.descend
        }
        var(func:uid(all_uid))@filter(has(ResultTable.typed)) {
            p as ResultTable.project
        }
        project_count(func:uid(p)) {
            count(uid)
        }
        bk_biz_id(func:uid(all_uid)) @filter(has(ResultTable.typed)) @groupby(ResultTable.bk_biz_id) {
            count(uid)
        }
    }
    """
        % dataset_id
    )
    if dataset_type == "raw_data":
        statement = statement.replace(
            "eq(ResultTable.result_table_id", "eq(AccessRawData.id"
        )
        statement = statement.replace(
            "all_uid as uid", "all_uid as uid id:AccessRawData.id"
        )
    try:
        range_dict = meta_api.complex_search(
            {
                "statement": statement,
                "backend_type": "dgraph",
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error("complex search error: {error}".format(error=e.message))
        return {}
    return range_dict


def get_node_info(dataset_id, dataset_type):
    """
    get the successor node info
    :param dataset_id:
    :param dataset_type:
    :return:
    """
    weighted_node_count = 0.0
    node_count_list = []
    node_count = 0
    node_count_dict = get_node_count(dataset_id, dataset_type)
    for key, value in sorted(node_count_dict.items()):
        if key == 0:
            continue
        if key < 30:
            node_count_list.append(len(set(value)))
            weighted_node_count += (1 - log(key) / log(30)) * len(set(value))
            node_count += len(set(value))
    depth = len(node_count_dict.keys()) - 1 if len(node_count_dict.keys()) > 1 else 0
    return weighted_node_count, json.dumps(node_count_list), node_count, depth


def get_range_score(weighted_node, biz_count, proj_count):
    """
    get range_socre and normalized_range_score(0~99.99)
    :param weighted_node:
    :param biz_count:
    :param proj_count:
    :return:
    """
    range_score = round((weighted_node + biz_count + proj_count), 2)
    a = 1 if weighted_node / 32.0 > 1 else weighted_node / 32.0
    b = 1 if biz_count / 4.0 > 1 else biz_count / 4.0
    c = 1 if proj_count / 7.0 > 1 else proj_count / 7.0
    normalized_range_score = (
        round((a + b + c) / 3.0 * 100, 2)
        if round((a + b + c) / 3.0 * 100, 2) < 100
        else 99.9
    )
    return range_score, normalized_range_score
