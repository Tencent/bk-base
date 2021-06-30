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

import logging

from api import dataquery_api, storekit_api
from common.meta import metadata_client
from conf.settings import metadata_settings
from lifecycle.utils.time import get_yesterday

logger = logging.getLogger(__name__)


def get_hdfs_capacity(dataset_id):
    yesterday = get_yesterday()
    prefer_storage = "tspider"
    sql = (
        "SELECT SUM(rt_capacity_bytes) AS capacity FROM 591_rt_capacity_hdfs_ccid "
        "WHERE thedate = {} and rt_id = '{}'".format(yesterday, dataset_id)
    )
    try:
        hdfs_cap_dict = dataquery_api.query(
            {
                "sql": sql,
                "prefer_storage": prefer_storage,
                "bk_app_code": "data",
                "bk_app_secret": metadata_settings.get("BK_APP_SECRET"),
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error(
            "get dataset_id:{} hdfs_capacity error:{}".format(dataset_id, e.message)
        )
        hdfs_capacity = 0
        return hdfs_capacity
    hdfs_capacity = (
        hdfs_cap_dict.get("list", [])[0].get("capacity")
        if hdfs_cap_dict and hdfs_cap_dict.get("list", [])
        else 0
    )
    hdfs_capacity = 0 if hdfs_capacity is None else hdfs_capacity
    return hdfs_capacity


def get_tspider_capacity(dataset_id):
    try:
        tspider_cap_list = storekit_api.tspider_capacity(
            {"result_table_id": dataset_id}, retry_times=3, raise_exception=True
        ).data
    except Exception as e:
        logger.error(
            "get dataset_id:{} tspider_capacity error:{}".format(dataset_id, e.message)
        )
        tspider_capacity = 0
        return tspider_capacity
    # the unit which gotten directly from the api is kb, should be converted to byte
    tspider_capacity = (
        tspider_cap_list[0].get("size", 0) * 1024 if tspider_cap_list else 0
    )
    return tspider_capacity


def get_cost_score(uniq_id):
    erp_param = {"Cost": {"starts": [uniq_id], "expression": {"*": True}}}
    try:
        ret_dict = metadata_client.query_via_erp(erp_param)
    except Exception as e:
        logger.error("query_via_erp error:{}".format(e))
        return None
    if ret_dict:
        cost_list = ret_dict.get("Cost", [])
        if cost_list:
            cost_score = cost_list[0].get("capacity_score", -1)
            return cost_score
    return None
