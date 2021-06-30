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


from datamanage.pizza_settings import STOREKIT_API_ROOT

from common.api.base import DataAPI, DataDRFAPISet


class _StorekitApi(object):
    def __init__(self):
        self.clusters = DataDRFAPISet(
            url=STOREKIT_API_ROOT + "/clusters/{cluster_type}/",
            primary_key="cluster_name",
            url_keys=["cluster_type"],
            module="storekit",
            description="获取存储集群信息",
        )

        self.tspider_capacity = DataAPI(
            url=STOREKIT_API_ROOT + "/capacities/tspider/capacity/",
            method="GET",
            module="storekit",
            description="获取tspier表的容量信息",
            default_timeout=300,
        )

        self.schema_and_sql = DataAPI(
            url=STOREKIT_API_ROOT + "/result_tables/{result_table_id}/schema_and_sql/",
            method="GET",
            url_keys=["result_table_id"],
            module="storekit",
            description="获取结果表可查询的存储",
        )


StorekitApi = _StorekitApi()
