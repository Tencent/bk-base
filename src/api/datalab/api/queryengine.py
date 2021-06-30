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

from common.api.base import DataAPI, DataDRFAPISet
from common.api.modules.utils import add_app_info_before_request
from datalab.pizza_settings import QUERYENGINE_API_ROOT


class _QueryEngineApi(object):
    def __init__(self):
        self.query_async = DataAPI(
            url=QUERYENGINE_API_ROOT + "/query_async/",
            method="POST",
            module="queryengine",
            description="获取query_id",
            before_request=add_app_info_before_request,
        )

        self.query_sync = DataAPI(
            url=QUERYENGINE_API_ROOT + "/query_sync/",
            method="POST",
            module="queryengine",
            description="查询数据",
            before_request=add_app_info_before_request,
        )

        self.get_result = DataDRFAPISet(
            url=QUERYENGINE_API_ROOT + "/query_async/result/",
            primary_key="query_id",
            module="queryengine",
            description="获取结果集",
            custom_headers={"Content-Type": "application/json"},
        )

        self.get_stage = DataDRFAPISet(
            url=QUERYENGINE_API_ROOT + "/query_async/stage/",
            primary_key="query_id",
            module="queryengine",
            description="获取SQL作业轨迹",
            custom_headers={"Content-Type": "application/json"},
        )

        self.get_info_list = DataAPI(
            url=QUERYENGINE_API_ROOT + "/query_async/info_list/",
            method="POST",
            module="queryengine",
            description="获取info列表",
            custom_headers={"Content-Type": "application/json"},
        )

        self.get_schema = DataDRFAPISet(
            url=QUERYENGINE_API_ROOT + "/dataset/",
            primary_key="query_id",
            module="queryengine",
            description="获取表结构信息",
            custom_headers={"Content-Type": "application/json"},
        )

        self.get_sqltype_and_result_tables = DataAPI(
            url=QUERYENGINE_API_ROOT + "/sqlparse/sqltype_and_result_tables/",
            method="POST",
            module="queryengine",
            description="获取结果表名",
            before_request=add_app_info_before_request,
        )

        self.delete_job_conf = DataAPI(
            url=QUERYENGINE_API_ROOT + "/batch/custom_jobs/{result_table_id}",
            method="DELETE",
            module="queryengine",
            description="清除job配置",
            url_keys=["result_table_id"],
            custom_headers={"Content-Type": "application/json"},
        )


QueryEngineApi = _QueryEngineApi()
