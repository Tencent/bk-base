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

from common.api.base import DataAPI, ProxyBaseApi, ProxyDataAPI
from conf.dataapi_settings import CC_API_URL


class _CCApi(ProxyBaseApi):

    MODULE = "CC"

    def __init__(self):
        self.get_app_list = ProxyDataAPI(
            description="查询业务列表",
        )
        self.get_app_by_id = ProxyDataAPI(
            description="查询业务信息",
        )
        self.get_app_info = DataAPI(
            method="POST",
            url=CC_API_URL + "get_query_info/",
            module=self.MODULE,
            description="查询业务信息",
        )
        self.get_host_by_topo_module_id = DataAPI(
            method="POST",
            url=CC_API_URL + "get_host_by_topo_module_id/",
            module=self.MODULE,
            description="根据topo_module_id查询host信息",
        )
        self.get_host_by_topo_set_id = DataAPI(
            method="POST",
            url=CC_API_URL + "get_host_by_topo_set_id/",
            module=self.MODULE,
            description="根据topo_set_id查询host信息",
        )
        self.get_host_by_app_id = DataAPI(
            method="GET",
            url=CC_API_URL + "get_host_by_app_id/",
            module=self.MODULE,
            description="根据app_id查询机器列表",
        )


CCApi = _CCApi()
