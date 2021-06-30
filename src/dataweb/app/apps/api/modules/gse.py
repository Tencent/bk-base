# -*- coding=utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from apps.api.modules.utils import add_esb_info_before_request

from ..base import BaseApi, DataAPI, DataAPISet, ProxyDataAPI


class _GseApi(DataAPISet):

    MODULE = "GSE"

    def __init__(self):
        from config.domains import GSE_APIGATEWAY_ROOT_V2

        self.get_agent_status = DataAPI(
            method="POST",
            url=GSE_APIGATEWAY_ROOT_V2 + "get_agent_status",
            module=self.MODULE,
            description="查询agent实时在线状态",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            after_request=None,
        )


class GseApiProxy(BaseApi):
    def __init__(self, default, proxy):
        super().__init__(default, proxy)

        self.get_agent_status = ProxyDataAPI("查询agent实时在线状态")


GseApi = GseApiProxy(default=_GseApi(), proxy={"ieod": "extend.tencent.common.api.modules.gse._GseApi"})
