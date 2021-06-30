# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.utils.translation import ugettext_lazy as _

from common.log import logger

from ..base import BaseApi, DataAPI, ProxyDataAPI
from .utils import add_esb_info_before_request


class _BKPAASApi(BaseApi):

    MODULE = _("PaaS平台")

    def __init__(self):
        try:
            from config.domains import BK_PAAS_APIGATEWAY_ROOT

            self.get_app_info = DataAPI(
                method="GET",
                url=BK_PAAS_APIGATEWAY_ROOT + "get_app_info/",
                module=self.MODULE,
                description="获取app信息",
                before_request=add_esb_info_before_request,
            )
        except ImportError:
            logger.error("无法在配置文件中找到BK_PAAS_APIGATEWAY_ROOT")


class BKPAASApiProxy(BaseApi):
    def __init__(self, default, proxy):
        super().__init__(default, proxy)

        self.get_app_info = ProxyDataAPI("获取app信息")


BKPAASApi = BKPAASApiProxy(default=_BKPAASApi(), proxy={"ieod": "extend.tencent.common.api.modules.bk_paas._BKPAASApi"})
