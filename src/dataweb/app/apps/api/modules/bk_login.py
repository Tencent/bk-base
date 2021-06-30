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

from apps.api.base import BaseApi, DataAPI, DataAPISet, ProxyDataAPI
from apps.api.modules.utils import add_esb_info_before_request
from common.log import logger


class _BKLoginApi(DataAPISet):
    MODULE = _("BK_Login")

    def __init__(self):
        try:
            from config.domains import BK_LOGIN_APIGATEWAY_ROOT

            self.get_all_user = DataAPI(
                method="POST",
                url=BK_LOGIN_APIGATEWAY_ROOT + "get_all_user/",
                module=self.MODULE,
                description="获取所有用户",
                before_request=add_esb_info_before_request,
                cache_time=300,
            )
            self.get_user = DataAPI(
                method="POST",
                url=BK_LOGIN_APIGATEWAY_ROOT + "get_user/",
                module=self.MODULE,
                description="获取单个用户",
                before_request=add_esb_info_before_request,
            )
            self.get_batch_user_platform_role = DataAPI(
                method="GET",
                url=BK_LOGIN_APIGATEWAY_ROOT + "get_batch_user_platform_role",
                module=self.MODULE,
                description="获取多个用户在平台应用的角色",
                before_request=add_esb_info_before_request,
            )
        except ImportError:
            logger.error("无法在配置文件中找到BK_LOGIN_API_URL")


class BKLoginApiProxy(BaseApi):
    def __init__(self, default, proxy):
        super().__init__(default, proxy)

        self.get_all_user = ProxyDataAPI(description="获取所有用户")
        self.get_user = ProxyDataAPI(description="获取单个用户")
        self.get_batch_user_platform_role = ProxyDataAPI(description="获取多个用户在平台应用的角色")


BKLoginApi = BKLoginApiProxy(
    default=_BKLoginApi(), proxy={"ieod": "extend.tencent.common.api.modules.bk_login._BKLoginApi"}
)
