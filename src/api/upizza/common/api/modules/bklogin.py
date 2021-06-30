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
from common.api.base import DataAPI, DataAPISet, ProxyBaseApi, ProxyDataAPI
from common.log import logger


class _BkLoginApi(DataAPISet):
    MODULE = "BK_LOGIN"

    def __init__(self):
        try:
            from conf.dataapi_settings import BK_LOGIN_API_URL

            self.get_user_info = DataAPI(method="GET", url=BK_LOGIN_API_URL + "get_user/", module=self.MODULE)
        except ImportError:
            logger.error("无法在配置文件中找到BK_LOGIN_API_URL")


class _BkLoginApiProxy(ProxyBaseApi):
    def __init__(self, default, proxy):
        super().__init__(default, proxy)

        self.get_user_info = ProxyDataAPI(
            description="获取用户信息",
            params=["bk_username", "bk_token", "bk_ticket"],
        )


BkLoginApi = _BkLoginApiProxy(
    default=_BkLoginApi(), proxy={"tencent": "extend.tencent.common.api.modules.bklogin._BkLoginApi"}
)
