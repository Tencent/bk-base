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
from __future__ import absolute_import, unicode_literals

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.local import get_request_username
from datahub.databus.settings import AUTH_API_URL


def add_app_info_before_request(params):
    params["bk_username"] = get_request_username()
    return params


class _AuthApi(object):

    MODULE = "auth"

    def __init__(self):
        self.raw_data = DataDRFAPISet(
            url=AUTH_API_URL + "raw_data/",
            primary_key="raw_data_id",
            module=self.MODULE,
            description="获取权限集群组",
            before_request=add_app_info_before_request,
            custom_config={
                "cluster_group": DRFActionAPI(method="get", url_path="cluster_group"),
            },
        )
        self.exchange_queue_user = DataDRFAPISet(
            url=AUTH_API_URL + "tokens/exchange_queue_user/",
            primary_key=None,
            module=self.MODULE,
            description="置换队列服务账号",
            before_request=add_app_info_before_request,
        )


AuthApi = _AuthApi()
