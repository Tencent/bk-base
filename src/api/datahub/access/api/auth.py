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

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from common.local import get_request_username

from datahub import pizza_settings

AUTH_API_URL = pizza_settings.AUTH_API_URL


def add_app_info_before_request(params):
    params["bk_username"] = get_request_username()
    return params


class _AuthApi(object):
    MODULE = "auth"

    def __init__(self):
        self.roles = DataDRFAPISet(
            url=AUTH_API_URL + "raw_data/",
            primary_key="raw_data_id",
            module=self.MODULE,
            description="更新用户角色",
            before_request=add_app_info_before_request,
            custom_config={
                "update_role_users": DRFActionAPI(method="put", url_path="role_users"),
                "query_role_users": DRFActionAPI(method="get", url_path="role_users"),
            },
        )

        self.create_common = DataAPI(
            url=AUTH_API_URL + "tickets/create_common/",
            method="POST",
            module=self.MODULE,
            description="创建单据",
            before_request=add_app_info_before_request,
        )

        self.list_tickets = DataAPI(
            url=AUTH_API_URL + "tickets/",
            method="GET",
            module=self.MODULE,
            description="单据列表",
            before_request=add_app_info_before_request,
        )

        self.withdraw = DataAPI(
            url=AUTH_API_URL + "tickets/{id}/withdraw/",
            method="POST",
            module=self.MODULE,
            description="撤销单据",
            url_keys=["id"],
            before_request=add_app_info_before_request,
        )


AuthApi = _AuthApi()
