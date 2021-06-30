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

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_AUTH_URL

from .test.test_call_auth import TestAuth


class _AuthApi(object):
    test_auth = TestAuth()

    def __init__(self):
        self.projects = DataDRFAPISet(
            url=BASE_AUTH_URL + "projects/",
            primary_key="project_id",
            module="auth",
            description=_("项目信息"),
            before_request=add_app_info_before_request,
            custom_config={
                "cluster_group": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_auth.set_return_value("list_project_cluster_group"),
                ),
                "role_users": DRFActionAPI(method="get", detail=True),
            },
        )

        self.tdw_user = DataDRFAPISet(
            url=BASE_AUTH_URL + "tdw/user/",
            primary_key="user_id",
            module="auth",
            description=_("校验 TDW 用户权限"),
            before_request=add_app_info_before_request,
            custom_config={
                "check_table_perm": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_auth.set_return_value("check_table_perm"),
                ),
                "check_app_group_perm": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_auth.set_return_value("check_app_group_perm"),
                ),
            },
        )

        self.roles = DataDRFAPISet(
            url=BASE_AUTH_URL + "roles/",
            primary_key="role_id",
            module="auth",
            description=_("获取项目成员"),
            before_request=add_app_info_before_request,
            custom_config={"users": DRFActionAPI(method="put", detail=True)},
        )

        self.user_role = DataDRFAPISet(
            url=BASE_AUTH_URL + "roles/user_role/",
            primary_key=None,
            module="auth",
            description=_("获取项目成员"),
            before_request=add_app_info_before_request,
        )

        self.tickets = DataDRFAPISet(
            url=BASE_AUTH_URL + "tickets/",
            primary_key="ticket_id",
            module="auth",
            description="审批单据",
            before_request=add_app_info_before_request,
            custom_config={
                "create_common": DRFActionAPI(method="post", detail=False),
                "withdraw": DRFActionAPI(method="post", detail=True),
            },
        )

        self.users = DataDRFAPISet(
            url=BASE_AUTH_URL + "users/",
            primary_key="user_id",
            module="auth",
            description="用户对对象的权限校验",
            before_request=add_app_info_before_request,
            custom_config={"batch_check": DRFActionAPI(method="post", detail=False)},
        )


AuthApi = _AuthApi()
