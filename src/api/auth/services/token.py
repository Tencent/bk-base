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
import datetime

from auth.config.ticket import TOKEN_DATA
from auth.config.token import DIRECT_PASS_ACTION, DIRECT_PASS_OBJECT, IS_DIRECT_PASS
from auth.constants import TOKEN, TokenPermissionStatus
from auth.core.ticket import TicketFactory
from auth.exceptions import DataScopeValidErr
from auth.handlers.role import RoleHandler
from auth.models.auth_models import AuthDataToken
from auth.utils import generate_random_string
from django.db import transaction
from django.utils import timezone
from django.utils.translation import ugettext as _


class TokenGenerator:
    def __init__(self, bk_username, data_token_bk_app_code, data_scope, expire):
        self.bk_username = bk_username
        self.data_token_bk_app_code = data_token_bk_app_code
        self.data_scope = data_scope
        self.expire = expire

    def create_token(self, reason=""):
        """
        创建token
        @return:
        """
        # 校验对象范围是否合法
        if not self.check_scope():
            raise DataScopeValidErr()

        with transaction.atomic(using="basic"):
            # 生成token对象
            o_token = self.generate_token(reason)

            # 自动添加为 DataToken 管理员
            RoleHandler.add_role(self.bk_username, "data_token.manager", self.bk_username, o_token.id)

            # 提交审批单
            self.apply_ticket(o_token, reason)

            # 直接授权
            self.add_permission(o_token)

        return o_token

    def update_token(self, o_token, reason=""):
        """
        更新token（添加权限），同create_token
        @param o_token: token对象
        @param reason:
        @return:
        """
        with transaction.atomic(using="basic"):
            # 提交审批单
            self.apply_ticket(o_token, reason)

            # 直接授权
            self.add_permission(o_token)
        return o_token

    def generate_token(self, reason=""):
        """
        随机生成token
        @return: 9KlJKYtWrNhd8nd4PW7Fepmgx3LFAnZRG9R9OsF2qXu876b4P5fH
        """
        # 创建token对象
        length = AuthDataToken._meta.get_field("data_token").max_length
        data_token = generate_random_string(length=length)
        o_token, created = AuthDataToken.objects.get_or_create(
            defaults={
                "created_by": self.bk_username,
                "data_token_bk_app_code": self.data_token_bk_app_code,
                "expired_at": timezone.now() + datetime.timedelta(self.expire),
                "description": reason,
            },
            **{"data_token": data_token}
        )
        if not created:
            # 假设token重复了的情况，重新再生成，触发概率极低
            return self.create_token()

        return o_token

    def apply_ticket(self, o_token, reason):
        """
        申请单据
        @return:
        """
        # 设置token对应的权限
        o_token.apply_permission(self.data_scope)
        data = {
            "created_by": self.bk_username,
            "reason": reason,
            "ticket_type": TOKEN_DATA,
            "permissions": [
                {
                    "subject_id": o_token.id,
                    "subject_name": _("APP(%s)的授权码" % self.data_token_bk_app_code),
                    "subject_class": TOKEN,
                    "action": _perm["action_id"],
                    "object_class": _perm["object_class"],
                    "scope_object_class": _perm["scope_object_class"],
                    "scope": _perm["scope"],
                }
                for _perm in self.need_approval_permission
            ],
        }
        TicketFactory(TOKEN_DATA).generate_ticket_data(data)

    def add_permission(self, o_token):
        """
        向AuthDataTokenPermission更新token权限
        @param o_token:
        """
        data_scope = {"all": False, "permissions": self.direct_pass_permission}
        o_token.update_permission_status(data_scope, status=TokenPermissionStatus.ACTIVE)

    def check_scope(self):
        """
        检查授权范围是否合法，通过handlers.object_classes的方法进行判断
        @return:
        """
        # 检查授权范围是否合法
        for _perm in self.data_scope_permission:
            action_id = _perm["action_id"]

            if action_id == "*":
                raise Exception(_("暂不支持申请全部操作权限"))

            # 检查action所属的对象与传入对象类型是否一致
            self.check_scope_object_class(_perm, raise_exception=True)

        return True

    @staticmethod
    def check_scope_object_class(_perm, raise_exception=False):
        """
        目前暂只支持 action 对应对象的申请
        判断action所属的对象与传入对象类型是否一致
        @param _perm: 权限范围
        @param raise_exception: 是否抛出异常
        @return:
        """
        if _perm["object_class"] != _perm["scope_object_class"]:
            if raise_exception:
                raise DataScopeValidErr()
            else:
                return False
        return True

    @staticmethod
    def is_direct_pass(action_id, object_class):
        """
        是否直接通过并授权，不走单据审批
        @return: True | False
        """
        return IS_DIRECT_PASS and (action_id in DIRECT_PASS_ACTION or object_class in DIRECT_PASS_OBJECT)

    @property
    def direct_pass_permission(self):
        """
        直接通过的权限范围
        @return: 同data_scope_permission
        """
        return [
            perm for perm in self.data_scope_permission if self.is_direct_pass(perm["action_id"], perm["object_class"])
        ]

    @property
    def need_approval_permission(self):
        """
        需要申请单据的权限范围
        @return: 同data_scope_permission
        """
        return [
            perm
            for perm in self.data_scope_permission
            if not self.is_direct_pass(perm["action_id"], perm["object_class"])
        ]

    @property
    def data_scope_permission(self):
        """
        申请的数据范围权限
        @return:
        [
            {
                # *表示对象全部操作，也可使用具体 action_id ，比如 project.update
                "action_id": "*",
                "object_class": "project",
                "scope": {
                    "project_id": 11
                }
            },
            {
                "action_id": "result_table.query_data",
                "object_class": "result_table",
                "scope": {
                    "project_id": 11
                }
            },
            {
                "action_id": "result_table.query_data",
                "object_class": "result_table",
                "scope": {
                    "result_table_id": "1_xxx"
                }
            }
        ]
        """
        return self.data_scope.get("permissions", [])
