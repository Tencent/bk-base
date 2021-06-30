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


from auth.core.beauty import hanlder_auth_check_exception
from auth.core.permission.base import BasePermissionMixin
from auth.exceptions import (
    AppNotMatchErr,
    PermissionDeniedError,
    TokenDisabledErr,
    TokenExpiredErr,
)
from auth.handlers.object_classes import oFactory
from auth.models.auth_models import AuthDataToken
from auth.models.base_models import ActionConfig


class TokenPermission(BasePermissionMixin):
    def __init__(self, token):
        self.token = token
        self.o_token = AuthDataToken.objects.get(data_token=self.token)

    def check_app_code(self, bk_app_code):
        if self.o_token.data_token_bk_app_code != bk_app_code:
            raise AppNotMatchErr()

        return True

    def check_token_status(self):
        """
        检查 Token 状态是否正常
        """
        if self.o_token.status == self.o_token.STATUS.EXPIRED:
            raise TokenExpiredErr()
        if self.o_token.status == self.o_token.STATUS.DISABLED:
            raise TokenDisabledErr()

        return True

    @hanlder_auth_check_exception
    def check(self, action_id, object_id=None, bk_app_code="", raise_exception=False, display_detail=False):
        """
        校验主体是否具有操作对象的权限

        @param {Boolean} raise_exception 是否抛出异常
        @param {Boolean} display_detail 是否显示详细的鉴权信息
        """
        ret = self.interpret_check(
            action_id, object_id, bk_app_code, raise_exception=raise_exception, display_detail=display_detail
        )
        if ret is not None:
            return ret

        self.check_app_code(bk_app_code)
        self.check_token_status()

        action_ids = [action_id]
        ActionConfig.get_parent_action_ids(action_id, action_ids)

        for action_id in action_ids:
            if self.core_check(action_id, object_id):
                return True

        raise PermissionDeniedError()

    def core_check(self, action_id, object_id):

        scopes = self.get_scopes(action_id)
        # 不存在匹配策略，直接结束
        if len(scopes) == 0:
            return False

        # 不存在 object_id，不需要进行 scope 校验
        if object_id is None:
            return True

        _obj = oFactory.init_instance(action_id, object_id)

        for _scope in scopes:
            if _obj.is_in_scope(_scope, action_id):
                return True

        return False

    def get_scopes(self, action_id):
        permissions = self.o_token.permissions
        scopes = [perm["scope"] for perm in permissions if perm["action_id"] == action_id]

        return scopes
