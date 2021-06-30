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
from functools import wraps

from common.local import get_local_param
from rest_framework.viewsets import GenericViewSet

from .exceptions import NoIdentityError, PermissionDeniedError, WrongIdentityError
from .identities import InnerIdentity, InnerUserIdentity, TokenIdentity, UserIdentity
from .middlewares import get_identity
from .perms import TokenPerm, UserPerm


def check_perm(action_id, object_id=None, raise_exception=True):
    """
    检查调用方是否具有操作权限，规定了 API 校验流程

    @param {String} [object_id] 对象ID
    @param {String} action_id 操作方式
    @param {String} [bk_username] 调用者，若 None 则从环境中获取
    @param {String} [bk_app_code] 调用方 APP CODE，若 None 则从环境中获取
    @param {Boolean} [raise_exception] 校验不通过，是否抛出异常

    @paramExample
        {
            object_class: "result_table",
            action_id: "query_data",
            object_id: "11_xxx"
        }
    """
    identity = get_identity()

    check_ret = False

    if identity is None:
        raise NoIdentityError()

    if isinstance(identity, InnerIdentity):
        check_ret = True

    elif isinstance(identity, TokenIdentity):
        check_ret = TokenPerm(identity.data_token).check(identity.bk_app_code, action_id, object_id)

    elif isinstance(identity, UserIdentity) or isinstance(identity, InnerUserIdentity):
        check_ret = UserPerm(identity.bk_username).check(identity.bk_app_code, action_id, object_id)

    if raise_exception and not check_ret:
        raise PermissionDeniedError()

    return check_ret


def perm_check(action_id, detail=True, url_key=None):
    """
    支持 viewset 函数的权限校验，从 URL 或者参数中通过 KEY 寻址方式获取获取 object_id

    @param {string} action_id 操作方式
    @param {string} url_key object_id 在 URL 中的 KEY
    @param {boolean} detail 是否存在 object_id
    """

    def _deco(view_func):
        @wraps(view_func)
        def _wrap_perm_check(self, request, *arg, **kwargs):

            if not isinstance(self, GenericViewSet):
                raise Exception("Only used in ther GenericViewSet")

            object_id = None

            # 是否需要校验至具体对象
            if detail:
                if url_key:
                    object_id = self.kwargs[url_key]
                elif isinstance(self, GenericViewSet):
                    object_id = self.kwargs[self.lookup_field]

            check_perm(action_id, object_id, raise_exception=True)
            return view_func(self, request, *arg, **kwargs)

        return _wrap_perm_check

    return _deco


def require_indentities(indentities=None):
    """
    申明仅支持的认证方式
    """

    def _deco(view_func):
        @wraps(view_func)
        def _wrap(self, request, *args, **kwargs):
            identity = get_local_param("identity")
            if identity is not None and (not indentities or identity.NAME in indentities):
                return view_func(self, request, *args, **kwargs)

            if indentities:
                raise WrongIdentityError("Wrong authentication information, only {} required".format(indentities))
            else:
                raise WrongIdentityError("No authentication information...")

        return _wrap

    return _deco
