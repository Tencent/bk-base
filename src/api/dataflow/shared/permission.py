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

import functools
from functools import wraps

from common.auth import check_perm
from common.auth.exceptions import PermissionDeniedError
from common.auth.perms import ProjectPerm
from common.local import get_request_username
from django.utils.decorators import available_attrs
from django.utils.translation import ugettext_lazy as _
from rest_framework.viewsets import GenericViewSet

from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper


# 针对批量操作，解析在body的对象参数，并进行逐个校验
def perm_check_param(action_id, url_key, is_array=False, detail=False):
    """
    支持 viewset 函数的权限校验，从 URL 或者 body 中获取 object_id 并检查权限
    非GET接口必须加参数校验器，并且加在参数校验器后面

    @param {string} action_id 操作方式
    @param {string} url_key object_id 在 URL或body 中的 KEY
    @param {boolean} detail 是否存在 object_id于url中
    """

    def _deco(view_func):
        @wraps(view_func, assigned=available_attrs(view_func))
        def _wrap_perm_check(self, request, *arg, **kwargs):

            if not isinstance(self, GenericViewSet):
                raise Exception("Only used in the GenericViewSet")

            # 是否需要校验至具体对象
            if detail and isinstance(self, GenericViewSet):
                object_id = self.kwargs[self.lookup_field]
                check_perm(action_id, object_id, raise_exception=True)

            if request.method in ["GET"]:
                # GET无需进行参数校验，直接从query_params取
                if is_array:
                    object_ids = [request.query_params[url_key]] if url_key in request.query_params else []
                else:
                    object_ids = request.query_params.getlist(url_key)
            else:
                # 其它方法都需进行参数校验，从cleaned_params取
                if is_array:
                    object_ids = request.cleaned_params[url_key]
                else:
                    object_ids = [request.cleaned_params[url_key]]

            for object_id in object_ids:
                check_perm(action_id, object_id, raise_exception=True)

            return view_func(self, request, *arg, **kwargs)

        return _wrap_perm_check

    return _deco


def perm_check_tag(tags, result_table_id):
    """
    检查项目 tag 是否与数据对得上
    @param tags:
    @param result_table_id:
    @return:
    """
    rt_tag = TagHelper.get_geog_area_code_by_rt_id(result_table_id)
    if rt_tag in tags:
        return True
    else:
        raise PermissionDeniedError(_("当前项目与结果表(%s)不拥有相同标签，不可使用该结果表数据.") % result_table_id)


def perm_check_data(project_id, result_table_id):
    """
    检查是否有数据权限
    @param project_id:
    @param result_table_id:
    @return:
    """
    if ProjectPerm(project_id).check_data(result_table_id):
        return True
    else:
        raise PermissionDeniedError(_("当前项目无权限操作结果表(%s)数据.") % result_table_id)


def perm_check_cluster_group(project_id, cluster_group_id):
    """
    检查是否有集群组权限
    @param project_id:
    @param cluster_group_id:
    @return:
    """
    if ProjectPerm(project_id).check_cluster_group(cluster_group_id):
        return True
    else:
        raise PermissionDeniedError(_("当前项目无权限操作集群组(%s).") % cluster_group_id)


def require_username(view_func):
    """
    强制要求输入用户名
    """

    @functools.wraps(view_func)
    def wrapper(self, request, *args, **kwargs):
        if not get_request_username():
            raise Exception(_("参数错误, 当前接口调用需传入bk_username参数."))
        return view_func(self, request, *args, **kwargs)

    return wrapper


def local_perm_check(action_id, detail=True, url_key=None):
    """
    本地测试时，由于所有的数据都在本地的，应绕过云端的权限认证
    @param action_id:
    @param detail:
    @param url_key:
    @return:
    """

    def _deco(view_func):
        @wraps(view_func, assigned=available_attrs(view_func))
        def _wrap_perm_check(self, request, *arg, **kwargs):

            return view_func(self, request, *arg, **kwargs)

        return _wrap_perm_check

    return _deco


def local_perm_check_param(action_id, url_key, is_array=False, detail=False):
    """
    本地测试时，由于所有的数据都在本地的，应绕过云端的权限认证
    @param action_id:
    @param url_key:
    @param is_array:
    @param detail:
    @return:
    """

    def _deco(view_func):
        @wraps(view_func, assigned=available_attrs(view_func))
        def _wrap_perm_check(self, request, *arg, **kwargs):

            return view_func(self, request, *arg, **kwargs)

        return _wrap_perm_check

    return _deco


def update_scope_role(role_id, scope_id, user_ids):
    """
    为用户列表(user_ids)对于某个对象(scope)赋予某个角色(role_id)的权限
    @param role_id:
    @param scope_id:
    @param user_ids:
    @return:
    """
    return AuthHelper.update_scope_role(role_id, scope_id, user_ids)


def perm_check_user(action_id, object_ids):
    """
    校验用户是否有操作对象列表的权限
    @param action_id:
    @param object_ids:
    @return:
    """
    data = AuthHelper.batch_check_user(get_request_username(), action_id, object_ids)
    for permission_result in data:
        if not permission_result["result"]:
            raise PermissionDeniedError(_("您没有查看对象(%s)的权限.") % permission_result["object_id"])


class LocalUserPerm(object):
    def __init__(self, *args, **kwargs):
        pass

    def list_scopes(self, *args, **kwargs):
        return [{"project_id": 162}]  # mock 返回
