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
from auth.handlers.raw_data import RawDataHandler
from auth.handlers.role import RoleHandler
from auth.models.base_models import RoleConfig
from auth.services.role import RoleService
from auth.views.auth_serializers import RawDataUsersUpdateSerializer
from common.decorators import detail_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from rest_framework.response import Response


class RawDataViewSet(APIViewSet):
    lookup_field = "raw_data_id"

    @detail_route(methods=["GET", "PUT"])
    def role_users(self, request, raw_data_id):
        if request.method == "GET":
            return self._get_role_users(request, raw_data_id)

        if request.method == "PUT":
            return self._update_role_users(request, raw_data_id)

    @staticmethod
    def _get_role_users(request, raw_data_id):
        """
        @api {get} /auth/raw_data/:raw_data_id/role_users/ 列举原始数据相关用户
        @apiName list_raw_data_role_user
        @apiGroup RawDataUser
        @apiSuccessExample {json} Success-Response.data
            [
                {
                    'role_id': 'raw_data.manager',
                    'role_name': '原始数据管理员',
                    'user_ids': ['user00', 'user01']
                }
            ]
        """
        roles = RoleConfig.objects.filter(object_class_id="raw_data")
        role_users = [
            {
                "role_id": _role.role_id,
                "role_name": _role.role_name,
                "users": RoleHandler.list_users(_role.role_id, scope_id=raw_data_id),
            }
            for _role in roles
        ]
        return Response(role_users)

    @params_valid(RawDataUsersUpdateSerializer, add_params=False)
    def _update_role_users(self, request, raw_data_id):
        """
        @api {put} /auth/raw_data/:raw_data_id/role_users/ 更新原始数据相关用户
        @apiName update_raw_data_role_user
        @apiGroup RawDataUser
        @apiParam {object[]} role_users 用户列表
        @apiParamExample {json} role_users 参数例子
            {
                role_users: [
                    {
                        'role_id': 'raw_data.manager',
                        'user_ids': ['user00', 'user01']
                    },
                ]
            }
        @apiSuccessExample {json} Success-Response.data
            'ok'
        """
        role_users = request.cleaned_params["role_users"]

        username = get_request_username()
        username = username if username else "admin"
        for _users in role_users:
            _role_id = _users["role_id"]
            _user_ids = _users["user_ids"]
            RoleService.update_roles(username, _role_id, _user_ids, raw_data_id, force_no_ticket=True)

        return Response("ok")

    @detail_route(methods=["GET"])
    def cluster_group(self, request, raw_data_id):
        """
        @api {get} /auth/raw_data/:raw_data_id/cluster_group/ 列举 raw_data 有权限的集群组
        @apiName list_raw_data_cluster_group
        @apiGroup RawDataClusterGroup
        @apiSuccessExample {json} Success-Response.data
            [
                {
                    "cluster_group_id": "mysql_lol_1",
                    "cluster_group_name": "mysql_lol_1",
                    "cluster_group_alias": "mysql_lol_1",
                    "scope": "private",
                    "created_by": "asdf",
                    "created_at": "2018-11-27 17:21:20",
                    "updated_by": "",
                    "updated_at": null,
                    "description": null
                },
                {
                    "cluster_group_id": "mysql_lol_2",
                    "cluster_group_name": "mysql_lol_2",
                    "cluster_group_alias": "mysql_lol_2",
                    "scope": "public",
                    "created_by": "asdf",
                    "created_at": "2018-11-27 17:33:35",
                    "updated_by": null,
                    "updated_at": null,
                    "description": null
                }
            ]
        """
        return Response(RawDataHandler(raw_data_id).list_cluster_group())
