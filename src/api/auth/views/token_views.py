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
from auth.audit.audit_stats import AUDIT_TYPE, Stats
from auth.core.permission import TokenPermission, UserPermission
from auth.exceptions import TokenNotExistErr
from auth.handlers.object_classes import oFactory
from auth.models.auth_models import AuthDataToken, DataTokenQueueUser
from auth.models.base_models import ObjectConfig
from auth.permissions import TokenPermission as TokenViewSetPermission
from auth.services.token import TokenGenerator
from auth.utils.filtersets import get_filterset
from auth.utils.serializer import DataPageNumberPagination
from auth.views.auth_serializers import (
    AuthDataTokenSerializer,
    DataTokenCheckSerializer,
    DataTokenCreateSerializer,
    DataTokenRetrieveSerializer,
    DataTokenUpdateSerializer,
)
from common.decorators import detail_route, list_route, params_valid
from common.views import APIModelViewSet
from django.core.cache import cache
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.response import Response

from common import local

CACHE_TIME_600 = 600
CACHE_TIME_60 = 60


class TokenPermViewSet(APIModelViewSet):
    queryset = AuthDataToken.objects.all()
    serializer_class = AuthDataTokenSerializer
    pagination_class = DataPageNumberPagination
    permission_classes = (TokenViewSetPermission,)

    filter_backends = (filters.SearchFilter, DjangoFilterBackend, filters.OrderingFilter)
    filter_class = get_filterset(AuthDataToken)

    @list_route(methods=["POST"])
    @params_valid(DataTokenCheckSerializer, add_params=False)
    def check(self, request):
        """
        @api {post} /auth/tokens/check/  校验 Token 权限
        @apiName check_perm
        @apiGroup TokenPerm
        @apiParam {string} check_app_code 应用ID
        @apiParam {string} check_data_token 授权码，一般为 30 位
        @apiParam {string} action_id 操作类型，例如 result_table.query_data
        @apiParam {string} object_id 对象ID
        @apiParamExample {json} 校验授权码对结果表是否有查询数据权限
            {
                "check_app_code": "data",
                "check_data_token": "xxxxxxxxxx",
                "action_id": "result_table.query_data",
                "object_id": "100107_cpu_parse"
            }
        @apiParamExample {json} 校验授权码对资源组是否有使用权限
            {
                "check_app_code": "data",
                "check_data_token": "xxxxxxxxxx",
                "action_id": "resource_group.use",
                "object_id": "gem"
            }
        @apiSuccessExample {json} Succees-Response.data
            True | False
        """

        app_code = request.cleaned_params["check_app_code"]
        data_token = request.cleaned_params["check_data_token"]
        action_id = request.cleaned_params["action_id"]
        object_id = request.cleaned_params.get("object_id", None)

        cache_key = "token_perm_check:{app_code}_{data_token}_{action_id}_{object_id}".format(
            app_code=app_code, data_token=data_token, action_id=action_id, object_id=object_id
        )
        cache_value = cache.get(cache_key)
        if cache_value is not None:
            ret = cache_value
        else:
            ret = TokenPermission(token=data_token).check(
                action_id, object_id, bk_app_code=app_code, raise_exception=False
            )
            if ret is True:
                # 校验结果为真时缓存10分钟
                cache_time = CACHE_TIME_600
            else:
                cache_time = CACHE_TIME_60
            cache.set(cache_key, ret, cache_time)
        return Response(ret)

    @list_route(methods=["get"])
    def retrive_by_data_token(self, request):
        """
        @api {get} /auth/tokens/retrive_by_data_token/  通过 data_token 查询详情
        @apiName retrive_by_data_token
        @apiGroup TokenPerm
        @apiSuccessExample {json} 返回样例
        {
          id: 70,
          data_token: 'xxxxxxxx',
          data_token_bk_app_code: "dataweb",
          status: "enabled",
          created_by: "xxx",
          created_at: "2019-02-20 17:29:23",
          expired_at: "2019-02-27 17:29:24"
        }
        """
        data_token = request.query_params["search_data_token"]
        o_token = AuthDataToken.objects.get(data_token=data_token)
        return Response(
            {
                "id": o_token.pk,
                "data_token": o_token.data_token,
                "data_token_bk_app_code": o_token.data_token_bk_app_code,
                "status": o_token.status,
                "created_by": o_token.created_by,
                "created_at": o_token.created_at,
                "expired_at": o_token.expired_at,
            }
        )

    @params_valid(DataTokenCreateSerializer, add_params=False)
    def create(self, request, *args, **kwargs):
        """
        @api {post} /auth/token_perm/ 创建token
        @apiName create_token
        @apiGroup TokenPerm
        @apiParam {string} bk_app_code 应用ID
        @apiParam {dict} data_scope 数据范围
        @apiParam {string} expire 过期时间
        @apiParamExample {json} 校验授权码对结果表是否有查询数据权限
        {
            "data_token_bk_app_code":"2018secretary_h5",
            "data_scope":{
                "is_all":false,
                "permissions":[
                    {
                        "action_id":"result_table.query",
                        "object_class":"result_table",
                        "scope_id_key":"result_table_id",
                        "scope_name_key":"result_table_name",
                        "scope_object_class":"result_table",
                        "scope":{
                            "result_table_id":"591_result_table",
                            "result_table_name":"结果表1"
                        }
                    }
                ]
            },
            "reason":"dsff",
            "expire":7
        }
        @apiSuccessExample {json} Succees-Response.data
            True | False
        """
        bk_username = local.get_request_username()
        bk_app_code = request.cleaned_params["data_token_bk_app_code"]
        data_scope = request.cleaned_params["data_scope"]
        expire = request.cleaned_params["expire"]
        token_generator = TokenGenerator(bk_username, bk_app_code, data_scope, expire)
        return Response(token_generator.create_token(request.cleaned_params["reason"]).data_token)

    @params_valid(DataTokenUpdateSerializer, add_params=False)
    def update(self, request, *args, **kwargs):
        """
        @api {put} /auth/token_perm/:id/  校验 Token 权限 参数同创建
        @apiName update_token
        @apiGroup TokenPerm
        """
        bk_username = local.get_request_username()
        data_scope = request.cleaned_params["data_scope"]
        expire = request.cleaned_params["expire"]
        o_token = AuthDataToken.objects.get(pk=kwargs.get("pk"))
        TokenGenerator(bk_username, o_token.data_token_bk_app_code, data_scope, expire).update_token(
            o_token, request.cleaned_params["reason"]
        )
        return Response()

    @params_valid(DataTokenRetrieveSerializer, add_params=False)
    def retrieve(self, request, *args, **kwargs):
        """
        @api {get} /auth/tokens/:id/  查询授权码详情
        @apiName retrieve_token
        @apiParam {String} permission_status 按照权限状态过滤，多个使用逗号隔开，目前有 active,applying,inactive，默认 active
        @apiParam {String} show_display 展示权限对象属性，默认 True，如果想快速获取关键数据，建议设置为 False
        @apiParam {String} show_scope_structure 显示授权码组织结构，默认 True，如果想快速获取关键数据，建议设置为 False
        @apiGroup TokenPerm
        @apiSuccessExample {json} 简单返回样例
            {
              status: "enabled",
              created_at: "2019-02-20 17:29:23",
              updated_by: "",
              permissions: [
                "status": "active",
                "scope_id_key": "result_table_id",
                "updated_by": "user01",
                "created_at": "2019-09-25 21:47:50",
                "updated_at": "2019-09-25 21:48:37",
                "created_by": "user01",
                "scope": {
                    "result_table_id": "591_abcddeeffgg_2"
                },
                "action_id": "result_table.query_queue",
                "object_class": "result_table",
                "id": 145,
                "scope_object_class": "result_table",
                "description": null,
              ],
              data_token_bk_app_code: "dataweb",
              description: null,
              data_token: "ul60********z36z",
              updated_at: null,
              created_by: "xxx",
              _status: "enabled",
              status_display: "已启用",
              id: 70,
              expired_at: "2019-02-27 17:29:24"
            }
        """
        permission_status_arr = request.cleaned_params["permission_status"].split(",")
        show_display = request.cleaned_params["show_display"]
        show_scope_structure = request.cleaned_params["show_scope_structure"]

        o_token = AuthDataToken.objects.get(pk=kwargs.get("pk"))
        data = super().retrieve(request, *args, **kwargs).data
        data["permissions"] = o_token.list_permissions(permission_status_arr)

        if show_display:
            data["permissions"] = oFactory.wrap_display_all(data["permissions"], key_path=["scope"])

        if show_scope_structure:
            data["scopes"] = o_token.scopes
            scope_key_map = ObjectConfig.get_object_scope_key_map()

            # 调整数据结构与前端匹配
            for perm in data["permissions"]:
                scope_name_key = scope_key_map[perm["scope_object_class"]]["scope_name_key"]
                perm["scope_name_key"] = scope_name_key
                perm["scope_display"] = {scope_name_key: perm["scope"].get(scope_name_key)}
        return Response(data)

    def list(self, request, *args, **kwargs):
        """
        @api {post} /v3/auth/tokens/  返回用户有权限的 DataToken
        @apiName list_data_token
        @apiGroup TokenPerm
        @apiSuccessExample {json} 返回样例
            [
                {
                    updated_at: "2019-07-04 18:03:35"
                    id: 428
                    data_token_bk_app_code: "northernlights"
                    data_token: "uKkR********cNgh"
                    updated_by: ""
                    _status: "enabled"
                    created_by: "user01"
                    description: "测试"
                    status_display: "已启用"
                    status: "enabled"
                    expired_nearly: True,
                    created_at: "2019-07-04 18:03:35"
                    expired_at: "2020-07-03 18:03:35"
                 }
             ]
        """
        scopes = UserPermission(local.get_request_username()).get_scopes("data_token.manage")
        data_token_ids = [scope["data_token_id"] for scope in scopes if "data_token_id" in scope]

        _queryset = AuthDataToken.get_status_queryset(request.GET.getlist("status__in"))

        # 过滤用户有权限的内容
        self.queryset = _queryset.filter(id__in=data_token_ids)
        return super().list(request, *args, **kwargs)

    @detail_route(methods=["GET"])
    def plaintext(self, request, *args, **kwargs):
        o_token = AuthDataToken.objects.get(pk=kwargs.get("pk"))
        return Response(o_token.data_token)

    @detail_route(methods=["POST"])
    def renewal(self, request, pk):
        """
        @api {post} /v3/auth/tokens/:id/renewal/  对 DataToken 进行续期
        @apiName renewal_data_token
        @apiGroup TokenPerm
        @apiParam {string} expire 过期时间，可选的有 7, 30, 90, 365
        @apiSuccessExample {json} 返回样例
            'ok'
        """
        expire = request.data["expire"]
        o_token = AuthDataToken.objects.get(pk=pk)
        if o_token.renewal(expire):
            Stats.add_to_audit_action(
                Stats.gene_audit_id(),
                o_token.id,
                AUDIT_TYPE.TOKEN_RENEWAL,
                1,
                [o_token],
                operator=local.get_request_username(),
            )
        return Response("ok")

    @list_route(methods=["POST"])
    def exchange_queue_user(self, request):
        """
        @api {post} /auth/tokens/exchange_queue_user/  置换队列服务账号
        @apiName exchange_queue_user
        @apiGroup TokenPerm
        @apiParam {string} data_token
        @apiSuccessExample {json} 成功返回
            {
                "data_token": "fadfakfdsakfak",
                "queue_user": "test_app&xxxxx",
                "queue_password": "fdafdasfdas"
            }
        """
        data_token = request.data["data_token"]

        try:
            AuthDataToken.objects.get(data_token=data_token)
        except AuthDataToken.DoesNotExist:
            raise TokenNotExistErr()

        queue_user = DataTokenQueueUser.get_or_init(data_token=data_token)
        return Response({"queue_user": queue_user.queue_user, "queue_password": queue_user.queue_password})

    @list_route(methods=["POST"], url_path="exchange_default_data_token")
    def exchange_default_data_token(self, request):
        """
        @api {POST} /auth/tokens/exchange_default_data_token/ 置换默认DataToken
        @apiName exchange_default_data_token
        @apiGroup TokenPerm
        @apiParam {string} data_token_bk_app_code
        @apiSuccessExample {json} 成功返回
          {
            id: 70,
            data_token: 'xxxxxxxx',
            data_token_bk_app_code: "dataweb",
            status: "enabled",
            created_by: "xxx",
            created_at: "2019-02-20 17:29:23",
            expired_at: "2019-02-27 17:29:24"
          }

        @apiErrorExample {json} 成功返回
          None
        """
        data_token_bk_app_code = request.data["data_token_bk_app_code"]

        o_tokens = AuthDataToken.objects.filter(data_token_bk_app_code=data_token_bk_app_code).order_by("pk")

        if o_tokens.count() == 0:
            return Response(None)

        o_token = o_tokens[0]

        return Response(
            {
                "id": o_token.pk,
                "data_token": o_token.data_token,
                "data_token_bk_app_code": o_token.data_token_bk_app_code,
                "status": o_token.status,
                "created_by": o_token.created_by,
                "created_at": o_token.created_at,
                "expired_at": o_token.expired_at,
            }
        )
