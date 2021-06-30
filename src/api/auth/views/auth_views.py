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
from django.conf import settings
from rest_framework.response import Response

from auth.api import ESBApi
from auth.exceptions import AuthCode
from auth.models.base_models import (
    ActionConfig,
    ObjectConfig,
    RoleConfig,
    RolePolicyInfo,
)
from auth.views.auth_serializers import AuthSyncSerializer
from common.decorators import list_route, params_valid
from common.errorcodes import ErrorCode
from common.exceptions import CommonCode
from common.views import APIView, APIViewSet


class HealthCheckView(APIView):
    def get(self, request):
        return Response("Hello Auth")


class CoreConfigViewSet(APIViewSet):
    def get(self, request):
        return Response(
            {
                "ObjectConfig": list(ObjectConfig.objects.values()),
                "ActionConfig": list(ActionConfig.objects.values()),
                "RoleConfig": list(RoleConfig.objects.values()),
                "RolePolicyInfo": list(RolePolicyInfo.objects.values()),
            }
        )


def get_codes(CodeCls, module_code):
    """
    从框架的标准错误码管理类中提取标准化信息
    """
    error_codes = []
    for name in dir(CodeCls):
        if name.startswith("__"):
            continue
        value = getattr(CodeCls, name)
        if type(value) == tuple:
            error_codes.append(
                {
                    "code": "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, module_code, value[0]),
                    "name": name,
                    "message": value[1],
                    "solution": value[2] if len(value) > 2 else "-",
                }
            )
    error_codes = sorted(error_codes, key=lambda _c: _c["code"])
    return error_codes


class AuthErrorCodesView(APIView):
    def get(self, request):
        return Response(get_codes(AuthCode, ErrorCode.BKDATA_AUTH))


class CommonErrorCodesView(APIView):
    def get(self, request):
        return Response(get_codes(CommonCode, ErrorCode.BKDATA_COMMON))


class AuthSyncViewSet(APIViewSet):
    @params_valid(AuthSyncSerializer, add_params=False)
    def post(self, request):
        """
        @api {post} /auth/auth_sync/ 同步权限
        @apiName auth_sync
        @apiGroup AuthSync
        @apiParam {List[Dict]} data_set_deletions 被删除的数据集
        @apiParam {List[Dict]} data_set_influences 受影响的数据集
        @apiParam {Dict} data_set_influences.data_set_type 数据集类型(result_table | raw_data)
        @apiParam {Dict} data_set_influences.data_set_id 数据集id
        @apiParamExample {json} 校验授权码对结果表是否有查询数据权限
        {
            "bk_username": "admin",
            "data_set_deletions": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": "591_stream1128"
                },
                {
                    "data_set_type": "raw_data",
                    "data_set_id": "328"
                }
            ],
            "data_set_influences": [
                {
                    "data_set_type": "raw_data",
                    "data_set_id": "329"
                },
                {
                    "data_set_type": "result_table",
                    "data_set_id": "591_hive_test_101"
                }
            ]
        }
        @apiSuccessExample {json} Success-Response.data
            'ok'
        """
        return Response("ok")


class ESBControlViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="list_public_keys")
    def list_public_keys(self, request):
        """
        @api {post} /v3/auth/esb/list_public_keys/ 获取网关公钥
        @apiName list_public_keys
        @apiGroup AuthESB
        @apiSuccessExample
            {
                'default': 'xxx',
                'bk-data-inner': 'xxx',
                'bk-data-inner': 'xxx',
                'bk-data-inner': 'xxx'
            }
        """
        APIGW_PUBLIC_KEY_FROM_ESB = getattr(settings, "APIGW_PUBLIC_KEY_FROM_ESB", False)

        # 如果本地没有配置解密的公钥
        if APIGW_PUBLIC_KEY_FROM_ESB:
            response = ESBApi.get_api_public_key({"bk_username": "admin"}, raise_exception=True)
            keys = {"default": response.data["public_key"]}
        else:
            keys = getattr(settings, "APIGW_PUBLIC_KEYS", {})

        return Response(keys)
