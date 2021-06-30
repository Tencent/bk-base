# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.http import QueryDict
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.utils.translation import ugettext_lazy as _
from django.views import View
from rest_framework import serializers
from rest_framework.response import Response

from apps.api import AuthApi
from apps.common.views import list_route
from apps.dataflow.datamart.datadict import list_dataset_in_apply
from apps.dataflow.handlers.business import Business
from apps.dataflow.models import OAuthApplication
from apps.exceptions import ApiRequestError, DataError
from apps.generic import APIViewSet


class AuthViewSet(APIViewSet):
    @list_route()
    def user_perm_scopes(self, request):
        return Response(
            AuthApi.get_user_perm_scope(
                {
                    "user_id": request.user.username,
                    "action_id": request.query_params.get("action_id"),
                    "show_display": request.query_params.get("show_display"),
                }
            )
        )

    @list_route(methods=["get"], url_path="judge_permission")
    def judge_permission(self, request):
        """
        @api {get} /auth/judge_permission/ 判断用户是否有对象的权限，以及权限是否正在申请中
        @apiName judge_permission
        @apiGroup Auth
        @apiParam {String} action_id 操作类型 result_table.query_data,raw_data.query_data,raw_data.etl
        @apiParam {string} object_id 对象ID
        @apiSuccessExample {json} 成功返回:
        {
            "message":"",
            "code":"00",
            "data":{
                "has_permission":true,
                "ticket_status":""
            },
            "result":true
        }
        """
        object_id = request.query_params.get("object_id")
        try:
            permission_dict = AuthApi.check_user_perm(
                {
                    "user_id": request.user.username,
                    "action_id": request.query_params.get("action_id"),
                    "object_id": request.query_params.get("object_id"),
                    "display_detail": True,
                }
            )
            has_permission = permission_dict.get("result", False)
        except DataError:
            has_permission = False
        username = request.user.username
        tickets = list_dataset_in_apply(username)
        ticket_status = "processing" if object_id in tickets else ""
        res = {"has_permission": has_permission, "ticket_status": ticket_status}
        if not has_permission:
            res["no_pers_reason"] = permission_dict.get("message", "")
        return Response(res)


class AuthorizeSerializer(serializers.Serializer):
    bk_app_code = serializers.CharField()
    data_token_id = serializers.IntegerField()
    state = serializers.CharField()
    redirect_url = serializers.CharField()
    scopes = serializers.CharField()
    is_popup = serializers.BooleanField(default=False, required=False)


class ParameterError(Exception):
    pass


class OauthAuthorizeView(View):
    class ErrTemplate(object):
        NO_AUTHOTIZATED_APP = _("应用 {bk_app_code} 未注册，请联系管理员")
        SERIALIZE_ERR = _("请求参数不符合预期，<hr/>{errors}")
        DATATOKEN_ERR = _("非法授权码，请传递正确的授权码ID")
        DATATOKE_NOT_MATCH_APPCODE = _("授权码ID与应用ID不一致")
        NO_RTS_ERR = _("请传递需要授权的结果表")
        SINGLE_BUSINESS = _("目前仅支持同一业务授权")

    def get(self, request):
        """
        Oauth 授权页面，目前仅支持结果表列表的授权
        """
        try:
            cleaned_data = self._clean(request.GET)
        except ParameterError as err:
            return render(request, "auth/oauth_400.html", {"message": str(err)})

        user_logo_url = request.user.avatar_url

        apps = OAuthApplication.objects.filter(bk_app_code=cleaned_data["bk_app_code"])

        if len(apps) == 0:
            _err_template = self.ErrTemplate.NO_AUTHOTIZATED_APP
            return render(
                request,
                "auth/oauth_400.html",
                {"message": _err_template.format(bk_app_code=cleaned_data["bk_app_code"])},
            )

        bk_app_logo = apps[0].bk_app_logo
        bk_app_name = apps[0].bk_app_name
        bk_biz_id = cleaned_data["bk_biz_id"]

        cleaned_data["user_logo_url"] = user_logo_url
        cleaned_data["bk_biz_name"] = Business(bk_biz_id).get_biz_name()
        cleaned_data["bk_app_logo"] = bk_app_logo
        cleaned_data["bk_app_name"] = bk_app_name
        cleaned_data["result_tables_length"] = len(cleaned_data["result_tables"])

        return render(request, "auth/oauth_authorize.html", cleaned_data)

    def post(self, request):
        """
        Oauth 提交授权
        """
        cleaned_data = request.POST
        scopes = cleaned_data["scopes"].strip()
        result_table_ids = [] if scopes == "" else scopes.split(",")

        redirect_url = cleaned_data["redirect_url"]
        state = cleaned_data["state"]
        data_token_id = cleaned_data["data_token_id"]
        bk_app_code = cleaned_data["bk_app_code"]

        # 组装回调 URL
        querystring = QueryDict({}, mutable=True)
        querystring["state"] = state
        querystring["data_token_id"] = data_token_id
        querystring["bk_app_code"] = bk_app_code

        whole_redirct_url = "{}?{}".format(redirect_url, querystring.urlencode(safe="/"))

        if len(result_table_ids) == 0:
            return HttpResponseRedirect(whole_redirct_url)

        data_scope = {
            "permissions": [
                {
                    "action_id": "result_table.query_data",
                    "object_class": "result_table",
                    "scope_id_key": "result_table_id",
                    "scope_object_class": "result_table",
                    "scope": {"result_table_id": result_table_id},
                }
                for result_table_id in result_table_ids
            ]
        }

        AuthApi.tokens.update(
            {
                "data_token_id": data_token_id,
                "data_token_bk_app_code": bk_app_code,
                "data_scope": data_scope,
                "reason": _("用户 {} 期望在 {} 上使用业务数据").format(request.user.username, bk_app_code),
                "expire": 360,
            }
        )
        return HttpResponseRedirect(whole_redirct_url)

    def _clean(self, data):
        """
        清理参数
        """
        # 基本参数校验
        serializer = AuthorizeSerializer(data=data)
        if not serializer.is_valid():
            _errors = "<br/>".join("（{}, {}）".format(item[0], item[1][0]) for item in list(serializer.errors.items()))
            _message = self.ErrTemplate.SERIALIZE_ERR.format(errors=_errors)
            raise ParameterError(_message)

        cleaned_data = serializer.data
        result_table_ids = cleaned_data["scopes"].split(",")
        bk_app_code = cleaned_data["bk_app_code"]
        is_popup = cleaned_data.get("is_popup", False)

        # 结果表不可为空
        if len(result_table_ids) == 0:
            raise ParameterError(self.ErrTemplate.NO_RTS_ERR)

        # 获取 DataToken
        try:
            datatoken = AuthApi.tokens.retrieve(
                {
                    "data_token_id": cleaned_data["data_token_id"],
                    "permission_status": "active,applying",
                    "show_display": False,
                    "show_scope_structure": False,
                }
            )
        except ApiRequestError:
            raise ParameterError(self.ErrTemplate.DATATOKEN_ERR)

        if datatoken["data_token_bk_app_code"] != bk_app_code:
            raise ParameterError(self.ErrTemplate.DATATOKE_NOT_MATCH_APPCODE)

        permission_mapping = dict()
        for perm in datatoken["permissions"]:
            if (
                perm["action_id"] == "result_table.query_data"
                and perm["scope_object_class"] == "result_table"
                and perm["status"] in ("active", "applying")
            ):

                permission_mapping[perm["scope"]["result_table_id"]] = {"status": perm["status"]}

        # 标记结果表状态
        result_tables = []
        for _id in result_table_ids:
            _status = permission_mapping[_id]["status"] if _id in permission_mapping else None
            result_tables.append({"result_table_id": _id, "status": _status, "bk_biz_id": _id.split("_", 1)[0]})

        # 目前要把结果表申请限制在一个业务下
        bk_biz_ids = {rt["bk_biz_id"] for rt in result_tables}
        if len(bk_biz_ids) != 1:
            raise ParameterError(self.ErrTemplate.SINGLE_BUSINESS)
        else:
            bk_biz_id = bk_biz_ids.pop()

        # 只有没有申请过的结果表的才需要重新提交申请
        result_table_ids = [rt["result_table_id"] for rt in result_tables if rt["status"] is None]

        context = {
            "bk_biz_id": bk_biz_id,
            "bk_app_code": bk_app_code,
            "data_token_id": cleaned_data["data_token_id"],
            "state": cleaned_data["state"],
            "redirect_url": cleaned_data["redirect_url"],
            "result_tables": result_tables,
            "scopes": ",".join(result_table_ids),
            "is_popup": is_popup,
        }
        return context
