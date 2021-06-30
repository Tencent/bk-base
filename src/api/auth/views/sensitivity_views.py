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
import copy

from auth.api.access import AccessApi
from auth.api.meta import MetaApi
from auth.constants import SensitivityConfigs, SensitivityMapping
from auth.handlers.object_classes import AuthRawData, AuthResultTable
from auth.models.auth_models import AuthDatasetSensitivity
from auth.models.base_models import RoleConfig
from auth.services.role import RoleService
from auth.views.auth_serializers import (
    DataSetSensitivityRetrieveSerializer,
    DataSetSensitivityUpdateSerializer,
    SensitivityListSerializer,
)
from common.auth import check_perm
from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from rest_framework.response import Response


class SensitivityViewSet(APIViewSet):

    lookup_field = "sensitivity_id"

    @params_valid(SensitivityListSerializer, add_params=False)
    def list(self, request):
        """
        @api {get} /v3/auth/sensitivity/ 敏感度配置
        @apiName list_sensitivity
        @apiGroup Sensitivity
        @apiParam {String} [has_biz_role] 是否携带业务角色信息
        @apiParam {String} [bk_biz_id] 业务ID
        @apiSuccessExample {json} has_biz_role=True 样例
            [
                {
                    "id": "public",
                    "name": "公开"
                    "description": "接入后平台所有用户均可使用此数",
                    "biz_role_memebers": ["processor666"],
                    "biz_role_name": "业务运维人员",
                    "biz_role_id": "biz.manager",
                    "active": true,
                },
                {
                    "id": "private",
                    "name": "业务私有"
                    "description": "由业务负责人直接接入至平台，业务人员可见，平台用户均可申请此数据",
                    "biz_role_memebers": ["processor666"],
                    "biz_role_name": "业务运维人员",
                    "biz_role_id": "biz.manager",
                    "active": true,
                },
                {
                    "id": "confidential",
                    "name": "业务机密"
                    "description": "由业务负责人接入平台，业务成员不可见，数据申请方需要leader级别",
                    "biz_role_memebers": [],
                    "biz_role_name": "业务总监",
                    "biz_role_id": "biz.leader",
                    "active": true,
                },
                {
                    "id": "topsecret",
                    "name": "业务绝密"
                    "description": "由业务负责人接入平台，仅授权人员可见，数据申请方需要总监级别",
                    "biz_role_memebers": [],
                    "biz_role_name": "业务总监",
                    "biz_role_id": "biz.leader",
                    "active": false,
                }
            ]
        """
        has_biz_role = request.cleaned_params.get("has_biz_role", False)
        bk_biz_id = request.cleaned_params.get("bk_biz_id", None)

        data = copy.deepcopy(SensitivityConfigs)
        roles_map = RoleConfig.get_role_config_map()

        for d in data:
            biz_role_id = d["biz_role_id"]

            if has_biz_role:
                d["biz_role_name"] = roles_map[biz_role_id]["role_name"]
                d["biz_role_memebers"] = RoleService.list_users(role_id=biz_role_id, scope_id=bk_biz_id)
            else:
                d["biz_role_name"] = biz_role_id
                d["biz_role_memebers"] = []

        return Response(data)

    @list_route(methods=["get"], url_path="retrieve_dataset")
    @params_valid(DataSetSensitivityRetrieveSerializer, add_params=False)
    def retrieve_dataset(self, request):
        """
        @api {get} /v3/auth/sensitivity/retrieve_dataset/ 获取数据集的敏感度信息
        @apiName retrieve_dataset
        @apiGroup Sensitivity
        @apiParam {String} data_set_type 数据集类型
        @apiParam {String} data_set_id 数据集ID
        @apiSuccessExample {json} 样例
            {
                "bk_biz_id": "591",
                "biz_role_id": "biz.manager",
                "biz_role_name": "业务运维人员",
                "biz_role_memebers": ["processor666"]
            }
        """
        data_set_type = request.cleaned_params["data_set_type"]
        data_set_id = request.cleaned_params["data_set_id"]

        if data_set_type == "raw_data":
            o_rawdata = AuthRawData(data_set_id)
            sensitivity = o_rawdata.obj__sensitivity
            bk_biz_id = o_rawdata.obj__bk_biz_id
        elif data_set_type == "result_table":
            o_rt = AuthResultTable(data_set_id)
            sensitivity = o_rt.obj__sensitivity
            bk_biz_id = o_rt.obj__bk_biz_id
        else:
            raise

        conf = SensitivityMapping[sensitivity]
        roles_map = RoleConfig.get_role_config_map()

        biz_role_id = conf["biz_role_id"]
        data = {
            "sensitivity_id": sensitivity,
            "sensitivity_name": conf["name"],
            "sensitivity_description": conf["description"],
            "tag_method": AuthDatasetSensitivity.get_dataset_tag_method(data_set_type, data_set_id),
            "bk_biz_id": bk_biz_id,
            "biz_role_id": biz_role_id,
            "biz_role_name": roles_map[biz_role_id]["role_name"],
            "biz_role_memebers": RoleService.list_users(role_id=biz_role_id, scope_id=bk_biz_id),
        }

        return Response(data)

    @list_route(methods=["post"], url_path="update_dataset")
    @params_valid(DataSetSensitivityUpdateSerializer, add_params=False)
    def update_dataset(self, request):
        """
        @api {post} /v3/auth/sensitivity/update_dataset/ 更新数据集的敏感度信息
        @apiName retrieve_dataset
        @apiGroup Sensitivity
        @apiParam {String} data_set_type 数据集类型
        @apiParam {String} data_set_id 数据集ID
        @apiParam {String} tag_method 标记范式
        @apiParam {String} sensitivity 敏感度
        @apiSuccessExample {json} 样例
            True
        """
        data_set_type = request.cleaned_params["data_set_type"]
        data_set_id = request.cleaned_params["data_set_id"]
        tag_method = request.cleaned_params["tag_method"]
        sensitivity = request.cleaned_params.get("sensitivity", None)

        action_id = f"{data_set_type}.manage_auth"
        check_perm(action_id, data_set_id)

        if data_set_type == "result_table":
            if tag_method == "user":
                MetaApi.update_result_table(
                    {"result_table_id": data_set_id, "sensitivity": sensitivity}, raise_exception=True
                )
                AuthDatasetSensitivity.tagged(data_set_type, data_set_id, sensitivity, get_request_username())
            elif tag_method == "default":
                AuthDatasetSensitivity.untagged(data_set_type, data_set_id)

        if data_set_type == "raw_data":
            AccessApi.update_raw_data({"raw_data_id": data_set_id, "sensitivity": sensitivity}, raise_exception=True)

        return Response(True)
