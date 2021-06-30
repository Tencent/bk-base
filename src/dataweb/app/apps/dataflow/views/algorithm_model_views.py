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
import json

from rest_framework import serializers
from rest_framework.response import Response

from apps.api import AlgoApi
from apps.common.views import detail_route, list_route
from apps.generic import APIViewSet


class AlgorithmModelViewSet(APIViewSet):
    serializer_class = serializers.Serializer
    lookup_field = "model_id"

    @detail_route(methods=["get"], url_path="list_model_versions")
    def list_model_versions(self, request, model_id):
        """
        @api {get} /algorithm_models/:model_id/list_model_versions/ 获取算法模型版本列表
        @apiName list_algorithm_version
        @apiGroup AlgorithmModel
        @apiSuccessExample {json} 成功返回
            [
                {
                    "version_description": "py 测试用例",
                    "model_status": "complete",
                    "model_version_id": 1,
                    "created_by": "py"
                }
            ]
        """
        api_params = {"model_id": model_id, "model_status": "complete"}
        return Response(AlgoApi.list_model_versions(api_params))

    @list_route(methods=["get"], url_path="list_sample")
    def list_sample(self, request):
        """
        @api {get} /algorithm_models/list_sample/
        @apiName list_sample
        @apiGroup AlgorithmModel
        @apiSuccessExample {json} 成功返回
            [
                {
                    "version_description": "py 测试用例",
                    "model_status": "complete",
                    "model_version_id": 1,
                    "created_by": "py"
                }
            ]
        """
        project_id = int(request.query_params["project_id"])

        api_params = {
            "project_id": project_id,
            "request_fields": json.dumps(
                [
                    "sample_db_zh_name",
                    "sample_db_name",
                    "description",
                    "is_public",
                    "is_dynamic",
                    "sample_start_time",
                    "sample_end_time",
                    "result_table_id",
                ]
            ),
        }
        return Response(AlgoApi.list_samples(api_params))
