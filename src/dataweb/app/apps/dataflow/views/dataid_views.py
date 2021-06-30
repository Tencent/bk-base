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

from rest_framework import serializers
from rest_framework.response import Response

from apps import exceptions
from apps import forms as data_forms
from apps.common.views import detail_route
from apps.dataflow.handlers.dataid import DataId
from apps.dataflow.permissions import DataIdPermissions
from apps.dataflow.validator import DataCountForm
from apps.generic import APIViewSet


class DataIdSet(APIViewSet):
    """
    DataId 操作集合
    """

    serializer_class = serializers.Serializer
    lookup_value_regex = r"\d+"
    lookup_field = "data_id"

    permission_classes = (DataIdPermissions,)

    @detail_route(methods=["get"], url_path="check")
    def check(self, request, data_id=None):
        return Response(True)

    @detail_route(methods=["get"], url_path="list_data_count_by_time")
    def list_data_count_by_time(self, request, data_id=None):
        """
        @api {get} /dataids/:raw_data_id/list_data_count_by_time  获取接入数据详情数据量
        @apiName list_data_count_by_time
        @apiGroup DataId
        @apiParam {String} frequency 数据量频率（'3m'：每三分钟，'1d'：一天）
        @apiParam {String} start_time 开始时间
        @apiParam {String} end_time 结束时间
        @apiParamExample {json} 日数据量 样例
            {
                'frequency': '1d'
                'start_time': '2017-09-21 00:00:00'
                'end_time': '2017-09-28 00:00:00'
            }
        @apiParamExample {json} 三分钟数据量 样例
            {
                'frequency': '3m'
                'start_time': '2017-09-27 21:17:00'
                'end_time': '2017-09-28 21:17:00'
            }
        @apiSuccessExample {json} 成功返回:
            {
                "message": "",
                "code": "00",
                "data": {
                    "cnt": [
                        0,
                        0,
                        0,
                        0,
                        1677,
                        1418,
                        1439,
                        347
                    ],
                    "time": [
                        "2017-09-21 08:00",
                        "2017-09-22 08:00",
                        "2017-09-23 08:00",
                        "2017-09-24 08:00",
                        "2017-09-25 08:00",
                        "2017-09-26 08:00",
                        "2017-09-27 08:00",
                        "2017-09-28 08:00"
                    ]
                },
                "result": true
            }
        """

        class DataIDDataCountForm(DataCountForm):
            storage_type = data_forms.StorageField(required=False)

        o_dataid = DataId(data_id=data_id)
        _params = self.valid(DataIDDataCountForm)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]
        try:
            data = o_dataid.list_data_count_by_time(start_timestamp, end_timestamp, frequency=frequency)
        except exceptions.ApiResultError:
            data = {"cnt": [], "time": []}
        return Response(data)
