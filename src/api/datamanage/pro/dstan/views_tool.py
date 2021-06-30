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


from datamanage.pro.dstan.constants import DataSet, StandardType
from datamanage.pro.dstan.managers import StandardManager
from datamanage.pro.dstan.models import DmStandardConfig
from datamanage.pro.dstan.serializers import BindDatasetStandardSerializer
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class ToolViewSet(APIViewSet):
    @list_route(methods=["post"])
    @params_valid(BindDatasetStandardSerializer, add_params=False)
    def bind_dataset_standard(self, request):
        """
        @api {post} /v3/datamanage/dstan/tools/bind_dataset_standard/ 给数据集绑定标准
        @apiVersion 1.0.0
        @apiGroup Dstan/Tools
        @apiName tools_bind_dataset_standard
        @apiParam {String} data_set_type 数据集类型，result_table
        @apiParam {String} data_set_id   数据集ID，result_table_id
        @apiParam {Integer} standard_id  数据标准ID
        @apiParam {String} standard_type  数据标准类型 ，目前可选的有 detaildata（明细数据）/indicator（指标数据）
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {

                },
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        manager = StandardManager()
        params = request.cleaned_params

        dataset = DataSet(params["data_set_type"], params["data_set_id"])
        standard = DmStandardConfig.objects.get(id=params["standard_id"])
        standard_type = StandardType(params["standard_type"])

        manager.bind_dataset_standard_directly(dataset, standard, standard_type)
        return Response("ok")
