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

from common.decorators import params_valid

from dataflow.modeling.algorithm.algorithm_controller import AlgorithmController
from dataflow.modeling.model.model_serializer import GetAlgorithmListSerializer
from dataflow.modeling.views.base_views import BaseViewSet
from dataflow.shared.permission import require_username


class AlgorithmViewSet(BaseViewSet):

    lookup_field = "algorithm_name"

    @require_username
    @params_valid(serializer=GetAlgorithmListSerializer)
    def list(self, request, params):
        """
        @api {get} /dataflow/modeling/algorithms/获取所有算法信息
        @apiName models/get_algorithm_list/
        @apiGroup modeling
        @apiParam {string} framework
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                            'spark_mllib':
                            {
                                '特征转换':
                                [
                                    {
                                        "input_params": [
                                            {
                                                "default_value": "label",
                                                "field_type": "double",
                                                "field_alias": "标注",
                                                "sample_value": null,
                                                "value": null,
                                                "allowed_values": null,
                                                "real_name": "label_col",
                                                "field_name": "label",
                                                "field_index": 1
                                            }
                                        ],
                                        "args_params": [
                                            {
                                                "default_value": "2",
                                                "field_type": "int",
                                                "field_alias": "聚合深度",
                                                "sample_value": null,
                                                "value": null,
                                                "allowed_values": null,
                                                "is_advanced": false,
                                                "real_name": "aggregation_depth",
                                                "field_name": "aggregation_depth",
                                                "field_index": 0
                                            }
                                        ],
                                        "output_params": [
                                            {
                                                "default_value": "prediction",
                                                "field_type": "double",
                                                "field_alias": "预测列",
                                                "sample_value": null,
                                                "value": null,
                                                "allowed_values": null,
                                                "real_name": "prediction_col",
                                                "field_name": "prediction_col",
                                                "field_index": 0
                                            }
                                        ],
                                        "description": "aft_survival_regression",
                                        "algorithm_name": "spark_aft_survival_regression",
                                        "sql": "xxx",
                                        "algorithm_alias": "aft_survival_regression"
                                    }
                                ]
                            }
                        } ,
                "result": true
            }
        """
        framework = params.get("framework", None)
        result = AlgorithmController().get_algorithm_list(framework=framework)
        return self.return_ok(result)
