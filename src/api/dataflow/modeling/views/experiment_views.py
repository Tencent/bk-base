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

from dataflow.modeling.job.model_experiment_controller import ModelExperimentController
from dataflow.modeling.views.base_views import BaseViewSet


class ModelExperimentViewSet(BaseViewSet):
    def list(self, request, model_id):
        """
        @api {post}  /dataflow/modeling/experiments/:model_id/ 模型实验的列表
        @apiName models/get_experiment_list
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "model_id": 'test_model_id'
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                 {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            'experiment_id':1,
                            'experiment_name':'name1',
                            'experiment_alias':'alias1'

                        },
                    ]
                    "result": true
                }
        """
        controller = ModelExperimentController(model_id)
        return self.return_ok(data=controller.get_experiment_list())
