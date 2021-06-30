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

from dataflow.uc.processing.processing_controller import ProcessingController
from dataflow.uc.processing.processing_serializers import CreateProcessing, UpdateProcessing
from dataflow.uc.views.base_views import BaseViewSet


class ProcessingViewSet(BaseViewSet):
    """
    processing reset api in uc api
    """

    lookup_field = "processing_id"

    @params_valid(serializer=CreateProcessing)
    def create(self, request, params):
        """
        @api {post} /dataflow/uc/processings/ 创建 uc processing
        @apiName create processing
        @apiGroup uc
        @apiParam {int} project_id 项目ID
        @apiParam {string} processing_id
        @apiParam {string} component_type 组件类型
        @apiParam {string} implement_type 实现方式，ex: code
        @apiParam {string} programming_language 编程语言
        @apiParam {list} tags 标签 ex: ["inland"]
        @apiParam {string} bk_username 用户名
        @apiParam {dict} data_link 数据链路
        @apiParam {dict} processor_logic 处理逻辑
        @apiParam {dict} schedule_info 调度信息
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "123_abc",
                "project_id": 123,
                "component_type": "tensorflow",
                "implement_type": "code",
                "programming_language": "python",
                "tags": ["inland"],
                "bk_username": "admin",
                "data_link": {},
                "processor_logic": {},
                "schedule_info": {}
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "processing_id": "123_abc",
                        "result_table_ids": ["123_abc"]
                    },
                    "result": true
                }
        """
        data = ProcessingController.create_processing(params)
        return self.return_ok(data)

    @params_valid(serializer=UpdateProcessing)
    def update(self, request, processing_id, params):
        """
        @api {put} /dataflow/uc/processings/:processing_id 更新 uc processing
        @apiName update processing
        @apiGroup uc
        @apiParam {int} project_id 项目ID
        @apiParam {string} component_type 组件类型
        @apiParam {string} implement_type 实现方式，ex: code
        @apiParam {string} programming_language 编程语言
        @apiParam {list} tags 标签 ex: ["inland"]
        @apiParam {string} bk_username 用户名
        @apiParam {dict} data_link 数据链路
        @apiParam {dict} processor_logic 处理逻辑
        @apiParam {dict} schedule_info 调度信息
        @apiParamExample {json} 参数样例:
            {
                "project_id": 123,
                "component_type": "tensorflow",
                "implement_type": "code",
                "programming_language": "python",
                "tags": ["inland"],
                "bk_username": "admin",
                "data_link": {},
                "processor_logic": {},
                "schedule_info": {}
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                    },
                    "result": true
                }
        """
        data = ProcessingController(processing_id).update_processing(params)
        return self.return_ok(data)
