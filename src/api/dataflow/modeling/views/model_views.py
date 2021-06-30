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

from common.decorators import list_route, params_valid
from common.django_utils import DataResponse
from rest_framework.response import Response

from dataflow.modeling.exceptions.comp_exceptions import DEFAULT_MODELING_ERR
from dataflow.modeling.model.model_controller import ModelController
from dataflow.modeling.model.model_serializer import (
    CreateModelSerializer,
    GetProjectReleaseSerializer,
    GetReleaseResultSerializer,
    GetUpdateReleaseSerializer,
    InspectionBeforeRelease,
    UpdateModelSerializer,
)
from dataflow.modeling.processing.processing_serializer import CheckModelUpdateSerializer, UpdateModelInfoSerializer
from dataflow.modeling.views.base_views import BaseViewSet
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.permission import require_username


class ModelViewSet(BaseViewSet):

    lookup_field = "model_name"

    @params_valid(serializer=CreateModelSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/modeling/models/ 创建新的模型
        @apiName create_models
        @apiGroup modeling
        @apiParam {string} last_sql 最后一条sql
        @apiParam {int} project_id
        @apiParam {string} model_name 模型名称
        @apiParam {string} model_alias 模型中文名称
        @apiParam {boolean} is_public (true/false) 是否公开
        @apiParam {list} result_table_ids 当前笔记产生的result table
        @apiParam {string} description 描述
        @apiParam {string} experiment_name 实验名称
        @apiParamExample {json} 参数样例:
            {
              "last_sql": "create table xxx run model xxx",
              "project_id": 123,
              "model_name": "abc_model",
              "model_alias": "阿彼此",
              "is_public": true,
              "result_table_ids": ['xx', 'xxx'],
              "description": "........",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": task_id,
                "result": true
            }
        """
        task_id = ModelController.create(
            params["last_sql"],
            params["project_id"],
            params["model_name"],
            params["model_alias"],
            params["is_public"],
            params["result_table_ids"],
            params["description"],
            params["experiment_name"],
            params["evaluation_result"],
            params["notebook_id"],
        )
        return self.return_ok(task_id)

    @params_valid(serializer=UpdateModelSerializer)
    def update(self, request, model_name, params):
        """
        @api {put} /dataflow/modeling/models/:model_name/ 更新已发布的模型
        @apiName update_models
        @apiGroup modeling
        @apiParam {string} last_sql 最后一条sql
        @apiParam {int} project_id
        @apiParam {list} result_table_ids 当前笔记产生的result table
        @apiParam {string} description 描述
        @apiParam {string} experiment_name 实验名称，创建新实验的时候输入
        @apiParam {string} experiment_id 实验id, 更新已有实验的时候输入
        @apiParamExample {json} 参数样例:
            {
              "last_sql": "create table xxx run model xxx",
              "project_id": 123,
              "result_table_ids": ['xx', 'xxx'],
              "description": "........",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": task_id,
                "result": true
            }
        """
        task_id = ModelController().update(
            model_name,
            params["last_sql"],
            params["project_id"],
            params["result_table_ids"],
            params["description"],
            params["experiment_name"] if "experiment_name" in params else None,
            params["experiment_id"] if "experiment_id" in params else None,
            params["evaluation_result"] if "evaluation_result" in params else None,
        )
        return self.return_ok(task_id)

    @list_route(methods=["post"], url_path="release_inspection")
    @params_valid(serializer=InspectionBeforeRelease)
    def inspection_before_release(self, request, params):
        """
        @api {post} /dataflow/modeling/models/release_inspection/ 检验是否符合模型发布条件
        @apiName models/release_inspection/
        @apiGroup modeling
        @apiParam {string} sql 最后一条sql
        @apiParamExample {json} 参数样例:
            {
              "sql": "create table xxx run model xxx",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": "create table xxx run model xxx",
                "result": true
            }
        """
        sql = params["sql"]
        last_sql = ModelController.inspection_before_release(sql)
        return self.return_ok(last_sql)

    @list_route(methods=["get"], url_path="release_result")
    @params_valid(serializer=GetReleaseResultSerializer)
    def get_release_result(self, request, params):
        """
        @api {get} /dataflow/modeling/models/release_result/ 查看模型发布结果
        @apiName models/release_result/
        @apiGroup modeling
        @apiParam {string} task_id 创建/更新模型对应的task id
        @apiParamExample {json} 参数样例:
            {
              "task_id": 123
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "status": "failure",
                    "logs":
                        [
                            {
                                "status": "success",
                                "message": "xxxx",
                                "time": "2020-10-28 22:13:48"
                            },
                            {
                                "status": "success",
                                "message": "xxxxx",
                                "time": "2020-10-28 22:13:55"
                            },
                            {
                                "status": "failure",
                                "message": "xxxxxx",
                                "time": "2020-10-28 22:13:56"
                            }
                        ]
                    }
                "result": true
            }
        """
        result = ModelController().get_release_result(params["task_id"])
        return DataResponse(data=result["data"], message=result["message"])

    @list_route(methods=["get"], url_path="get_user_model_release")
    @require_username
    @params_valid(serializer=GetProjectReleaseSerializer)
    def get_model_release(self, request, params):
        """
        @api {get} /dataflow/modeling/models/get_user_model_release/ 获取project下已发布的所有模型
        @apiName models/get_user_model_release/
        @apiGroup modeling
        @apiParam {string} project_id
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                            {
                                "model_id": "591_bb_test_model30xx",
                                "model_config_template": {
                                    "source": {},
                                    "transform": {}
                                },
                                "model_name": "591_bb_test_model30xx",
                                "input_info": {
                                    "predict_args": [
                                        {
                                            "default_value": "error",
                                            "field_type": "string",
                                            "field_alias": "u5904u7406u65e0u6548u6570u636e",
                                            "sample_value": null,
                                            "value": null,
                                            "allowed_values": [
                                                "skip",
                                                "error",
                                                "keep"
                                            ],
                                            "real_name": "handle_invalid",
                                            "field_name": "handle_invalid",
                                            "field_index": 0
                                        }
                                    ],
                                    "feature_columns": [
                                        {
                                            "field_type": "string",
                                            "field_alias": "",
                                            "field_name": "char_1"
                                        }
                                    ]
                                },
                                "output_info": [
                                    [
                                        {
                                            "origin": [
                                                ""
                                            ],
                                            "field": "indexed",
                                            "type": "double"
                                        },
                                        {
                                            "origin": [
                                                "double_1"
                                            ],
                                            "field": "double_1",
                                            "type": "double"
                                        },
                                        {
                                            "origin": [
                                                "double_2"
                                            ],
                                            "field": "double_2",
                                            "type": "double"
                                        }
                                    ]
                                ]
                            }
                        ] ,
                "result": true
            }
        """
        project_id = params["project_id"]
        result = ModelController().get_release_model_by_project(project_id)
        return self.return_ok(result)

    @list_route(methods=["get"], url_path="get_update_model_release")
    @require_username
    @params_valid(serializer=GetUpdateReleaseSerializer)
    def get_update_model_release(self, request, params):
        """
        @api {get} /dataflow/modeling/models/get_update_model_release/ 获取project当前用户可更新的模型列表
        @apiName models/get_update_model_release/
        @apiGroup modeling
        @apiParam {string} project_id
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                            {
                                "model_id": "591_bb_test_model30xx",
                                "model_name": "591_bb_test_model30xx",
                                "model_alias": "591_bb_test_model30xx"
                            }
                        ] ,
                "result": true
            }
        """
        project_id = params["project_id"]
        result = ModelController().get_update_model_by_project(project_id)
        return self.return_ok(result)

    def retrieve(self, request, model_name):
        """
        @api {post} /dataflow/modeling/models/:model_name/ 获取模型详情
        @apiName retrieve_model
        @apiGroup modeling
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                            {
                                "model_type": "datalab",
                                "model_id": "xxx",
                                "train_mode": "xxx",
                                "model_name": "xxxx",
                                "sensitivity": "public"
                            }
                        ],
                "result": true
            }
        """
        model = ModelController().get_release_model_by_name(model_name)
        return self.return_ok(model)

    @list_route(methods=["get"], url_path="check_model_update")
    @params_valid(serializer=CheckModelUpdateSerializer)
    def check_model_update(self, request, params):
        """
        @api {get}  /dataflow/modeling/models/check_model_update/ 检查模型是否有更新
        @apiName models/check_model_update
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "xxxxx",
                "model_id": xxxx
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                 {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": true,
                    "result": true
                }
        """
        try:
            controller = ModelController()
            result = controller.check_model_update(params["model_id"], params["processing_id"])
            return Response(result)
        except Exception as e:
            logger.error(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="update_model_info")
    @params_valid(serializer=UpdateModelInfoSerializer)
    def update_model_info(self, request, params):
        """
        @api {post}  /dataflow/modeling/models/update_model_info/ 更新模型配置
        @apiName models/update_model_info
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "processing_id": "xxxxx",
                "model_id": xxxx
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                 {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": true,
                    "result": true
                }
        """
        try:
            controller = ModelController()
            result = controller.update_model_info(params["model_id"], params["processing_id"])
            return Response(result)
        except Exception as e:
            logger.error(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))
