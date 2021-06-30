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
from common.django_utils import DataResponse
from rest_framework.response import Response

from dataflow.modeling.basic_model.basic_model_serializer import (
    BasicModelSerializer,
    CreateBasicModelSerializer,
    UpdateBasicModelSerializer,
)
from dataflow.modeling.basic_model.model_controller import ModelingModelController, ModelingModelStorageController
from dataflow.modeling.exceptions.comp_exceptions import DEFAULT_MODELING_ERR, DropModelError
from dataflow.modeling.views.base_views import BaseViewSet
from dataflow.shared.permission import require_username


class BasicModelViewSet(BaseViewSet):
    @params_valid(serializer=CreateBasicModelSerializer, add_params=False)
    def create(self, request):
        """
        @api {post}  /dataflow/modeling/model 创建模型
        @apiName create model
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "status": "developing",
                "sensitivity": "private",
                "model_alias": "591_pca_model",
                "path": "hdfs://xxxx/591_pca_model",
                "active": 0,
                "model_framework": "spark",
                "train_mode": "supervised",
                "modified_by": "xxxx",
                "created_by": "xxx",
                "algorithm_name": "pca",
                "model_type": "process",
                "project_id": 13105,
                "model_name": "591_pca_model",
                "storage_cluster_config_id": 24
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "status": "developing",
                    "sensitivity": "private",
                    "model_alias": "591_pca_model",
                    "path": "hdfs://xxxx/591_pca_model",
                    "active": 0,
                    "model_framework": "spark",
                    "train_mode": "supervised",
                    "modified_by": "xxxx",
                    "created_by": "xxxx",
                    "algorithm_name": "pca",
                    "model_type": "process",
                    "project_id": 13105,
                    "model_name": "591_pca_model",
                    "storage_cluster_config_id": 24
                }
        """
        args = request.data
        params = {}
        if "expires" not in params:
            params["expires"] = "5"
        if "active" not in params:
            params["active"] = 1
        if "priority" not in params:
            params["priority"] = 0
        if "generate_type" not in params:
            params["generate_type"] = "user"
        params["physical_table_name"] = args["model_name"]
        params["storage_config"] = {"path": args["path"]}
        params["storage_cluster_config_id"] = args["storage_cluster_config_id"]
        model_storage_controller = ModelingModelStorageController(
            params["expires"],
            params["active"],
            params["priority"],
            params["generate_type"],
            params["physical_table_name"],
            params["storage_config"],
            params["storage_cluster_config_id"],
            args["created_by"],
            args["modified_by"],
        )
        model_storage_controller.create()
        model_args = {
            "status": args["status"],
            "sensitivity": args["sensitivity"],
            "model_alias": args["model_alias"],
            "active": args["active"],
            "model_framework": args["model_framework"],
            "train_mode": args["train_mode"],
            "modified_by": args["modified_by"],
            "created_by": args["created_by"],
            "algorithm_name": args["algorithm_name"],
            "model_type": args["model_type"],
            "project_id": args["project_id"],
            "model_name": args["model_name"],
            "model_storage_id": model_storage_controller.id,
        }
        model_controller = ModelingModelController(model_args["model_name"], args["created_by"])
        model_controller.create(model_args)
        return Response(args)

    """
    @list_route(methods=['post'], url_path='update')
    def update_model(self, request):
        args = request.data
        model_name = args['model_name']
        MLSqlModelInfo.objects.filter(model_name=model_name).update(status=args['status'], active=args['active'])
        return Response()
    """

    @params_valid(serializer=UpdateBasicModelSerializer, add_params=False)
    def update(self, request, pk):
        """
        @api {post}  /dataflow/modeling/model 更新模型
        @apiName update model
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "status": "trained",
                "model_name": "591_pca_model",
                "active": 1
            }
        """
        args = request.data
        model = ModelingModelController(pk, args["bk_username"])
        model.update(status=args["status"], active=args["active"])
        return Response()

    def retrieve(self, request, pk):
        """
        @api {post}  /dataflow/modeling/model/:model_name/ 获取模型
        @apiName retrieve model
        @apiGroup modeling
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "id": 4,
                    "modified_at": "2020-05-20 20:30:25",
                    "model_name": "591_pca_model",
                    "model_alias": "591_pca_model",
                    "model_framework": "spark",
                    "model_type": "process",
                    "project_id": 13105,
                    "algorithm_name": "pca",
                    "model_storage_id": 10,
                    "train_mode": "supervised",
                    "sensitivity": "private",
                    "active": 1,
                    "status": "developing",
                    "input_standard_config": "{}",
                    "output_standard_config": "{}",
                    "created_by": "xxxx",
                    "create_at": "2020-05-20 20:30:25",
                    "modified_by": "xxxxx",
                    "description": "",
                    "processing_info": null,
                    "storage": {
                        "path": "hdfs://xxxx/591_pca_model"
                    }
                }
        """
        args = request.data
        other_args = {}
        if "active" in args:
            other_args["active"] = args["active"]
        if "status" in args:
            other_args["status"] = args["status"]

        model = ModelingModelController(pk)
        return Response(model.retrieve(**other_args))

    @require_username
    def destroy(self, request, pk):
        """
        @api {post}  /dataflow/modeling/model/:mdname/ 删除模型
        @apiName delete model
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "model_name": "591_pca_model",
                "active": 1
            }
        """
        args = request.data
        active = 1
        if "active" in args:
            active = args["active"]
        model = ModelingModelController(pk, args["bk_username"])
        try:
            return Response(model.destroy(active))
        except DropModelError as e:
            return DataResponse(result=False, code=e.code, message=e.message, data=e.message)
        except Exception as e:
            err_msg = "删除失败：%s" % e
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message=err_msg, data=err_msg)

    @params_valid(serializer=BasicModelSerializer)
    def list(self, request, params):
        """
        @api {get}  /dataflow/modeling/basic_models/ 获取所有模型信息 或者 获取指定模型信息
        @apiName modeling/basic_model/
        @apiGroup modeling
        @apiParam {String} bk_username 可选
        @apiParam {String} model_name 可选，支持多个, 注： bk_username 和 model_name 必须要有一个，而且仅可以存在一个
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        "flow_id": 1,
                        "created_at": "2018-10-13 21:57:14",
                        "updated_at": "2018-10-23 17:41:17",
                        "active": true,
                        "flow_name": "flow01",
                        "project_id": 1,
                        "status": "no-start",
                        "is_locked": 0,
                        "latest_version": "V2018102317411792358",
                        "bk_app_code": "bk_data",
                        "created_by": null,
                        "locked_by": null,
                        "locked_at": null,
                        "updated_by": "admin",
                        "description": ""
                    },
                    ...
                ]
                或者
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "model_name": xxx,
                            "algorithm_name"： "pca",
                            "status": "unreleased/released",
                            "created_by": "admin",
                            "created_at": "2020-10-01 01:01:01"
                        }
                    ],
                    "result": true
                }

        """
        bk_username = params.get("bk_username", None)
        model_name = params.get("model_name", [])
        result_models = ModelingModelController.fetch_list(bk_username, model_name)
        return Response(result_models)
