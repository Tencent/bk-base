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
from common.exceptions import ApiRequestError
from common.views import APIModelViewSet

from dataflow.flow.handlers.modeling_controller import ModelingController
from dataflow.flow.serializer.serializers import ExecuteSqlSerializer, ModelingJobSerializer
from dataflow.modeling.exceptions.comp_exceptions import DEFAULT_MODELING_ERR, DEFAULT_SUCCESS_CODE, SQLSubmitError
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.permission import require_username


class ModelViewSet(APIModelViewSet):
    @list_route(methods=["post"], url_path="start_modeling")
    @params_valid(serializer=ExecuteSqlSerializer, add_params=False)
    @require_username
    def start_modeling(self, request):
        """
        @api {post} /dataflow/flow/modeling/start_modeling 执行sql
        @apiName execute mlsql
        @apiGroup Flow
        @apiParam {string} sql_list
        @apiParam {int} project_id
        @apiParam {string} type
        @apiParam {string} bk_username
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "task_id": 12770
                },
                "result": true
            }
        """
        args = request.data
        exec_args = args.copy()
        try:
            if "sql" in args:
                sql = args["sql"]
                sql_list = [sql]
                exec_args["sql_list"] = sql_list
            return ModelingController.start(exec_args)
        except SQLSubmitError as e:
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="stop_modeling")
    @params_valid(serializer=ModelingJobSerializer, add_params=False)
    def stop_modeling(self, request):
        """
        @api {get} /dataflow/flow/modeling/stop_modeling 终止job
        @apiName kill_mlsql
        @apiGroup Flow
        @apiParam {int} task_id
        @apiParam {string} bk_username
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "task_id": 12770
                },
                "result": true
            }
        """
        args = request.data
        task_id = args["task_id"]
        controller = ModelingController(task_id)
        return controller.kill()

    @list_route(methods=["get"], url_path="sync_status")
    @params_valid(serializer=ModelingJobSerializer, add_params=False)
    def sync_status(self, request):
        """
        @api {get} /dataflow/flow/modeling/sync_status 检查job的状态
        @apiName sync_status
        @apiGroup Flow
        @apiParam {int} task_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "status": "finished",
                    "message": null
                },
                "result": true
            }
        """
        args = request.GET
        try:
            task_id = args["task_id"]
            controller = ModelingController(task_id)
            check_res = controller.check()
            clear_res = controller.clear()
            if not clear_res.result:
                return DataResponse(
                    result=clear_res.result,
                    code=clear_res.code,
                    message=clear_res.message,
                    data=clear_res.data,
                )
            return DataResponse(
                result=check_res.result,
                code=check_res.code,
                message=check_res.message,
                data=check_res.data,
            )
        except Exception as e:
            logger.error("sync job status error:%s" % e)
            return DataResponse(
                result=False,
                code=DEFAULT_MODELING_ERR,
                message="check job error:{}".format(e),
            )

    @list_route(methods=["post"], url_path="clear")
    @params_valid(serializer=ModelingJobSerializer, add_params=False)
    def clear(self, request):
        """
        @api {get} /dataflow/flow/modeling/clear 检查job的状态
        @apiName clear_job
        @apiGroup Flow
        @apiParam {int} task_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "status": "finished",
                    "message": null
                },
                "result": true
            }
        """
        args = request.GET
        try:
            task_id = args["task_id"]
            controller = ModelingController(task_id)
            res = controller.clear()
            return DataResponse(
                result=res.is_success(),
                code=res.code,
                message=res.message,
                data=res.data,
            )
        except ApiRequestError as e:
            return DataResponse(result=False, code=e.code, message="clear job error:%s" % e.message)
        except Exception as e:
            return DataResponse(
                result=False,
                code=DEFAULT_MODELING_ERR,
                message="check job error:{}".format(e),
            )

    @list_route(methods=["post"], url_path="parse_modeling")
    def parse_modeling(self, request):
        """
        @api {post} /dataflow/flow/model/parse 解析sql获取输入与输出表
        @apiName execute mlsql
        @apiGroup Flow
        @apiParam {string} sql_list
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "read": [
                        "591_string_indexer_result_60",
                        "591_test_mock_mix_1"
                    ],
                    "write": []
                },
                "result": true
            }
        """
        args = request.data
        try:
            if "sql" in args:
                sql = args["sql"]
                sql_list = [sql]
            else:
                sql_list = args["sql_list"]
            return ModelingController.parse(sql_list)
        except Exception as e:
            logger.error("parse mlsql error:%s" % e)
            return DataResponse(
                result=False,
                code=DEFAULT_MODELING_ERR,
                message="parse sql error:{}".format(e),
            )

    @list_route(methods=["get"], url_path="get_task_id")
    def get_task_id(self, request):
        """
        @api {get} /dataflow/flow/modeling/get_task_id 给探索获取当前执行的task_id
        @apiName get_task_id
        @apiGroup Flow
        @apiParam {int} notebook_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "status": "finished",
                    "task_id": 365
                },
                "result": true
            }
        """
        args = request.GET
        notebook_id = args["notebook_id"]
        result = ModelingController.get_task_id(notebook_id)
        return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, data=result)
