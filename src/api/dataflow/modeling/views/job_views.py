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


from common.decorators import detail_route, list_route, params_valid
from common.django_utils import DataResponse
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.modeling.exceptions.comp_exceptions import (
    DEFAULT_MODELING_ERR,
    DEFAULT_SUCCESS_CODE,
    CheckTaskError,
    ClearJobError,
    CreateJobError,
    DropModelNotExistsError,
    DropTableNotExistsError,
    SQLSubmitError,
    StartJobError,
)
from dataflow.modeling.job.job_controller import ModelingJobController
from dataflow.modeling.job.job_serializer import (
    CreateModelingJobSerializer,
    CreateMultiJobSerializer,
    DeleteJobSerializer,
    DeleteMultiJobSerializer,
    ExecuteSqlSerializer,
    JobStatusSerializer,
    ModelingJobSerializer,
    ParseJobResultSerializer,
    StartModelingJobSerializer,
    StartMultiJobSerializer,
    StopMultiJobSerializer,
    SyncStatusSerializer,
)
from dataflow.modeling.job.tensorflow.tensorflow_job_driver import TensorFlowJobDriver
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.permission import require_username


class JobViewSet(APIViewSet):
    lookup_field = "job_id"

    @params_valid(serializer=CreateModelingJobSerializer)
    @require_username
    def create(self, request, params):
        """
        @api {post} /dataflow/modeling/jobs/ 创建TensorFlow Job
        @apiName create_job
        @apiGroup modeling
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} cluster_group 集群组
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
        {
            "processing_id": "xxx",
            "bk_username": "xxx",
            "cluster_group": "gem",
            "project_id": 13105
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data": {
                        "job_id": "591_xxx"
                },
                "result": true
            }
        """
        ModelingJobController.create_job(params)
        return Response({"job_id": params["processing_id"]})

    @params_valid(serializer=CreateModelingJobSerializer)
    def update(self, request, job_id, params):
        """
        @api {post} /dataflow/modeling/jobs/:job_id 更新TensorFlow Job
        @apiName update_job
        @apiGroup modeling
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} cluster_group 集群组
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
        {
            "processing_id": "xxx",
            "bk_username": "xxx",
            "cluster_group": "gem",
            "project_id": 13105
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data": {
                        "job_id": "591_xxx"
                },
                "result": true
            }
        """
        handler = ModelingJobController(job_id)
        handler.update_job(params)
        return Response({"job_id": params["processing_id"]})

    @detail_route(methods=["post"], url_path="start")
    @params_valid(serializer=StartModelingJobSerializer)
    @require_username
    def start(self, request, job_id, params):
        """
        @api {post}  /dataflow/modeling/jobs/:job_id/start 启动Job
        @apiName start_job
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xxxxxx",
                "is_restart": False
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                True
        """
        try:
            handler = ModelingJobController(job_id)
            res = handler.submit_job(params)
            return Response(res)
        except Exception as e:
            logger.exception(e)
            logger.error("start job error:%s" % e)
            raise StartJobError(message_kv={"content": "{}".format(e)})

    @detail_route(methods=["post"], url_path="stop")
    @require_username
    def stop(self, request, job_id):
        """
        @api {post}  /dataflow/modeling/jobs/:job_id/stop 停止Job
        @apiName stop_job
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xxxxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                True
        """
        handler = ModelingJobController(job_id)
        res = handler.stop_job()
        return Response(res)

    @require_username
    @params_valid(serializer=DeleteJobSerializer)
    def destroy(self, request, job_id, params):
        """
        @api {delete} /dataflow/modeling/jobs/:job_id 删除Job
        @apiName delete_job
        @apiGroup modeling
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {"job_id": job_id},
                    "result": true
                }
        """
        handler = ModelingJobController(job_id)
        handler.delete_job(params)
        return Response({"job_id": job_id})

    @detail_route(methods=["get"], url_path="status")
    @params_valid(serializer=JobStatusSerializer)
    def get_scheduler_status(self, request, job_id, params):
        """
        @api {post}  /dataflow/modeling/jobs/:job_id/status 获取jobnavi上调度任务的状态
        @apiName get_scheduler_job_status
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "job_id": "591_xxxxx",
                "start_time": "2020-09-01 00:00:00",
                "end_time": "2020-09-02 00:00:00"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "execute_id": 12544,
                        "schedule_time": 1539778425000,
                        "status": "running",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    },
                    {
                        "execute_id": 14425,
                        "schedule_time": 1539778425000,
                        "status": "finished",
                        "created_at": 1539778425000,
                        "updated_at": 1539778425000
                    }
                ],
                "result": true
            }
        """
        res = TensorFlowJobDriver.get_scheduler_status(job_id, params["start_time"], params["end_time"])
        return Response(res)

    @list_route(methods=["get"], url_path="sync_status_old")
    def sync_status_old(self, request):
        """
        @api {post}  /dataflow/modeling/jobs/sync_status 获取job状态(数据探索应用)
        @apiName start job
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "task_id": 12324
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                12325
        """
        try:
            args = request.GET
            job_id = args["job_id"]
            handler = ModelingJobController(job_id)
            check_result = handler.check_job()
            return DataResponse(
                result=check_result.result,
                code=check_result.code,
                message=check_result.message,
                data=check_result.data,
            )
        except Exception as e:
            logger.error("check job error:%s" % e)
            raise CheckTaskError(message_kv={"content": "{}".format(e)})

    @list_route(methods=["post"], url_path="clear")
    def clear(self, request):
        """
        @api {post}  /dataflow/modeling/jobs/clear 清除job执行的相关信息(数据探索应用)
        @apiName clear_job
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "task_id": 12324
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                12325
        """
        try:
            args = request.data
            job_id = args["job_id"]
            status = args["status"]
            handler = ModelingJobController(job_id)
            return handler.clear_job(status)
        except Exception as e:
            logger.error("clear job error:%s" % e)
            raise ClearJobError(message_kv={"content": "{}".format(e)})

    @list_route(methods=["post"], url_path="multi_create_jobs")
    @require_username
    @params_valid(serializer=CreateMultiJobSerializer)
    def multi_create_jobs(self, request, params):
        """
        @api {post}  /dataflow/modeling/jobs/multi_create_jobs 根据heads与tails生成（多个）job(模型应用节点使用)
        @apiName create_multi_jobs
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "project_id": xxxx,
                "processing_id": "591_xxxx",
                "jobserver_config": {
                    "geog_area_code": "xxx",
                    "cluster_id": "xxx"
                }
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            591_xxxxxx
        """
        try:
            jobserver_config = params["jobserver_config"]
            processing_id = params["processing_id"]
            cluster_group = params["cluster_group"]
            cluster_name = params["cluster_name"]
            job_info = ModelingJobController.create_multi_jobs(
                jobserver_config, processing_id, cluster_group, cluster_name
            )
            return Response(job_info)
        except CreateJobError as e:
            logger.error("create job error:%s" % e)
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_update_jobs")
    @require_username
    @params_valid(serializer=CreateMultiJobSerializer)
    def multi_update_jobs(self, request, params):
        """
        @api {post}  /dataflow/modeling/jobs/multi_update_jobs 根据heads与tails更新（多个）job的信息(模型应用节点使用)
        @apiName update_multi_jobs
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "project_id": xxxx,
                "processing_id": "591_xxxx",
                "jobserver_config": {
                    "geog_area_code": "xxx",
                    "cluster_id": "xxx"
                }
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            591_xxxxxx
        """
        try:
            jobserver_config = params["jobserver_config"]
            job_id = params["job_id"]
            controller = ModelingJobController(job_id)
            job_info = controller.update_multi_jobs(jobserver_config)
            return Response(job_info)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_start_jobs")
    @require_username
    @params_valid(serializer=StartMultiJobSerializer)
    def multi_start_jobs(self, request, params):
        """
        @api {post}  /dataflow/modeling/jobs/multi_start_jobs 执行多个job(模型应用节点使用)
        @apiName start_multi_jobs
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "job_id": "13105_xxxxxx",
                'is_restart': false
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            12345
        """
        try:
            job_id = params["job_id"]
            is_restart = params["is_restart"]
            controller = ModelingJobController(job_id)
            scheduler_id = controller.start_multi_jobs(is_restart)
            return Response(scheduler_id)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_stop_jobs")
    @require_username
    @params_valid(serializer=StopMultiJobSerializer)
    def multi_stop_jobs(self, request, params):
        """
        @api {post}  /dataflow/modeling/jobs/multi_stop_jobs 停止多个job组成的workflow的执行(模型应用节点使用)
        @apiName stop_multi_jobs
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "job_id": "13105_67bd38ca85ff4255a58a738c31096e8f"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
        """
        try:
            job_id = params["job_id"]
            controller = ModelingJobController(job_id)
            controller.multi_stop_jobs()
            return Response()
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["post"], url_path="multi_delete_jobs")
    @require_username
    @params_valid(serializer=DeleteMultiJobSerializer)
    def multi_delete_jobs(self, request, params):
        """
        @api {post}  /dataflow/modeling/jobs/multi_delete_jobs 删除多个job信息
        @apiName delete_multi_jobs
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "job_id": "13105_67bd38ca85ff4255a58a738c31096e8f"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            12345
        """
        try:
            job_id = params["job_id"]
            controller = ModelingJobController(job_id)
            controller.multi_delete_jobs()
            return Response()
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["get"], url_path="parse_job_outputs")
    @require_username
    @params_valid(serializer=ParseJobResultSerializer)
    def parse_job_output(self, request, params):
        """
        @api {get} /dataflow/flow/modeling/jobs/parse_job_output 获取某个任务内所有的产出物(数据探索使用)
        @apiName get_modeling_result
        @apiGroup modeling
        @apiParamExample {json} 参数样例:
            {
                "task_id" : 123,
            }
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    'basic_model': {'add':['591_model_xxx_1'], 'remove':['591_model_xxx_2']},
                    'query_set': {'add':['591_queryset_xxx_1'], 'remove':['591_queryset_xxx_2']}
                },
                "result": true
            }
        """
        try:
            controller = ModelingJobController(task_id=params["task_id"])
            return Response(controller.get_mlsql_result())
        except Exception as e:
            logger.exception(e)
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))

    @list_route(methods=["get"], url_path="sync_status")
    @params_valid(serializer=SyncStatusSerializer)
    def sync_status(self, request, params):
        """
        @api {get} /dataflow/modeling/jobs/sync_status 检查job的状态（数据探索使用）
        @apiName sync_status
        @apiGroup modeling
        @apiParam {int} task_id
        @apiSuccessExample {json} 成功返回
            {
                "errors": null,
                "message": "xxxxxx",
                "code": "1586001",
                "data": {
                "status": "failed",
                "stages": [
                    {
                        "stage_status": "failure",
                        "time_taken": 1,
                        "stage_type": "parse",
                        "description": "SQL解析",
                        "stage_seq": 1
                    },
                    {
                        "stage_status": "pending",
                        "time_taken": 0,
                        "stage_type": "submit",
                        "description": "提交任务",
                        "stage_seq": 2
                    },
                    {
                        "stage_status": "pending",
                        "time_taken": 0,
                        "stage_type": "execute",
                        "description": "任务执行",
                        "stage_seq": 3
                    }
                ],
                "submit": false
            },
            "result": false
        }
        """
        args = request.GET
        try:
            task_id = args["task_id"]
            controller = ModelingJobController(task_id=task_id)
            check_res = controller.check_job()
            clear_res = controller.clear_job()
            if not clear_res.result:
                return DataResponse(
                    result=clear_res.result,
                    code=clear_res.code,
                    message=clear_res.message,
                    data=clear_res.data,
                    errors=clear_res.errors,
                )
            return DataResponse(
                result=check_res.result,
                code=check_res.code,
                message=check_res.message,
                data=check_res.data,
                errors=check_res.errors,
            )
        except Exception as e:
            logger.error("sync job status error:%s" % e)
            return DataResponse(
                result=False,
                code=DEFAULT_MODELING_ERR,
                message="check job error:{}".format(e),
            )

    @list_route(methods=["post"], url_path="parse_modeling")
    def parse_modeling(self, request):
        """
        @api {post} /dataflow/modeling/jobs/parse_modeling 解析sql获取输入与输出表(数据探索使用)
        @apiName parse_modeling_inputs_outputs
        @apiGroup modeling
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
            return ModelingJobController.parse(sql_list)
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
        @api {get} /dataflow/modeling/jobs/get_task_id 给探索获取当前执行的task_id（数据探索使用）
        @apiName get_task_id
        @apiGroup modeling
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
        result = ModelingJobController.get_task_id(notebook_id)
        return DataResponse(result=True, code=DEFAULT_SUCCESS_CODE, data=result)

    @list_route(methods=["post"], url_path="stop_modeling")
    @params_valid(serializer=ModelingJobSerializer, add_params=False)
    def stop_modeling(self, request):
        """
        @api {get} /dataflow/modeling/jobs/stop_modeling 终止job(模型应用节点使用)
        @apiName stop_modeling_jobs
        @apiGroup modeling
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
        controller = ModelingJobController(task_id=task_id)
        return controller.stop_job()

    @list_route(methods=["post"], url_path="start_modeling")
    @params_valid(serializer=ExecuteSqlSerializer, add_params=False)
    @require_username
    def start_modeling(self, request):
        """
        此接口为启动整个执行的流程，包括创建processing，job以及提交任务
        @api {post} /dataflow/modeling/jobs/start_modeling 执行sql(数据探索使用)
        @apiName start_modeling
        @apiGroup modeling
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
            return ModelingJobController.start(exec_args)
        except (DropModelNotExistsError, DropTableNotExistsError) as e:
            return DataResponse(result=False, code=e.code, message=e.message, errors=e.errors)
        except SQLSubmitError as e:
            return DataResponse(result=False, code=e.code, message=e.message)
        except Exception as e:
            return DataResponse(result=False, code=DEFAULT_MODELING_ERR, message="{}".format(e))
