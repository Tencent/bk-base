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

import json
import time

from common.base_utils import CustomJSONEncoder, model_to_dict
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

import dataflow.batch.settings as settings
import dataflow.shared.handlers.processing_udf_info as processing_udf_info
import dataflow.shared.handlers.processing_udf_job as processing_udf_job
from dataflow.batch.api.api_helper import DatabusHelper
from dataflow.batch.custom_calculates.custom_calculate_driver import get_job_execute_list
from dataflow.batch.exceptions.comp_execptions import (
    BatchIllegalArgumentError,
    BatchIllegalStatusError,
    BatchInfoAlreadyExistsException,
    JobInfoNotFoundException,
)
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.handlers.storage_hdfs_export_retry import StorageHdfsExportRetryHander
from dataflow.batch.job import job_driver
from dataflow.batch.periodic.jobs_v2_driver import JobsV2Driver
from dataflow.batch.processings import processings_driver
from dataflow.batch.serializer.serializers import (
    JobExecuteSerializer,
    JobRetrySerializer,
    JobSerializer,
    JobsUpdateEngineConfSerializer,
    JobsUpdateNodeLabelSerializer,
)
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper


class JobViewSet(APIViewSet):
    lookup_field = "job_id"
    lookup_value_regex = r"\w+"

    @params_valid(serializer=JobSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/jobs/ 创建v2作业
        @apiName create_job
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} code_version 代码版本
        @apiParam {string} cluster_group 集群组
        @apiParam {string} deploy_mode 部署模式
        @apiParam {json} deploy_config 部署配置
        @apiParam {json} job_config 作业配置
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
        {
            "api_version": "v2",
            "processing_id": "xxx",
            "bk_username": "xxx",
            "code_version": "0.1.0",
            "cluster_group": "gem",
            "deploy_mode": "yarn",
            "jobserver_config": {"geog_area_code": "inland", "cluster_id": "default"},
            "deploy_config": {},
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
        """
        @api {post} /dataflow/batch/jobs/ 创建作业
        @apiName create_job
        @apiGroup Batch
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} code_version 代码版本
        @apiParam {string} cluster_group 集群组
        @apiParam {string} cluster_name 细分集群名称
        @apiParam {string} deploy_mode 部署模式
        @apiParam {string} deploy_config 部署配置
        @apiParam {json} job_config 作业配置
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
            {
              "processing_id": "xxx",
              "bk_username": "xxx",
              "code_version": "0.1.0",
              "cluster_group": "gem",
              "cluster_name": "root.bkdata.gem",
              "deploy_mode": "yarn",
              "jobserver_config": {"geog_area_code": "inland", "cluster_id": "default"}
              "deploy_config": "{executor_memory:1024m}",
              "job_config": { -- 这里先没有

              }
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
        args = request.data
        if "api_version" in args and args["api_version"] == "v2":
            JobsV2Driver.create_job(args)
            return Response({"job_id": args["processing_id"]})

        args["bk_username"] = get_request_username()
        job_id = args["processing_id"]
        args["job_id"] = job_id

        if args["job_config"] is not None and "submit_args" in args["job_config"]:
            submit_args = args["job_config"]["submit_args"]
            if "batch_type" in submit_args and submit_args["batch_type"] == "spark_python_code":
                job_info_param = job_driver.parse_code_job_info_params(args)
                if not ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(job_id):
                    try:
                        processing_type = DataProcessingHelper.get_data_processing_type(job_id)
                    except Exception as e:
                        batch_logger.info(e)
                        processing_type = "batch"
                    job_id = ProcessingJobInfoHandler.create_proc_job_info(
                        job_info_param,
                        processing_type=processing_type,
                        implement_type="code",
                    )
                    return Response({"job_id": job_id})
                else:
                    raise BatchInfoAlreadyExistsException(
                        "processing_id {} already exists in batch info table".format(job_id)
                    )

        args["job_config"] = json.dumps(
            model_to_dict(ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(job_id)),
            cls=CustomJSONEncoder,
        )
        job_driver.set_cluster_group(args)
        job_driver.set_deploy_config_engine_conf(args)
        job_id = ProcessingJobInfoHandler.create_proc_job_info(args)
        return Response({"job_id": job_id})

    @params_valid(serializer=JobSerializer)
    def update(self, request, job_id, params):
        """
        @api {put} /dataflow/batch/jobs/ 更新v2作业
        @apiName update_job
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} code_version 代码版本
        @apiParam {string} cluster_group 集群组
        @apiParam {string} deploy_mode 部署模式
        @apiParam {json} deploy_config 部署配置
        @apiParam {json} job_config 作业配置
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
        {
            "api_version": "v2",
            "processing_id": "xxx",
            "bk_username": "xxx",
            "code_version": "0.1.0",
            "cluster_group": "gem",
            "deploy_mode": "yarn",
            "jobserver_config": {"geog_area_code": "inland", "cluster_id": "default"},
            "deploy_config": {},
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
        """
        @api {put} /dataflow/batch/jobs/ 更新作业
        @apiName update_job
        @apiGroup Batch
        @apiParam {string} processing_id processing的ID
        @apiParam {string} bk_username 提交人
        @apiParam {string} code_version 代码版本
        @apiParam {string} cluster_group 集群组
        @apiParam {string} cluster_name 细分集群名称
        @apiParam {string} deploy_mode 部署模式
        @apiParam {string} deploy_config 部署配置
        @apiParam {json} job_config 作业配置
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {int} project_id 项目ID
        @apiParamExample {json} 参数样例:
            {
              "processing_id": "xxx",
              "bk_username": "xxx",
              "code_version": "0.1.0",
              "cluster_group": "gem",
              "cluster_name": "root.bkdata.gem",
              "deploy_mode": "yarn",
              "deploy_config": "{executor_memory:1024m}",
              "job_config": { -- 这里先没有

              }
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
        args = request.data
        if "api_version" in args and args["api_version"] == "v2":
            JobsV2Driver.update_job(args)
            return Response({"job_id": args["processing_id"]})

        args["job_id"] = job_id
        args["bk_username"] = get_request_username()

        if args["job_config"] is not None and "submit_args" in args["job_config"]:
            submit_args = args["job_config"]["submit_args"]
            if "batch_type" in submit_args and submit_args["batch_type"] == "spark_python_code":
                job_info_param = job_driver.parse_code_job_info_params(args)
                try:
                    processing_type = DataProcessingHelper.get_data_processing_type(job_id)
                except Exception as e:
                    batch_logger.info(e)
                    processing_type = "batch"
                job_id = ProcessingJobInfoHandler.update_proc_job_info(job_info_param, processing_type=processing_type)
                return Response({"job_id": job_id})

        args["job_config"] = json.dumps(
            model_to_dict(ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(job_id)),
            cls=CustomJSONEncoder,
        )
        job_driver.set_cluster_group(args)
        job_driver.set_deploy_config_engine_conf(args)
        ProcessingJobInfoHandler.update_proc_job_info(args)
        return Response({"job_id": job_id})

    def retrieve(self, request, job_id):
        """
        @api {get} /dataflow/batch/jobs/:job_id 获取离线processing
        @apiName update_processing
        @apiGroup Batch
        @apiParam {string} processing_id processing的id
        @apiSuccessExample {json}
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "job_id": xxx,
                        "processor_type": "sql",
                        "code_version": "0.1.0",
                        "component_type": "xxx",
                        "jobserver_config": "{}" ,
                        "cluster_group": "default",
                        "cluster_name": "default",
                        "job_config": "{}",
                        "deploy_mode": "xxx",
                        "deploy_config": "591_xxx",
                        "implement_type": "2018-12-19T14:43:29",
                        "programming_language": "591_xxx",
                        "created_by": "xxx"
                        "created_at": "2018-12-19T14:43:29"
                        "updated_by": "xxx"
                        "updated_at": "2018-12-19T14:43:29"
                        "description": "xxx"
                    },
                    "result": true
                }
        """
        if ProcessingJobInfoHandler.is_proc_job_info(job_id):
            job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
            return Response(model_to_dict(job_info))
        else:
            raise JobInfoNotFoundException(message=_("不存在的processing_job_info id (%s)" % job_id))

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, job_id):
        """
        @api {post} /dataflow/batch/jobs/:jid/start 启动作业
        @apiName job_start
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} job_id 作业ID
        @apiParamExample {json} 参数样例:
            {
                "api_version": "v2",
                "job_id": "xxx",
                "is_restart": False,
                "bk_username": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": null,
                    "result": true
                }
        """
        args = request.data

        if "api_version" in args and args["api_version"] == "v2":
            JobsV2Driver.start_job(job_id, args)
            return Response()

        is_restart = args["is_restart"]
        current_time_mills = int(time.time() * 1000)
        created_by = get_request_username()
        if not created_by:
            raise BatchIllegalArgumentError(message=_("无创建人"))
        job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)

        # TODO 灰度parquet验证，在启动节点更新hdfs存储为parquet
        job_driver.process_parquet_white_list(job_info, job_id)

        job_driver.register_schedule(job_id, job_info, current_time_mills, created_by, is_restart)
        udfs = processing_udf_info.where(processing_id=job_id)
        job_driver.ProcessingBatchJobHandler.save_proc_batch_job(job_id, job_info, current_time_mills, created_by)
        job_driver.delete_udf(job_id)
        job_driver.save_udf(job_id, udfs)
        return Response()

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, job_id):
        """
        @api {post} /dataflow/batch/jobs/:jid/stop 停止作业
        @apiName job_stop
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} job_id 作业ID
        @apiParamExample {json} 参数样例:
            {
                "api_version": "v2",
                "job_id": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        if "api_version" in args and args["api_version"] == "v2":
            JobsV2Driver.stop_job(job_id)
            return Response()

        job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
        jobserver_config = json.loads(job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
        jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.stop_schedule(job_id)
        job_driver.ProcessingBatchJobHandler.stop_proc_batch_job(job_id)
        return Response()

    def destroy(self, request, job_id):
        """
        @api {delete} /dataflow/batch/jobs/:jid/ 删除离线job
        @apiName delete_processing
        @apiGroup Batch
        @apiParam {string} processing_id processing的id
        @apiParamExample {json} 参数样例:
            {
                "job_id": job_id
            }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {"processing_id": processing_id},
                    "result": true
                }
        """
        if ProcessingJobInfoHandler.is_proc_job_info(job_id):
            job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)

            submit_args = json.loads(json.loads(job_info.job_config)["submit_args"])
            if submit_args["batch_type"] != "spark_python_code":
                raise BatchIllegalArgumentError(message=_("离线(%s)作业不是一个code项目") % job_id)

            jobserver_config = json.loads(job_info.jobserver_config)
            geog_area_code = jobserver_config["geog_area_code"]

            cluster_id = job_driver.JobNaviHelper.get_jobnavi_cluster("batch")
            jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
            if job_driver.ProcessingBatchJobHandler.is_proc_batch_job_active(job_id):
                raise BatchIllegalStatusError(message=_("离线(%s)作业正在运行") % job_id)
            jobnavi.delete_schedule(job_id)
            # 删除离线作业processing维度配置
            ProcessingJobInfoHandler.delete_proc_job_info(job_id)
            batch_logger.info("{} has been deleted".format(job_id))
        return Response()

    @detail_route(methods=["post"], url_path="stop_source")
    def stop_source(self, request, job_id):
        geog_area_code = TagHelper.get_geog_area_code_by_rt_id(job_id)
        cluster_id = job_driver.JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
        res = jobnavi.get_schedule_info(job_id)
        if res:
            jobnavi.stop_schedule(job_id)
        return Response()

    @detail_route(methods=["get"], url_path="execute_history")
    def execute_history(self, request, job_id):
        limit = request.GET.get("sql", 24)
        geog_area_code = TagHelper.get_geog_area_code_by_rt_id(job_id)
        cluster_id = job_driver.JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
        results = jobnavi.get_schedule_execute_result(job_id, limit)
        custom_calculate_list = get_job_execute_list(job_id)
        recovery_execute_list = jobnavi.get_recovery_execute(job_id, limit)
        for result in results:
            for custom_calculate in custom_calculate_list:
                if result["execute_id"] == custom_calculate["execute_id"]:
                    result["type"] = "custom_calculate"
            for recovery_execute in recovery_execute_list:
                if result["execute_id"] == recovery_execute["execute_id"]:
                    result["type"] = "recovery_calculate"
            if "type" not in result:
                result["type"] = "default_calculate"
        return Response(results)

    @detail_route(methods=["get"], url_path="get_param")
    def get_param(self, request, job_id):
        """
        @api {get} /dataflow/batch/jobs/:jid/get_param 获取作业参数
        @apiName job_get_param
        @apiGroup Batch
        @apiParam {string} job_id 作业ID
        @apiParamExample {json} 参数样例:
            {
                "job_id": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                                "count_freq": 1
                                "schedule_time": 1539778425000,
                                "schedule_period": hour,
                                "storages": [
                                    {
                                        "id": 1,
                                        "result_table_id": "xxx",
                                        "cluster": {
                                            "cluster_name": "xxx",
                                            "cluster_type": "xxx",
                                            "cluster_domain": "xxx",
                                            "cluster_group": "xxx",
                                            "connection_info": "{}",
                                            "priority": 234,
                                            "version": "23423",
                                            "belongs": "bkdata"
                                        },
                                        "physical_table_name": "xxx",
                                        "expires": "xxx",
                                        "storage_config": "xxx",
                                        "priority": 1,
                                        "description": "xxx",
                                        "created_by": null,
                                        "created_at": null,
                                        "updated_by": null,
                                        "updated_at": null
                                    },
                                    ...
                                ],
                                "content": "select * from xxx",
                                "parent_ids": {
                                    "104_bep_tgp_install_hour": {
                                    "window_size": 1,
                                    "window_size_period": hour
                                    "type": "batch",
                                    },
                                    ...
                                },
                                "delay": 1,
                                "accumulate": false

                    },
                    "result": true
                }
        """
        batch_job = job_driver.ProcessingBatchJobHandler.get_proc_batch_job(job_id)
        if batch_job.processor_type.lower() == "batch_sql_v2":
            return Response(JobsV2Driver.get_param(batch_job))

        batch_job = job_driver.ProcessingBatchJobHandler.get_proc_batch_job(job_id)
        submit_args = json.loads(batch_job.submit_args)

        if "batch_type" in submit_args and submit_args["batch_type"] == "spark_python_code":
            return Response(job_driver.create_topo_response(batch_job))

        result_tables = processings_driver.handle_static_data_and_storage_type(submit_args)
        job_driver.set_self_dependency_rt(submit_args, job_id, result_tables)
        job_driver.format_parent_rt_type(result_tables)
        udfs = processing_udf_job.where(job_id=job_id)
        udf_array = []
        for udf in udfs:
            udf_array.append(model_to_dict(udf))

        result = {
            "count_freq": batch_job.count_freq,
            "schedule_time": batch_job.schedule_time,
            "schedule_period": batch_job.schedule_period,
            "storages": json.loads(batch_job.storage_args),
            "content": batch_job.processor_logic,
            "parent_ids": result_tables,
            "accumulate": submit_args["accumulate"],
            "data_start": submit_args["data_start"],
            "data_end": submit_args["data_end"],
            "delay": batch_job.delay,
            "udf": udf_array,
        }

        try:
            deploy_config = json.loads(batch_job.deploy_config)
            if "engine_conf" in deploy_config and len(deploy_config["engine_conf"]) > 0:
                result["engine_conf"] = deploy_config["engine_conf"]
        except Exception as e:
            batch_logger.exception(e)

        # return_value = {
        #     "errors": None,
        #     "message": "ok",
        #     "code": 1500200,
        #     "data": result,
        #     "result": True
        # }
        #
        # from django.http import HttpResponse
        # res_content = json.dumps(return_value).replace('\\\\', '\\')
        # return HttpResponse(res_content, content_type='application/json')
        return Response(result)

    @detail_route(methods=["get"], url_path="schedule_info")
    def schedule_info(self, request, job_id):
        """
        @api {get} /dataflow/batch/jobs/:jid/schedule_info 执行作业
        @apiName schedule_info
        @apiGroup Batch
        @apiParam {string} job_id 作业ID
        @apiParamExample {json} 参数样例:
            {
                "job_id": "2018010122"
            }
        @apiSuccessExample {string} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "node_label": null,
                        "recovery_info": {
                            "enable": false,
                            "interval_time": "1h",
                            "retry_times": 0
                        },
                        "description": " Project 591_sz_normal_test submit by xxx",
                        "type_id": "spark_sql",
                        "period": {
                            "delay": null,
                            "frequency": 1,
                            "timezone": "Asia/Shanghai",
                            "first_schedule_time": 1582540200000,
                            "cron_expression": null,
                            "period_unit": "hour"
                        },
                        "created_by": null,
                        "schedule_id": "591_sz_normal_test",
                        "exec_oncreate": false,
                        "decommission_timeout": null,
                        "active": true,
                        "parents": [],
                        "max_running_task": -1,
                        "execute_before_now": false,
                        "extra_info": "{\"geog_area_code\": \"inland\", \"makeup\": false,
                                        \"queue\": \"root.dataflow.batch.default\", \"debug\": false, \"type\":
                                        \"batch_sql\", \"run_mode\": \"PRODUCT\"}"
                    }
                    "result": true
                }
        """
        job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
        jobserver_config = json.loads(job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
        jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
        jobnavi_response = jobnavi.get_schedule_info(job_id)
        return Response(jobnavi_response)

    @detail_route(methods=["get"], url_path="execute")
    @params_valid(serializer=JobExecuteSerializer)
    def execute(self, request, job_id, params):
        """
        @api {get} /dataflow/batch/jobs/:jid/execute 执行作业
        @apiName execute
        @apiGroup Batch
        @apiParam {string} job_id 作业ID
        @apiParam {string} data_time 数据时间,格式为YYYYMMDD 或者 YYYYMMDDHH
        @apiParamExample {json} 参数样例:
            {
                "data_time": "2018010122"
            }
        @apiSuccessExample {string} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": null,
                    "result": true
                }
        """
        data_time = params["data_time"]
        job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
        job_driver.execute_job(job_id, job_info, data_time)
        return Response()

    @detail_route(methods=["get"], url_path="add_retry")
    @params_valid(serializer=JobRetrySerializer)
    def add_retry(self, request, job_id, params):
        """
        @api {get} /dataflow/batch/jobs/:jid/add_retry 调用总线分发接口失败时，使用此接口增加一条重试记录
        @apiName job_add_retry
        @apiGroup Batch
        @apiParam {string} job_id 作业ID
        @apiParam {string} schedule_time 调度时间
        @apiParam {string} data_dir 数据目录
        @apiParamExample {json} 参数样例:
            {
                "job_id": "xxx"
                "schedule_time": 1539778425000
                "data_dir": "/api/flow/xxx/xxxx_xx/2018/01/01/01"
            }
        @apiSuccessExample {string} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": null,
                    "result": true
                }
        """
        schedule_time = params["schedule_time"]
        data_dir = params["data_dir"]
        batch_storage_type = params["batch_storage_type"] if "batch_storage_type" in params else "HDFS"
        StorageHdfsExportRetryHander.save_retry_info(job_id, schedule_time, data_dir, batch_storage_type)
        return Response()

    @list_route(methods=["get"], url_path="retry_shipper_api")
    def retry_shipper_api(self, request):
        """
        @api {get} /dataflow/batch/jobs/retry_shipper_api 重试调用总线分发接口
        @apiName job_retry_shipper_api
        @apiGroup Batch
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": null,
                    "result": true
                }
        """
        current_ts = time.time()
        start_min_ts = int(round(current_ts * 1000)) - 24 * 60 * 60 * 1000
        retry_list = StorageHdfsExportRetryHander.list_by_schedule_time_gt(start_min_ts)
        if retry_list:
            for retry in retry_list:
                if retry.retry_times + 1 >= settings.databus_max_retry_times:
                    err_msg = _("%(result_table_id)s %(schedule_time)s %(data_dir)s调用总线(分发)超过最大重试次数") % {
                        "result_table_id": retry.result_table_id,
                        "schedule_time": retry.schedule_time,
                        "data_dir": retry.data_dir,
                        "batch_storage_type": retry.batch_storage_type,
                    }
                    batch_logger.error(err_msg)
                    continue
                try:
                    if retry.batch_storage_type == "TDW":
                        if len(retry.data_dir) > 8:
                            # data_dir = tdw://cluster:db_name/table_name/2019101323
                            data_time = retry.data_dir[6:].split("/")[2]
                            DatabusHelper.tdw_shipper(retry.result_table_id, data_time, "admin")
                    else:
                        DatabusHelper.import_hdfs(retry.result_table_id, retry.data_dir, "admin")
                    StorageHdfsExportRetryHander.delete_retry_info(
                        retry.result_table_id,
                        retry.schedule_time,
                        retry.data_dir,
                        retry.batch_storage_type,
                    )
                except Exception as e:
                    batch_logger.exception(e)
                    StorageHdfsExportRetryHander.update_retry_info(
                        retry.result_table_id,
                        retry.schedule_time,
                        retry.data_dir,
                        retry.batch_storage_type,
                        retry.retry_times + 1,
                    )
        return Response()

    @detail_route(methods=["post"], url_path="update_node_label")
    @params_valid(serializer=JobsUpdateNodeLabelSerializer)
    def update_node_label(self, request, job_id, params):
        """
        @api {post} /dataflow/batch/jobs/:jid/update_node_label 注册清理过期数据
        @apiName update_node_label
        @apiGroup Batch
        @apiParam {string} job_id job_id
        @apiParam {string} node_label jobnavi的node_label
        @apiParamExample {json} 参数样例:
        {
            "node_label": "label_1"
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(job_driver.update_batch_node_label(job_id, params))

    @detail_route(methods=["post"], url_path="update_engine_conf")
    @params_valid(serializer=JobsUpdateEngineConfSerializer)
    def update_engine_conf(self, request, job_id, params):
        """
        @api {post} /dataflow/batch/jobs/:jid/update_engine_conf 注册清理过期数据
        @apiName update_engine_conf
        @apiGroup Batch
        @apiParam {string} job_id job_id
        @apiParam {dict} engine_conf spark配置
        @apiParamExample {json} 参数样例:
        {
            "engine_conf": {
                "spark.executor.cores": "4"
            }
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(job_driver.update_batch_engine_conf(job_id, params))

    @detail_route(methods=["get"], url_path="get_engine_conf")
    def get_engine_conf(self, request, job_id):
        """
        @api {post} /dataflow/batch/jobs/:jid/get_engine_conf 注册清理过期数据
        @apiName get_engine_conf
        @apiGroup Batch
        @apiParam {string} job_id job_id
        @apiParamExample {json} 参数样例:
        {
            "job_id": "591_test_job_id"
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "spark.executor.cores": "4"
                }
        """
        return Response(job_driver.get_engine_conf(job_id))

    @detail_route(methods=["get"], url_path="jobnavi_data_dependency_param")
    def jobnavi_data_dependency_param(self, request, job_id):
        """
        @api {post} /dataflow/batch/jobs/:jid/jobnavi_data_dependency_param get new jobnavi param
        @apiName jobnavi_data_dependeny_param
        @apiGroup Batch
        @apiParam {string} job_id job_id
        @apiParamExample {json} 参数样例:
        {
            "job_id": "591_test_job_id"
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "data_time_offset": "4h"
                    "result_tables": {}
                }
        """
        return Response(job_driver.get_jobnavi_data_dependency_param(job_id))


class JobProcessingsViewSet(APIViewSet):
    lookup_field = "processing_id"
    lookup_value_regex = r"\w+"

    @detail_route(methods=["get"], url_path="fetch_top_parent_delay")
    def fetch_top_parent_delay(self, request, job_id, processing_id):
        """
        @api {get} /dataflow/batch/jobs/:jid/processings/:pid/fetch_top_parent_delay 获取离线作业琏中第一个节点配置的延迟
        @apiName fetch_top_parent_delay
        @apiGroup Batch
        @apiParam {string} job_id 作业ID
        @apiParamExample {json} 参数样例:
            {
                "job_id": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": 0,
                    "result": true
                }
        """
        delay = job_driver.fetch_top_parent_delay(job_id)
        if not delay:
            delay = 0
        return Response(delay)
