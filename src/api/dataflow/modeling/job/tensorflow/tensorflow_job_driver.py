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

from django.utils.translation import ugettext_lazy as _
from pizza import settings

from dataflow.batch.exceptions.comp_execptions import BatchIllegalStatusError
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.periodic.jobs_v2_driver import JobsV2Driver
from dataflow.batch.utils import time_util
from dataflow.modeling.debug.debugs import filter_debug_metric_log
from dataflow.modeling.job.tensorflow.tensorflow_batch_job_builder import TensorFlowBatchJobBuilder
from dataflow.modeling.job.tensorflow.tensorflow_batch_job_params import TensorFlowBatchJobParams
from dataflow.modeling.job.tensorflow.tensorflow_job_info_builder import TensorFlowJobInfoBuilder
from dataflow.modeling.job.tensorflow.tensorflow_job_info_params import TensorFlowJobInfoParams
from dataflow.modeling.job.tensorflow.tensorflow_scheduler_register import TensorFlowSchedulerRegister
from dataflow.modeling.processing.tensorflow.tensorflow_code_processings_back_end import (
    TensorFlowCodeProcessingsBackend,
)
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.pizza_settings import BASE_DATAFLOW_URL
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper


class TensorFlowJobDriver(JobsV2Driver):
    @staticmethod
    def create_job(args):
        """
        本函数用于flow-api调用，创建深度学习任务对应的job，其中参数需要直接指名cluster,jobserver_config等信息
        """
        try:
            job_info_params_builder = TensorFlowJobInfoBuilder()
            job_info_params_obj = job_info_params_builder.build_job_from_flow_api(args)
            job_info_params_obj.save_to_processing_job_info_db()
        except Exception as e:
            logger.exception(e)
            raise e

    @staticmethod
    def get_job_params_from_processing(processing_id, project_id, cluster_group=None):
        geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobserver_config = {"geog_area_code": geog_area_code, "cluster_id": cluster_id}

        if not cluster_group:
            # 获取默认的cluster_group
            cluster_group = settings.MLSQL_CLUSEER_GRUUP
            default_cluster_group = settings.MLSQL_DEFAULT_CLUSTER_GROUP
            default_cluster_name = settings.MLSQL_DEFAULT_CLUSTER_NAME
            cluster_group, cluster_name = ModelingUtils.get_cluster_group(
                cluster_group, default_cluster_group, default_cluster_name
            )
        job_params = {
            "api_version": "v2",
            "processing_id": processing_id,
            "jobserver_config": jobserver_config,
            "deploy_mode": "yarn",
            "deploy_config": {},
            "cluster_group": cluster_group,
            "code_version": "0.1.0",
        }
        return job_params

    @staticmethod
    def update_job(args):
        job_info_params_builder = TensorFlowJobInfoBuilder()
        job_info_params_obj = job_info_params_builder.build_job_from_flow_api(args)
        job_info_params_obj.update_to_processing_job_info_db()

    @staticmethod
    def start_job(job_id, args):
        batch_job_param_builder = TensorFlowBatchJobBuilder()
        batch_job_param_obj = batch_job_param_builder.build_from_flow_start_api(job_id, args)

        scheduler_register = TensorFlowSchedulerRegister(batch_job_param_obj)
        scheduler_register.register_jobnavi()
        batch_job_param_obj.save_to_processing_batch_job_db(True)

    @staticmethod
    def stop_job(job_id):
        periodic_job_info_obj = TensorFlowJobInfoParams()
        periodic_job_info_obj.from_processing_job_info_db(job_id)
        geog_area_code = periodic_job_info_obj.jobserver_config.geog_area_code
        cluster_id = periodic_job_info_obj.jobserver_config.cluster_id
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.stop_schedule(job_id)
        ProcessingBatchJobHandler.stop_proc_batch_job(job_id)

    @staticmethod
    def delete_job(job_id, with_data=False):
        periodic_job_info_obj = TensorFlowJobInfoParams()
        periodic_job_info_obj.from_processing_job_info_db(job_id)
        processing_id = periodic_job_info_obj.processing_id
        # 如果在运行，直接退出，提醒无法删除
        if TensorFlowJobDriver.check_job_running(processing_id):
            raise BatchIllegalStatusError(message=_("离线(%s)作业正在运行") % processing_id)
        # 先移除调度信息
        TensorFlowJobDriver.delete_schedule_info(processing_id)
        # 删除processnng信息
        TensorFlowCodeProcessingsBackend.delete_procesing_info(processing_id, with_data=with_data)
        # 删除job信息
        TensorFlowJobDriver.delete_job_info(job_id)

    @staticmethod
    def check_job_running(processing_id):
        if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            if ProcessingBatchJobHandler.is_proc_batch_job_active(processing_id):
                return True
        return False

    @staticmethod
    def delete_schedule_info(processing_id):
        geog_area_code = TagHelper.get_geog_area_code_by_processing_id(processing_id)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.delete_schedule(processing_id)

    @staticmethod
    def delete_job_info(job_id):
        periodic_job_info_obj = TensorFlowJobInfoParams()
        periodic_job_info_obj.from_processing_job_info_db(job_id)
        # 删除batch job
        ProcessingBatchJobHandler.delete_proc_batch_job_v2(job_id)
        # 删除job
        ProcessingJobInfoHandler.delete_proc_job_info(job_id)

    @staticmethod
    def get_scheduler_status(job_id, start_time, end_time):
        periodic_job_info_obj = TensorFlowJobInfoParams()
        periodic_job_info_obj.from_processing_job_info_db(job_id)
        geog_area_code = periodic_job_info_obj.jobserver_config.geog_area_code
        cluster_id = periodic_job_info_obj.jobserver_config.cluster_id
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        start_time_in_mills = time_util.to_milliseconds_timestamp_in_hour(start_time, "%Y-%m-%d %H:%M:%S")
        end_time_in_mills = time_util.to_milliseconds_timestamp_in_hour(end_time, "%Y-%m-%d %H:%M:%S")
        return jobnavi.get_submitted_history_by_time(job_id, start_time_in_mills, end_time_in_mills)

    @staticmethod
    def get_param(batch_job_obj, run_mode=None):
        """
        :type batch_job_obj: dataflow.batch.models.bkdata_flow.ProcessingBatchJob
        """
        batch_job_param_obj = TensorFlowBatchJobParams()
        batch_job_param_obj.from_processing_batch_job_db_obj(batch_job_obj)
        batch_job_param_builder = TensorFlowBatchJobBuilder()
        try:
            batch_job_param_new_obj = batch_job_param_builder.copy_and_refresh_batch_job_params(batch_job_param_obj)
            logger.info("copy and refresh..")
            if not batch_job_param_new_obj.content_equal(batch_job_param_obj):
                logger.info(
                    "New job json {} is different from old, will refresh batch job table".format(
                        batch_job_param_obj.job_id
                    )
                )
                logger.info("Old json: {}".format(batch_job_param_obj.to_json()))
                logger.info("New json: {}".format(batch_job_param_new_obj.to_json()))
                batch_job_param_new_obj.save_to_processing_batch_job_db(True)
                return batch_job_param_new_obj.to_json()
        except Exception as e:
            logger.error(e)
            logger.exception(e)
        param_json = batch_job_param_obj.to_json()
        # 补充公共信息
        param_json["version"] = "v2"
        param_json["run_mode"] = run_mode
        param_json["time_zone"] = "Asia/Shanghai"
        if run_mode == "debug":
            DEBUG_URL = BASE_DATAFLOW_URL + "modeling/debugs"
            debug_info = filter_debug_metric_log(debug_id=batch_job_param_obj.job_id)[0]
            param_json["debug"] = {
                "debug_id": debug_info.debug_id,
                "debug_rest_api_url": "{debug_url}/{debug_id}/debug_result_info/".format(
                    debug_url=DEBUG_URL, debug_id=batch_job_param_obj.job_id
                ),
                "debug_exec_id": str(debug_info.job_id),
            }

        else:
            url = BASE_DATAFLOW_URL
            url = url.rstrip("/").rstrip("dataflow").rstrip("/")
            param_json["metric"] = {"metric_rest_api_url": url + "/datamanage/dmonitor/metrics/report/"}
        return param_json
