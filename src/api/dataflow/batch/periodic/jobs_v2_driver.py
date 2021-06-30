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

from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.periodic.param_info.builder.periodic_batch_job_builder import PeriodicBatchJobBuilder
from dataflow.batch.periodic.param_info.builder.periodic_job_info_builder import PeriodicJobInfoBuilder
from dataflow.batch.periodic.param_info.periodic_batch_job_params import PeriodicBatchJobParams
from dataflow.batch.periodic.param_info.periodic_job_info_params import PeriodicJobInfoParams
from dataflow.batch.periodic.scheduler.periodic_scheduler_register import PeriodicSchedulerRegister
from dataflow.shared.handlers import processing_udf_job
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger


class JobsV2Driver(object):
    @staticmethod
    def create_job(args):
        job_info_params_builder = PeriodicJobInfoBuilder()
        job_info_params_obj = job_info_params_builder.build_sql_from_flow_api(args)
        job_info_params_obj.save_to_processing_job_info_db()

    @staticmethod
    def update_job(args):
        job_info_params_builder = PeriodicJobInfoBuilder()
        job_info_params_obj = job_info_params_builder.build_sql_from_flow_api(args)
        job_info_params_obj.update_to_processing_job_info_db()

    @staticmethod
    def start_job(job_id, args):
        batch_job_param_builder = PeriodicBatchJobBuilder()
        batch_job_param_obj = batch_job_param_builder.build_from_flow_start_api(job_id, args)

        scheduler_register = PeriodicSchedulerRegister(batch_job_param_obj)
        scheduler_register.register_jobnavi()
        batch_job_param_obj.save_to_processing_batch_job_db(True)
        JobsV2Driver.__delete_udf_job(batch_job_param_obj.job_id)
        JobsV2Driver.__save_to_udf_job(batch_job_param_obj.job_id, batch_job_param_obj.udf)

    @staticmethod
    def stop_job(job_id):
        periodic_job_info_obj = PeriodicJobInfoParams()
        periodic_job_info_obj.from_processing_job_info_db(job_id)
        geog_area_code = periodic_job_info_obj.jobserver_config.geog_area_code
        cluster_id = periodic_job_info_obj.jobserver_config.cluster_id
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.stop_schedule(job_id)
        ProcessingBatchJobHandler.stop_proc_batch_job(job_id)

    @staticmethod
    def get_param(batch_job_obj):
        """
        :type batch_job_obj: dataflow.batch.models.bkdata_flow.ProcessingBatchJob
        """
        batch_job_param_obj = PeriodicBatchJobParams()
        batch_job_param_obj.from_processing_batch_job_db_obj(batch_job_obj)
        batch_job_param_builder = PeriodicBatchJobBuilder()
        try:
            batch_job_param_new_obj = batch_job_param_builder.copy_and_refresh_batch_job_params(batch_job_param_obj)
            if not batch_job_param_new_obj.content_equal(batch_job_param_obj):
                batch_logger.info(
                    "New job json {} is different from old, will refresh batch job table".format(
                        batch_job_param_obj.job_id
                    )
                )
                batch_logger.info("Old json: {}".format(batch_job_param_obj.to_json()))
                batch_logger.info("New json: {}".format(batch_job_param_new_obj.to_json()))
                batch_job_param_new_obj.save_to_processing_batch_job_db(True)
                return batch_job_param_new_obj.to_json()
        except Exception as e:
            batch_logger.error(e)
            batch_logger.exception(e)

        return batch_job_param_obj.to_json()

    @staticmethod
    def __delete_udf_job(job_id):
        processing_udf_job.delete(job_id=job_id)

    @staticmethod
    def __save_to_udf_job(job_id, udf_list):
        for udf in udf_list:
            processing_udf_job.save(
                job_id=job_id,
                processing_id=job_id,
                processing_type="batch",
                udf_name=udf["udf_name"],
                udf_info=udf["udf_info"],
            )
