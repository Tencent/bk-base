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

from django.utils.translation import ugettext as _

from dataflow.batch.exceptions.comp_execptions import BatchIllegalStatusError
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.periodic.backend.processings_sql_backend import ProcessingsSqlBackend
from dataflow.batch.periodic.backend.validator.processings_validator import ProcessingsValidator
from dataflow.batch.periodic.param_info.builder.periodic_batch_info_builder import PeriodicBatchInfoBuilder
from dataflow.shared.handlers import processing_udf_info, processing_udf_job
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper


class ProcessingsSqlV2Driver(object):
    @staticmethod
    def create_processings(args):
        batch_info_params_builder = PeriodicBatchInfoBuilder()
        batch_info_params_obj = batch_info_params_builder.build_sql_from_flow_api(args)
        processings_validator = ProcessingsValidator()
        processings_validator.validate(batch_info_params_obj)
        processing_bachend = ProcessingsSqlBackend(batch_info_params_obj)
        return processing_bachend.create_processings()

    @staticmethod
    def update_processings(args):
        batch_info_params_builder = PeriodicBatchInfoBuilder()
        batch_info_params_obj = batch_info_params_builder.build_sql_from_flow_api(args)
        processings_validator = ProcessingsValidator()
        processings_validator.validate(batch_info_params_obj)
        processing_bachend = ProcessingsSqlBackend(batch_info_params_obj)
        return processing_bachend.update_processings()

    @staticmethod
    def delete_processings(processing_id, args):
        """
        To delete all periodic batch job
        :param processing_id:
        :param with_data:
        """
        geog_area_code = TagHelper.get_geog_area_code_by_processing_id(processing_id)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)

        if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            if ProcessingBatchJobHandler.is_proc_batch_job_active(processing_id):
                raise BatchIllegalStatusError(message=_("离线(%s)作业正在运行") % processing_id)
            ProcessingBatchInfoHandler.delete_proc_batch_info(processing_id)

        with_data = False
        if "with_data" in args:
            with_data = args["with_data"]

        DataProcessingHelper.delete_data_processing(processing_id, with_data=with_data)

        jobnavi.delete_schedule(processing_id)
        ProcessingJobInfoHandler.delete_proc_job_info(processing_id)
        processing_udf_job.delete(job_id=processing_id, processing_type="batch")
        processing_udf_info.delete(processing_id=processing_id, processing_type="batch")

    @staticmethod
    def upgrade_old_to_v2(processing_id):
        pass

    @staticmethod
    def revert_v2_to_old(processing_id):
        pass
