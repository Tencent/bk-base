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

from common.local import get_request_username

from dataflow.shared.log import uc_logger as logger
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.uc.adapter.unified_computing_operation import UnifiedComputingOperation
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException


class TensorFlowCodeOperationAdapter(UnifiedComputingOperation):
    @classmethod
    def create_processing(cls, processing_conf):
        request_params = cls.__generate_proc_request_params(processing_conf)
        return ModelingHelper.create_processing(**request_params)

    def update_processing(self, processing_id, processing_conf):
        request_params = self.__generate_proc_request_params(processing_conf)
        request_params.update({"processing_id": processing_id})
        return ModelingHelper.update_processing(**request_params)

    @classmethod
    def __generate_proc_request_params(cls, processing_conf):
        bk_biz_id, name = processing_conf["processing_id"].split("_", 1)
        return {
            "bk_username": get_request_username(),
            "project_id": processing_conf["project_id"],
            "tags": processing_conf["tags"],
            "name": name,
            "bk_biz_id": bk_biz_id,
            "processor_logic": {
                "user_args": processing_conf["processor_logic"]["user_args"],
                "user_main_class": processing_conf["processor_logic"]["user_main_class"],
                "user_package": processing_conf["processor_logic"]["user_package"],
            },
            "dedicated_config": {
                "batch_type": processing_conf["component_type"],
                "self_dependence": processing_conf["processor_logic"]["self_dependence"],
                "schedule_config": processing_conf["schedule_info"]["schedule_config"],
                "recovery_config": processing_conf["schedule_info"]["recovery_config"],
                "input_config": processing_conf["data_link"]["input_config"],
                "output_config": processing_conf["data_link"]["output_config"],
            },
            "window_info": processing_conf["processor_logic"]["window_info"],
        }

    @classmethod
    def create_job(cls, job_vertex):
        if len(job_vertex.processing) != 1:
            raise IllegalArgumentException(
                "The tensorflow code job will only contain one processing, actually contains %s"
                % " ".join(processing.processing_id for processing in job_vertex.processing)
            )
        one_processing = job_vertex.processing[0]
        logger.info("To create tensorflow code job, and processing is %s" % str(one_processing))
        request_params = cls.__generate_common_request_params(one_processing)
        return ModelingHelper.create_model_job(request_params)["job_id"]

    def update_job(self, job_id, job_vertex):
        if len(job_vertex.processing) != 1:
            raise IllegalArgumentException(
                "The tensorflow code job will only contain one processing, actually contains %s"
                % " ".join(processing.processing_id for processing in job_vertex.processing)
            )
        one_processing = job_vertex.processing[0]
        logger.info("To update tensorflow code job, and processing is %s" % str(one_processing))
        request_params = self.__generate_common_request_params(one_processing)
        request_params.update({"job_id": job_id})
        return ModelingHelper.update_modeling_job(**request_params)["job_id"]

    def delete_job(self, job_id):
        ModelingHelper.delete_job(job_id)

    @classmethod
    def __generate_common_request_params(cls, one_processing):
        return {
            "processing_id": one_processing.processing_id,
            "cluster_group": one_processing.conf["deploy_config"]["cluster_group"],
            "project_id": one_processing.project_id,
        }

    def start_job(self, job_id):
        request_params = {"job_id": job_id, "bk_username": get_request_username()}
        ModelingHelper.start_model_job(request_params)
        return {"operate": "start", "operate_id": None}

    def get_operate_result(self, job_id, operate_info):
        if operate_info["operate"] == "start":
            return {job_id: "ACTIVE"}
        elif operate_info["operate"] == "stop":
            return {job_id: None}
        else:
            raise IllegalArgumentException("Not support the operation %s" % operate_info["operate"])

    def stop_job(self, job_id):
        ModelingHelper.stop_model_job(job_id)
        return {"operate": "stop", "operate_id": None}
