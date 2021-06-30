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

from common.local import get_request_username

import dataflow.batch.settings as settings
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.periodic.param_info.periodic_batch_info_params import PeriodicBatchInfoParams
from dataflow.batch.periodic.param_info.periodic_job_info_params import (
    DeployConfig,
    JobConfig,
    JobserverConfig,
    PeriodicJobInfoParams,
)
from dataflow.batch.utils import resource_util
from dataflow.shared.handlers import processing_udf_info


class PeriodicJobInfoBuilder(object):
    def __init__(self):
        self.periodic_job_info_obj = None  # type: PeriodicJobInfoParams
        self.batch_info_obj = None  # type: PeriodicBatchInfoParams

    def build_sql_from_flow_api(self, params):
        self.periodic_job_info_obj = PeriodicJobInfoParams()
        self.batch_info_obj = PeriodicBatchInfoParams()
        self.build_from_flow_api(params)
        self.combine_engine_conf_node_label()
        return self.periodic_job_info_obj

    def build_from_flow_api(self, params):
        self.periodic_job_info_obj.bk_username = get_request_username()
        self.periodic_job_info_obj.processing_id = params["processing_id"]

        self.batch_info_obj.from_processing_batch_info_db(self.periodic_job_info_obj.processing_id)
        if self.batch_info_obj.batch_type.lower() == "batch_sql_v2":
            self.periodic_job_info_obj.implement_type = "sql"
            self.periodic_job_info_obj.programming_language = "java"

        self.periodic_job_info_obj.jobserver_config = self.build_jobserver_config(params["jobserver_config"])
        self.periodic_job_info_obj.deploy_config = self.build_user_deploy_config(params["deploy_config"])

        self.periodic_job_info_obj.cluster_group = params["cluster_group"]
        queue_name, resource_group_id = self.find_cluster_name()
        self.periodic_job_info_obj.cluster_name = queue_name

        self.periodic_job_info_obj.code_version = params["code_version"]
        self.periodic_job_info_obj.deploy_mode = params["deploy_mode"]
        self.periodic_job_info_obj.deploy_config = DeployConfig()

        self.periodic_job_info_obj.job_config = JobConfig()
        self.periodic_job_info_obj.job_config.processings.append(self.periodic_job_info_obj.processing_id)
        self.periodic_job_info_obj.job_config.resource_group_id = resource_group_id

        return self.periodic_job_info_obj

    def find_cluster_name(self):
        cluster_group = self.periodic_job_info_obj.cluster_group
        geog_area_code = self.periodic_job_info_obj.jobserver_config.geog_area_code
        return resource_util.get_yarn_queue_name(geog_area_code, cluster_group, "batch")

    def build_jobserver_config(self, jobserver_config_params):
        tmp_jobserver_config = JobserverConfig()
        tmp_jobserver_config.geog_area_code = jobserver_config_params["geog_area_code"]
        tmp_jobserver_config.cluster_id = jobserver_config_params["cluster_id"]
        return tmp_jobserver_config

    def build_user_deploy_config(self, deploy_config_params):
        tmp_deploy_config = DeployConfig()

        if "user_engine_conf" in deploy_config_params:
            tmp_deploy_config.user_engine_conf = deploy_config_params["user_engine_conf"]

        udfs = processing_udf_info.where(processing_id=self.periodic_job_info_obj.processing_id)
        if len(udfs) > 0:
            for udf_conf_item in settings.SPARK_SQL_UDF_ENGINE_CONF:
                tmp_deploy_config.user_engine_conf[udf_conf_item] = settings.SPARK_SQL_UDF_ENGINE_CONF[udf_conf_item]
        return tmp_deploy_config

    def combine_engine_conf_node_label(self):
        if ProcessingJobInfoHandler.is_proc_job_info(self.periodic_job_info_obj.processing_id):
            job_info = ProcessingJobInfoHandler.get_proc_job_info(self.periodic_job_info_obj.processing_id)
            deploy_config = json.loads(job_info.deploy_config)
            if "engine_conf" in deploy_config:
                self.periodic_job_info_obj.deploy_config.engine_conf = deploy_config["engine_conf"]

            if "node_label" in deploy_config:
                self.periodic_job_info_obj.deploy_config.node_label = deploy_config["node_label"]
