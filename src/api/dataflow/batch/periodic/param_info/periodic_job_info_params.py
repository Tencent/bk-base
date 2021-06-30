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

from dataflow.batch import settings
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingJobInfo


class DeployConfig(object):
    def __init__(self):
        # for admin to update engine_conf and node_label, these params should have high priority
        self.engine_conf = {}  # type: dict
        self.node_label = None  # type: str
        # for user to pass engine_conf and node_label
        self.user_engine_conf = {}  # type: dict

    def from_db_json(self, json_obj):

        if "engine_conf" in json_obj:
            self.engine_conf = json_obj["engine_conf"]

        if "node_label" in json_obj:
            self.node_label = json_obj["node_label"]

        if "user_engine_conf" in json_obj:
            self.engine_conf = json_obj["user_engine_conf"]

    def to_db_json(self):
        result_json = {}

        if self.engine_conf is not None and len(self.engine_conf) > 0:
            result_json["engine_conf"] = self.engine_conf

        if self.user_engine_conf is not None and len(self.user_engine_conf) > 0:
            result_json["user_engine_conf"] = self.user_engine_conf

        if self.node_label is not None:
            result_json["node_label"] = self.node_label
        return result_json


class JobserverConfig(object):
    def __init__(self):
        self.geog_area_code = None  # type: str
        self.cluster_id = None  # type: str

    def from_db_json(self, json_obj):
        self.geog_area_code = json_obj["geog_area_code"]
        self.cluster_id = json_obj["cluster_id"]

    def to_db_json(self):
        db_json = {"geog_area_code": self.geog_area_code, "cluster_id": self.cluster_id}
        return db_json


class JobConfig(object):
    def __init__(self):
        self.processings = []  # type: list
        self.resource_group_id = None  # type: str

    def from_db_json(self, json_obj):
        self.processings = json_obj["processings"]
        if "resource_group_id" in json_obj:
            self.resource_group_id = json_obj["resource_group_id"]

    def to_db_json(self):
        db_json = {
            "processings": self.processings,
            "resource_group_id": self.resource_group_id,
        }
        return db_json


class PeriodicJobInfoParams(object):
    def __init__(self):
        self.processing_id = None  # type: str
        self.processing_type = "batch"  # type: str
        self.bk_username = None  # type: str
        self.cluster_group = None  # type: str
        self.cluster_name = None  # type: str
        self.deploy_mode = None  # type: str
        self.deploy_config = None  # type: DeployConfig
        self.jobserver_config = None  # type: JobserverConfig
        self.job_config = None  # type: JobConfig
        self.implement_type = None  # type: str
        self.programming_language = None  # type: str

        self.code_version = self.__choose_value("code_version")
        self.component_type = self.__choose_value("component_type")

    def from_processing_job_info_db(self, job_id):
        job_info_obj = ProcessingJobInfoHandler.get_proc_job_info(job_id)  # type: ProcessingJobInfo
        self.bk_username = job_info_obj.created_by
        self.processing_id = job_info_obj.job_id
        self.code_version = job_info_obj.code_version
        self.cluster_group = job_info_obj.cluster_group
        self.cluster_name = job_info_obj.cluster_name
        self.deploy_mode = job_info_obj.deploy_mode

        self.deploy_config = DeployConfig()
        self.deploy_config.from_db_json(json.loads(job_info_obj.deploy_config))
        self.job_config = JobConfig()
        self.job_config.from_db_json(json.loads(job_info_obj.job_config))
        self.jobserver_config = JobserverConfig()
        self.jobserver_config.from_db_json(json.loads(job_info_obj.jobserver_config))
        self.implement_type = job_info_obj.implement_type
        self.programming_language = job_info_obj.programming_language

    def __choose_value(self, key):
        return getattr(settings, "%s_DEFAULT" % key.upper())

    def save_to_processing_job_info_db(self):
        ProcessingJobInfoHandler.create_processing_job_info_v2(
            job_id=self.processing_id,
            processing_type=self.processing_type,
            code_version=self.code_version,
            component_type=self.component_type,
            job_config=json.dumps(self.job_config.to_db_json()),
            jobserver_config=json.dumps(self.jobserver_config.to_db_json()),
            cluster_group=self.cluster_group,
            cluster_name=self.cluster_name,
            deploy_mode=self.deploy_mode,
            deploy_config=json.dumps(self.deploy_config.to_db_json()),
            created_by=self.bk_username,
            updated_by=self.bk_username,
            implement_type=self.implement_type,
            programming_language=self.programming_language,
        )

    def update_to_processing_job_info_db(self):
        ProcessingJobInfoHandler.update_processing_job_info_v2(
            processing_id=self.processing_id,
            job_id=self.processing_id,
            processing_type=self.processing_type,
            code_version=self.code_version,
            component_type=self.component_type,
            job_config=json.dumps(self.job_config.to_db_json()),
            jobserver_config=json.dumps(self.jobserver_config.to_db_json()),
            cluster_group=self.cluster_group,
            cluster_name=self.cluster_name,
            deploy_mode=self.deploy_mode,
            deploy_config=json.dumps(self.deploy_config.to_db_json()),
            created_by=self.bk_username,
            updated_by=self.bk_username,
            implement_type=self.implement_type,
            programming_language=self.programming_language,
        )
