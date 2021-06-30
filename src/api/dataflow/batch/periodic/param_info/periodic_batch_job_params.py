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
from dataflow.batch.exceptions.comp_execptions import BatchIllegalArgumentError
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.periodic.param_info.periodic_job_info_params import DeployConfig


class SourceNode(object):
    def __init__(self):
        self.id = None  # type: str
        self.name = None  # type: str
        self.fields = []  # type: list
        self.window_type = None  # type: str
        self.dependency_rule = None  # type: str
        self.window_offset = None  # type: str
        self.window_size = None  # type: str
        self.window_start_offset = None  # type: str
        self.window_end_offset = None  # type: str
        self.accumulate_start_time = None  # type: int
        self.storage_type = None  # type: str
        self.storage_conf = {}  # type: dict
        self.is_managed = None  # type: int
        self.self_dependency_mode = ""  # type: str
        self.role = None  # type: str

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "fields": self.fields,
            "storage_type": self.storage_type,
            "storage_conf": self.storage_conf,
            "is_managed": self.is_managed,
            "self_dependency_mode": self.self_dependency_mode,
            "dependency_rule": self.dependency_rule,
            "window_type": self.window_type,
        }

        if self.window_type.lower() == "scroll" or self.window_type.lower() == "slide":
            result_json["window_offset"] = self.window_offset
            result_json["window_size"] = self.window_size
        elif self.window_type.lower() == "accumulate":
            result_json["window_offset"] = self.window_offset
            result_json["window_size"] = self.window_size
            result_json["window_start_offset"] = self.window_start_offset
            result_json["window_end_offset"] = self.window_end_offset
            result_json["accumulate_start_time"] = self.accumulate_start_time

        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.role = json_obj["role"]
        self.fields = json_obj["fields"]
        self.storage_type = json_obj["storage_type"]
        self.storage_conf = json_obj["storage_conf"]
        self.is_managed = json_obj["is_managed"]
        self.self_dependency_mode = json_obj["self_dependency_mode"]
        self.dependency_rule = json_obj["dependency_rule"]
        self.window_type = json_obj["window_type"]

        if "window_offset" in json_obj:
            self.window_offset = json_obj["window_offset"]

        if "window_size" in json_obj:
            self.window_size = json_obj["window_size"]

        if "window_start_offset" in json_obj:
            self.window_start_offset = json_obj["window_start_offset"]

        if "window_end_offset" in json_obj:
            self.window_end_offset = json_obj["window_end_offset"]

        if "accumulate_start_time" in json_obj:
            self.accumulate_start_time = json_obj["accumulate_start_time"]


class TransformNode(object):
    def __init__(self):
        self.id = None  # type: str
        self.name = None  # type: str
        self.processor_type = None  # type: str
        self.processor_logic = {}  # type: dict

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "processor_type": self.processor_type,
            "processor_logic": self.processor_logic,
        }
        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.processor_type = json_obj["processor_type"]
        self.processor_logic = json_obj["processor_logic"]


class SinkNode(object):
    def __init__(self):
        self.id = None  # type: str
        self.name = None  # type: str
        self.only_from_whole_data = False  # type: bool
        self.data_time_offset = None  # type: str
        self.fields = []  # type: list
        self.storage_type = ""  # type: list
        self.storage_conf = {}  # type: dict
        self.call_databus_shipper = False  # type: dict

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "data_time_offset": self.data_time_offset,
            "only_from_whole_data": self.only_from_whole_data,
            "fields": self.fields,
            "storage_type": self.storage_type,
            "storage_conf": self.storage_conf,
            "call_databus_shipper": self.call_databus_shipper,
        }
        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.data_time_offset = json_obj["data_time_offset"]
        self.only_from_whole_data = json_obj["only_from_whole_data"]
        self.fields = json_obj["fields"]
        self.storage_type = json_obj["storage_type"]
        self.storage_conf = json_obj["storage_conf"]
        self.call_databus_shipper = json_obj["call_databus_shipper"]


class Resource(object):
    cluster_group = None  # type: str
    queue_name = None  # type: str
    resource_group_id = None  # type: str

    def to_json(self):
        result_json = {
            "cluster_group": self.cluster_group,
            "queue_name": self.queue_name,
            "resource_group_id": self.resource_group_id,
        }
        return result_json

    def from_json(self, json_obj):
        self.cluster_group = json_obj["cluster_group"]
        self.queue_name = json_obj["queue_name"]
        if "resource_group_id" in json_obj:
            self.resource_group_id = json_obj["resource_group_id"]  # type: str


class RecoveryInfo(object):
    def __init__(self):
        self.recovery_enable = False  # type: bool
        self.recovery_interval = "1H"  # type: str
        self.retry_times = 0  # type: int

    def to_json(self):
        result_json = {
            "recovery_enable": self.recovery_enable,
            "recovery_interval": self.recovery_interval,
            "retry_times": self.retry_times,
        }
        return result_json

    def from_json(self, json_obj):
        self.recovery_enable = json_obj["recovery_enable"]
        self.recovery_interval = json_obj["recovery_interval"]
        self.retry_times = json_obj["retry_times"]


class ScheduleInfo(object):
    def __init__(self):
        self.is_restart = False  # type: bool
        self.jobnavi_task_type = None  # type: bool

        self.geog_area_code = None  # type: str
        self.cluster_id = None  # type: str

        self.count_freq = None  # type: int
        self.schedule_period = None  # type: str
        self.start_time = None  # type: int

    def to_json(self):
        result_json = {
            "jobnavi_task_type": self.jobnavi_task_type,
            "is_restart": self.is_restart,
            "geog_area_code": self.geog_area_code,
            "cluster_id": self.cluster_id,
            "count_freq": self.count_freq,
            "schedule_period": self.schedule_period,
            "start_time": self.start_time,
        }
        return result_json

    def from_json(self, json_obj):
        self.is_restart = json_obj["is_restart"]
        self.geog_area_code = json_obj["geog_area_code"]
        self.cluster_id = json_obj["cluster_id"]
        self.count_freq = json_obj["count_freq"]
        self.schedule_period = json_obj["schedule_period"]
        self.start_time = json_obj["start_time"]
        self.jobnavi_task_type = json_obj["jobnavi_task_type"]


class PeriodicBatchJobParams(object):
    def __init__(self):
        self.job_id = None  # type: str
        self.job_name = None  # type: str
        self.bk_username = None  # type: str

        self.code_version = None  # type: str

        self.batch_type = None  # type: str
        self.processor_type = None  # type: str

        self.schedule_info = ScheduleInfo()  # type: ScheduleInfo

        self.resource = Resource()  # type: Resource
        self.recovery_info = RecoveryInfo()  # type: RecoveryInfo

        self.deploy_config = DeployConfig()  # type: DeployConfig

        self.source_nodes = {}  # type: dict[str, SourceNode]
        self.transform_nodes = {}  # type: dict[str, TransformNode]
        self.sink_nodes = {}  # type: dict[str, SinkNode]

        self.udf = []  # type: list[dict]

        self.implement_type = None  # type: str
        self.programming_language = None  # type: str

    def save_to_processing_batch_job_db(self, force_to_save=False):
        if force_to_save and ProcessingBatchJobHandler.is_proc_batch_job_exist(self.job_id):
            ProcessingBatchJobHandler.delete_proc_batch_job_v2(self.job_id)

        self.__valid_db_text_length()
        ProcessingBatchJobHandler.save_proc_batch_job_v2(
            batch_id=self.job_id,
            processor_type=self.transform_nodes[self.job_id].processor_type,
            processor_logic=json.dumps(self.transform_nodes[self.job_id].processor_logic),
            schedule_time=self.schedule_info.start_time,
            schedule_period=self.schedule_info.schedule_period,
            count_freq=self.schedule_info.count_freq,
            delay=0,
            submit_args=json.dumps(self.to_json()),
            storage_args="{}",
            running_version=self.code_version,
            jobserver_config=json.dumps(self.schedule_info.to_json()),
            cluster_group=self.resource.cluster_group,
            cluster_name=self.resource.queue_name,
            deploy_mode="{}",
            deploy_config=json.dumps(self.deploy_config.to_db_json()),
            active=1,
            created_by=self.bk_username,
            implement_type=self.implement_type,
            programming_language=self.programming_language,
        )

    def update_to_processing_batch_job_db(self):
        self.__valid_db_text_length()
        ProcessingBatchJobHandler.save_proc_batch_job_v2(
            batch_id=self.job_id,
            processor_type=self.transform_nodes[self.job_id].processor_type,
            processor_logic=json.dumps(self.transform_nodes[self.job_id].processor_logic),
            schedule_time=self.schedule_info.start_time,
            schedule_period=self.schedule_info.schedule_period,
            count_freq=self.schedule_info.count_freq,
            delay=0,
            submit_args=json.dumps(self.to_json()),
            storage_args="{}",
            running_version=self.code_version,
            jobserver_config=json.dumps(self.schedule_info.to_json()),
            cluster_group=self.resource.cluster_group,
            cluster_name=self.resource.queue_name,
            deploy_mode="{}",
            deploy_config=json.dumps(self.deploy_config.to_db_json()),
            active=1,
            created_by=self.bk_username,
            implement_type=self.implement_type,
            programming_language=self.programming_language,
        )

    def __valid_db_text_length(self):
        if len(json.dumps(self.transform_nodes[self.job_id].processor_logic)) > settings.SPARK_SQL_TEXT_MAX_LENGTH:
            raise BatchIllegalArgumentError("SQL exceeded max length, please check your sql")

        if len(self.to_json()) > settings.SPARK_SQL_TEXT_MAX_LENGTH:
            raise BatchIllegalArgumentError("SQL exceeded max length, please check your sql")

    def from_processing_batch_job_db(self, batch_id):
        batch_job_obj = ProcessingBatchJobHandler.get_proc_batch_job(batch_id)
        self.from_processing_batch_job_db_obj(batch_job_obj)

    def from_processing_batch_job_db_obj(self, batch_job_obj):
        """
        :type batch_job_obj: dataflow.batch.models.bkdata_flow.ProcessingBatchJob
        """
        submit_args = json.loads(batch_job_obj.submit_args)
        self.from_json(submit_args)
        self.implement_type = batch_job_obj.implement_type
        self.programming_language = batch_job_obj.programming_language

    def content_equal(self, other):
        if isinstance(other, PeriodicBatchJobParams):
            this_sort_json_str = json.dumps(self.to_json(), sort_keys=True)
            other_sort_json_str = json.dumps(other.to_json(), sort_keys=True)
            result_equal = (
                (this_sort_json_str == other_sort_json_str)
                and (self.implement_type == other.implement_type)
                and (self.programming_language == other.programming_language)
            )

            return result_equal
        return False

    def to_json(self):
        result_json = {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "bk_username": self.bk_username,
            "batch_type": self.batch_type,
            "schedule_info": self.schedule_info.to_json(),
            "recovery_info": self.recovery_info.to_json(),
            "deploy_config": self.deploy_config.to_db_json(),
            "resource": self.resource.to_json(),
            "nodes": {"source": {}, "transform": {}, "sink": {}},
            "udf": self.udf,
        }

        for source_node_id in self.source_nodes:
            result_json["nodes"]["source"][source_node_id] = self.source_nodes[source_node_id].to_json()

        for transform_node_id in self.transform_nodes:
            result_json["nodes"]["transform"][transform_node_id] = self.transform_nodes[transform_node_id].to_json()

        for sink_node_id in self.sink_nodes:
            result_json["nodes"]["sink"][sink_node_id] = self.sink_nodes[sink_node_id].to_json()

        return result_json

    def from_json(self, json_obj):
        self.job_id = json_obj["job_id"]
        self.job_name = json_obj["job_name"]
        self.bk_username = json_obj["bk_username"]
        self.batch_type = json_obj["batch_type"]
        self.schedule_info = ScheduleInfo()
        self.schedule_info.from_json(json_obj["schedule_info"])
        self.recovery_info = RecoveryInfo()
        self.recovery_info.from_json(json_obj["recovery_info"])
        self.resource = Resource()
        self.resource.from_json(json_obj["resource"])
        self.udf = json_obj["udf"]

        if "deploy_config" in json_obj:
            self.deploy_config = DeployConfig()
            self.deploy_config.from_db_json(json_obj["deploy_config"])

        for source_node_id in json_obj["nodes"]["source"]:
            self.source_nodes[source_node_id] = SourceNode()
            self.source_nodes[source_node_id].from_json(json_obj["nodes"]["source"][source_node_id])

        for transform_node_id in json_obj["nodes"]["transform"]:
            self.transform_nodes[transform_node_id] = TransformNode()
            self.transform_nodes[transform_node_id].from_json(json_obj["nodes"]["transform"][transform_node_id])

        for sink_node_id in json_obj["nodes"]["sink"]:
            self.sink_nodes[sink_node_id] = SinkNode()
            self.sink_nodes[sink_node_id].from_json(json_obj["nodes"]["sink"][sink_node_id])
