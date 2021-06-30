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

from dataflow.batch.periodic.param_info.periodic_batch_job_params import (
    PeriodicBatchJobParams,
    RecoveryInfo,
    Resource,
    ScheduleInfo,
    SinkNode,
    SourceNode,
)
from dataflow.batch.periodic.param_info.periodic_job_info_params import DeployConfig


class ModelSourceNode(object):
    def __init__(self):
        self.id = None
        self.name = None
        self.type = "model"
        self.input = None
        self.description = None

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "input": self.input,
            "description": self.description,
        }
        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.type = json_obj["type"]
        self.input = json_obj["input"]
        self.description = json_obj["description"]


class ModelSinkNode(object):
    def __init__(self):
        self.id = None
        self.name = None
        self.type = "model"
        self.output = None
        self.description = None

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "output": self.output,
            "description": self.description,
        }
        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.type = json_obj["type"]
        self.output = json_obj["output"]
        self.description = json_obj["description"]


class TransformNode(object):
    def __init__(self):
        self.id = None
        self.name = None
        self.processor = {}
        self.processor_logic = {}
        self.processor_type = None

    def to_json(self):
        result_json = {
            "id": self.id,
            "name": self.name,
            "processor": self.processor,
            "processor_logic": self.processor_logic,
            "processor_type": self.processor_type,
        }
        return result_json

    def from_json(self, json_obj):
        self.id = json_obj["id"]
        self.name = json_obj["name"]
        self.processor = json_obj["processor"]
        self.processor_logic = json_obj["processor_logic"]
        self.processor_type = json_obj["processor_type"]


class TensorFlowSourceNode(object):
    def __init__(self, source_node, extra_info={}):
        # 普通sourceNode初始化
        self.source_node = source_node
        self.id = source_node.id
        self.name = source_node.name
        self.fields = source_node.fields
        self.window_type = source_node.window_type
        self.dependency_rule = source_node.dependency_rule
        self.window_offset = source_node.window_offset
        self.window_size = source_node.window_size
        self.window_start_offset = source_node.window_start_offset
        self.window_end_offset = source_node.window_end_offset
        self.accumulate_start_time = source_node.accumulate_start_time
        self.storage_type = source_node.storage_type
        self.storage_conf = source_node.storage_conf
        self.is_managed = source_node.is_managed
        self.self_dependency_mode = source_node.self_dependency_mode
        self.role = source_node.role
        self.feature_shape = extra_info["feature_shape"] if "feature_shape" in extra_info else 0
        self.label_shape = extra_info["label_shape"] if "label_shape" in extra_info else 0
        self.parent_node_url = extra_info["parent_node_url"] if "parent_node_url" in extra_info else None

    def to_json(self):
        result_json = self.source_node.to_json()
        result_json["feature_shape"] = self.feature_shape
        result_json["label_shape"] = self.label_shape
        result_json["parent_node_url"] = self.parent_node_url
        return result_json

    def from_json(self, json_obj):
        source_node = SourceNode()
        source_node.from_json(json_obj)
        self.__init__(source_node, json_obj)


class TensorFlowBatchJobParams(PeriodicBatchJobParams):
    def __init__(self):
        super(TensorFlowBatchJobParams, self).__init__()

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
            source_node_obj = json_obj["nodes"]["source"][source_node_id]
            if "type" in source_node_obj and source_node_obj["type"] == "model":
                self.source_nodes[source_node_id] = ModelSourceNode()
                self.source_nodes[source_node_id].from_json(source_node_obj)
            else:
                periodic_source_node = SourceNode()
                periodic_source_node.from_json(source_node_obj)
                tf_source_node = TensorFlowSourceNode(
                    periodic_source_node,
                    {
                        "feature_shape": source_node_obj["feature_shape"],
                        "label_shape": source_node_obj["label_shape"],
                        "parent_node_url": source_node_obj["parent_node_url"],
                    },
                )
                self.source_nodes[source_node_id] = tf_source_node

        for transform_node_id in json_obj["nodes"]["transform"]:
            self.transform_nodes[transform_node_id] = TransformNode()
            self.transform_nodes[transform_node_id].from_json(json_obj["nodes"]["transform"][transform_node_id])

        for sink_node_id in json_obj["nodes"]["sink"]:
            sink_node_obj = json_obj["nodes"]["sink"][sink_node_id]
            if "type" in sink_node_obj and sink_node_obj["type"] == "model":
                self.sink_nodes[sink_node_id] = ModelSinkNode()
            else:
                self.sink_nodes[sink_node_id] = SinkNode()

            self.sink_nodes[sink_node_id].from_json(sink_node_obj)
