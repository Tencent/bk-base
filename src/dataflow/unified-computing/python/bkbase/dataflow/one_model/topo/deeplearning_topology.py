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

from abc import ABCMeta

from bkbase.dataflow.one_model.topo.deeplearning_transform_node import (
    ModelTransformerNodeBuilder,
)
from bkbase.dataflow.one_model.topo.model_builder_factory import (
    ModelSinkBuilderFactory,
    ModelSourceBuilderFactory,
)
from bkbase.dataflow.one_model.utils.deeplearning_constant import NodeType, RunMode


class Topology(metaclass=ABCMeta):
    def __init__(self, builder):
        self.job_id = builder.job_id
        self.job_name = builder.job_name
        self.job_type = builder.job_type
        self.run_mode = builder.run_mode
        self.source_nodes = {}
        self.sink_nodes = {}
        self.transform_nodes = {}


class DeepLearningTopology(Topology):
    def __init__(self, builder):
        super().__init__(builder)
        self.time_zone = builder.time_zone
        self.queue_name = builder.resource["queue_name"]
        self.cluster_group = builder.resource["cluster_group"]
        self.is_debug = builder.is_debug
        if self.is_debug:
            self.debug_config = builder.debug_config
        else:
            self.metric_config = builder.metric_config
        builder.build_nodes()
        self.source_nodes = builder.source_nodes
        self.transform_nodes = builder.transform_nodes
        self.sink_nodes = builder.sink_nodes


class Builder(metaclass=ABCMeta):
    def __init__(self, params):
        self.job_id = params["job_id"]
        self.job_name = params["job_name"]
        self.version = params.get("version", "v1")
        self.run_mode = params["run_mode"].lower()
        self.is_debug = True if self.run_mode == RunMode.DEBUG.value else False

    def build(self):
        pass


class DeepLearningTopologyBuilder(Builder):
    def __init__(self, params):
        super().__init__(params)
        self.time_zone = params["time_zone"]
        self.nodes = params["nodes"]
        self.resource = params["resource"]
        self.schedule_time = params["schedule_time"]
        if self.version == "v1":
            self.job_type = params["job_type"]
            # self.dataflow_url = params.get('data_flow_url', '')
        else:
            self.job_type = params["batch_type"]

        if self.is_debug:
            debug_info = params["debug"]
            self.debug_config = {
                "debug_id": debug_info["debug_id"],
                "debug_url": debug_info["debug_rest_api_url"],
                "debug_exec_id": debug_info["debug_exec_id"],
            }
        else:
            if "metric" in params:
                self.metric_config = {"metric_rest_api_url": params["metric"]["metric_rest_api_url"]}
            else:
                self.metric_config = {}

        self.source_nodes = {}
        self.sink_nodes = {}
        self.transform_nodes = {}

    def build_nodes(self):
        source_nodes = {}
        transform_nodes = {}
        sink_nodes = {}
        source_factory = ModelSourceBuilderFactory(self.schedule_time, self.job_type, self.is_debug, self.version)
        sink_factory = ModelSinkBuilderFactory(self.job_type, self.schedule_time, self.nodes["source"], self.version)
        user_main_module = None
        for node_type in self.nodes:
            for node_id in self.nodes[node_type]:
                if node_type == "source":
                    source_node = source_factory.get_builder(self.nodes[node_type][node_id]).build()
                    source_nodes[node_id] = source_node
                elif node_type == "transform":
                    builder = ModelTransformerNodeBuilder(self.nodes[node_type][node_id])
                    transform_node = builder.build()
                    transform_nodes[node_id] = transform_node
                    user_main_module = transform_node.user_main_module
                    # user_feature_shape = transform_node.feature_shape
                    # user_label_shape = transform_node.label_shape
                elif node_type == "sink":
                    sink_node = sink_factory.get_builder(self.nodes[node_type][node_id]).build()
                    sink_nodes[node_id] = sink_node
                else:
                    raise Exception("unsupported node type:{}".format(node_type))
        self.source_nodes = source_nodes

        for source_id in self.source_nodes:
            if self.source_nodes[source_id].type == NodeType.DATA.value:
                self.source_nodes[source_id].user_main_module = user_main_module

        self.transform_nodes = transform_nodes
        self.sink_nodes = sink_nodes

    def build(self):
        return DeepLearningTopology(self)
