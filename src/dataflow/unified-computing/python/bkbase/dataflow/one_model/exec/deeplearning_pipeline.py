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

import importlib
import json

from bkbase.dataflow.core.exec.pipeline import Pipeline
from bkbase.dataflow.metrics.util.exceptions import MetricsVerifyException
from bkbase.dataflow.one_model.conf import deeplearning_conf
from bkbase.dataflow.one_model.exception.tensorflow_exception import (
    TensorFlowReportException,
)
from bkbase.dataflow.one_model.metric.monitor_handler import MonitorHandler
from bkbase.dataflow.one_model.topo.deeplearning_source_node import (
    ModelIcebergResultSetSourceNode,
)
from bkbase.dataflow.one_model.utils.deeplearning_constant import NodeType, ProcessType
from bkbase.dataflow.one_model.utils.deeplearning_logger import (
    logger as deeplearning_logger,
)


class DeepLearningPipeline(Pipeline):
    def __init__(self, topology):
        super().__init__(topology)
        self.monitors = {}
        if deeplearning_conf.ENABLE_MONITOR:
            self.enable_monitor()
        self.dataframe_dict = {}
        self.model_dict = {}

    def source(self):
        deeplearning_logger.info("Pipeline source start")
        self.__create_source(self.topology.source_nodes)

    def sink(self):
        deeplearning_logger.info("Pipeline sink start")
        self.__create_sink(self.topology.sink_nodes, self.topology.source_nodes)

    def transform(self):
        deeplearning_logger.info("Pipeline transform start")
        return self.__create_transform(self.topology.transform_nodes)

    def submit(self):
        try:
            self.source()
            deeplearning_logger.info("source_data_dict: {}".format(self.dataframe_dict))
            deeplearning_logger.info("model_dict:{}".format(self.model_dict))
            transform_result = self.transform()
            deeplearning_logger.info("transform_dict: {}".format(transform_result))
            self.sink()
            self.send_monitor()
            deeplearning_logger.info("pipeline submit finished")
        except Exception as e:
            self.send_monitor()
            raise e

    def __create_source(self, source_nodes):
        for node in source_nodes:
            source = source_nodes[node].create_source()
            node_type = source_nodes[node].type
            if node_type == NodeType.MODEL.value:
                self.model_dict[node] = source
            else:
                self.dataframe_dict[node] = source

    def __create_sink(self, sink_nodes, source_nodes):
        deeplearning_logger.info(sink_nodes)
        for node in sink_nodes:
            node_type = sink_nodes[node].type
            if node_type == NodeType.MODEL.value:
                if node in self.model_dict:
                    sink_nodes[node].create_sink(self.model_dict[node])
            else:
                # 将源dataset与目标dataset结合起来作为输出
                target_dataset = self.dataframe_dict[node]
                sink_nodes[node].create_sink(target_dataset)

    def __create_transform(self, transform_nodes):
        for node_id in transform_nodes:
            process_type = transform_nodes[node_id].process_type
            transform_module = importlib.import_module(transform_nodes[node_id].user_main_module)
            if process_type == ProcessType.UNTRAINED_RUN.value:
                result_dict = transform_module.transform(
                    self.dataframe_dict, self.model_dict, transform_nodes[node_id].user_args
                )
            elif process_type == ProcessType.TRAINED_RUN.value:
                result_dict = transform_module.predict(self.dataframe_dict, self.model_dict)
            elif process_type == ProcessType.TRAIN.value:
                result_dict = transform_module.train(self.dataframe_dict, self.model_dict)
            else:
                raise Exception("unsuppoted operation:{}".format(process_type))
            return result_dict

    def send_monitor(self):
        try:
            if deeplearning_conf.ENABLE_MONITOR:
                deeplearning_logger.info("Pipeline send monitor")
                input_count_info = self.get_input_count_info()
                for monitor in self.monitors:
                    self.monitors[monitor].report(input_count_info)
            else:
                deeplearning_logger.info("Monitor disabled, won't send report data")
        except (ValueError, TypeError, AssertionError, MetricsVerifyException, TensorFlowReportException) as e:
            deeplearning_logger.exception(e)
            deeplearning_logger.error(e)

    def get_input_count_info(self):
        input_info = {}
        source_nodes = self.topology.source_nodes
        try:
            for source_id in source_nodes:
                source_obj = source_nodes[source_id]
                if isinstance(source_obj, ModelIcebergResultSetSourceNode):
                    # rt表的时候才需要上报
                    start = source_obj.input["time_range_list"][0]["start_time"]
                    end = source_obj.input["time_range_list"][0]["end_time"]
                    count = MonitorHandler.get_rt_count(
                        start, end, source_obj.input["storage_conf"]["storekit_hdfs_conf"]
                    )
                    input_info[source_id] = {"count": count, "start": start, "end": end}
            deeplearning_logger.info("input info:" + json.dumps(input_info))
            return input_info
        except Exception as e:
            deeplearning_logger.error(e)
            raise TensorFlowReportException("Get input information error:%s" % e)

    def enable_monitor(self):
        for sink_node in self.topology.sink_nodes:
            sink_obj = self.topology.sink_nodes[sink_node]
            # if isinstance(sink_obj, ModelIcebergResultSetSinkNode):
            if sink_obj.fields:
                # fields不为空，说明为表
                self.monitors[sink_node] = MonitorHandler(self.topology, sink_node)
