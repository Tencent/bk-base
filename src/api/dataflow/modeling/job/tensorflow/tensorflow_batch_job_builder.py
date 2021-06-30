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

from copy import deepcopy

from conf.dataapi_settings import MLSQL_MODEL_INFO_HDFS_USER

from dataflow.batch.periodic.param_info.builder.periodic_batch_job_builder import PeriodicBatchJobBuilder
from dataflow.modeling.job.tensorflow.tensorflow_batch_job_params import (
    ModelSinkNode,
    ModelSourceNode,
    TensorFlowBatchJobParams,
    TensorFlowSourceNode,
    TransformNode,
)
from dataflow.modeling.job.tensorflow.tensorflow_job_info_params import TensorFlowJobInfoParams
from dataflow.modeling.processing.tensorflow.tensorflow_batch_info_params import (
    InputModelConfigParams,
    OutputModelConfigParam,
    TensorFlowBatchInfoParams,
)
from dataflow.pizza_settings import BASE_BATCH_URL, BASE_COMPONENT_URL
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class TensorFlowBatchJobBuilder(PeriodicBatchJobBuilder):
    def __init__(self):
        self.job_info_obj = TensorFlowJobInfoParams()
        self.batch_info_obj = TensorFlowBatchInfoParams()
        self.api_params = None
        self.job_id = None
        self.periodic_batch_job_obj = None

    def build_from_flow_start_api(self, job_id, args):
        self.api_params = args
        self.job_id = job_id
        self.job_info_obj.from_processing_job_info_db(job_id)
        self.batch_info_obj.from_processing_batch_info_db(self.job_info_obj.job_config.processings[0])
        self.periodic_batch_job_obj = TensorFlowBatchJobParams()
        self.build_batch_job()
        return self.periodic_batch_job_obj

    def build_schedule_info(self):
        self.periodic_batch_job_obj.schedule_info.is_restart = self.api_params["is_restart"]

        self.periodic_batch_job_obj.schedule_info.geog_area_code = self.job_info_obj.jobserver_config.geog_area_code
        self.periodic_batch_job_obj.schedule_info.cluster_id = self.job_info_obj.jobserver_config.cluster_id

        self.periodic_batch_job_obj.schedule_info.count_freq = self.batch_info_obj.count_freq
        self.periodic_batch_job_obj.schedule_info.schedule_period = self.batch_info_obj.schedule_period
        self.periodic_batch_job_obj.schedule_info.start_time = self.batch_info_obj.start_time

        self.periodic_batch_job_obj.schedule_info.jobnavi_task_type = self.batch_info_obj.batch_type

    def build_source_nodes(self):
        # 重写source方法
        # 生成源表
        for input_table in self.batch_info_obj.input_result_tables:
            periodic_source_node = self.build_source_node_from_input_table(input_table)
            tf_source_node = TensorFlowSourceNode(
                periodic_source_node,
                {
                    "feature_shape": input_table.feature_shape,
                    "label_shape": input_table.label_shape,
                    "parent_node_url": BASE_BATCH_URL,
                },
            )
            self.periodic_batch_job_obj.source_nodes[input_table.result_table_id] = tf_source_node

        # 生成源模型
        for input_model in self.batch_info_obj.input_models:
            self.periodic_batch_job_obj.source_nodes[input_model.model_name] = self.__build_source_node_from_model(
                input_model
            )

    def __build_source_node_from_model(self, input_model):
        model = ModelingHelper.get_model(input_model.model_name)
        input_model_info = {
            "id": model["id"],
            "name": model["model_name"],
            "description": model["model_alias"],
            "type": "model",
            "input": {"type": "hdfs", "format": model["algorithm_name"], "path": model["storage"]["path"]},
        }
        model_source_node = ModelSourceNode()
        model_source_node.from_json(input_model_info)
        return model_source_node

    def build_sink_nodes(self):
        # 重写sink方法
        # 生成目标表
        for output_table in self.batch_info_obj.output_result_tables:
            self.periodic_batch_job_obj.sink_nodes[
                output_table.result_table_id
            ] = self.build_sink_node_from_output_table(output_table)
        #  生成目标模型
        for output_model in self.batch_info_obj.output_models:
            self.periodic_batch_job_obj.sink_nodes[output_model.model_name] = self.__build_sink_node_from_output_model(
                output_model
            )

    def build_transform_nodes(self):
        tmp_transform_node = TransformNode()
        tmp_transform_node.id = self.batch_info_obj.processing_id
        tmp_transform_node.name = self.batch_info_obj.processing_id
        tmp_transform_node.processor_logic = self.batch_info_obj.processor_logic
        tmp_transform_node.processor_type = self.batch_info_obj.processor_type
        processor_logic = self.batch_info_obj.processor_logic
        user_script = processor_logic["script"]
        user_args_dict = self.map_args_to_dic(processor_logic["user_args"])
        tmp_transform_node.processor = {
            "user_main_module": user_script[0 : len(user_script) - 3],
            "args": user_args_dict,
            "type": "untrained-run",
        }
        self.periodic_batch_job_obj.transform_nodes[tmp_transform_node.id] = tmp_transform_node

    def __build_sink_node_from_output_model(self, output_model):
        hdfs_cluster = StorekitHelper.get_default_storage(
            "hdfs", self.periodic_batch_job_obj.schedule_info.geog_area_code
        )["cluster_group"]
        model = ModelingHelper.get_model(output_model.model_name)
        output_model_info = {
            "id": model["id"],
            "name": model["model_name"],
            "description": model["model_alias"],
            "type": "model",
            "output": {
                "type": "hdfs",
                "format": model["algorithm_name"],
                "path": model["storage"]["path"],
                "cluster_group": hdfs_cluster,
                "hdfs_user": MLSQL_MODEL_INFO_HDFS_USER,
                "component_url": BASE_COMPONENT_URL,
            },
        }
        model_sink_node = ModelSinkNode()
        model_sink_node.from_json(output_model_info)
        return model_sink_node

    def map_args_to_dic(self, user_args):
        user_args_dict = {}
        for arg_item in user_args.split():
            arg_name = arg_item.split("=")[0]
            arg_value = arg_item.split("=")[1]
            user_args_dict[arg_name] = arg_value
        return user_args_dict

    def refresh_transform_node(self, transform_node):
        transform_node.processor_logic = self.batch_info_obj.processor_logic
        user_script = transform_node.processor_logic["script"]
        # 将k=v格式的参数调整为dict
        user_args_dict = self.map_args_to_dic(transform_node.processor_logic["user_args"])
        transform_node.processor = {
            "user_main_module": user_script[0 : len(user_script) - 3],
            "args": user_args_dict,
            "type": "untrained-run",
        }
        return transform_node

    def copy_and_refresh_batch_job_params(self, periodic_batch_job_obj):
        """
        This is used to refresh runtime params by calling meta api, storekit api and modeling api
        :param periodic_batch_job_obj:
        :return new batch job object
        """
        self.periodic_batch_job_obj = deepcopy(periodic_batch_job_obj)
        for source_node_id in self.periodic_batch_job_obj.source_nodes:
            source_node = self.periodic_batch_job_obj.source_nodes[source_node_id]
            if hasattr(source_node, "type") and source_node.type == "model":
                input_model = InputModelConfigParams()
                input_model.model_name = source_node.name
                self.periodic_batch_job_obj.source_nodes[source_node_id] = self.__build_source_node_from_model(
                    input_model
                )
            else:
                self.periodic_batch_job_obj.source_nodes[source_node_id] = self.refresh_source_node_with_extra_info(
                    source_node
                )

        for sink_node_id in self.periodic_batch_job_obj.sink_nodes:
            sink_node = self.periodic_batch_job_obj.sink_nodes[sink_node_id]
            if hasattr(sink_node, "type") and sink_node.type == "model":
                output_model = OutputModelConfigParam()
                output_model.model_name = sink_node.name
                self.periodic_batch_job_obj.sink_nodes[sink_node_id] = self.__build_sink_node_from_output_model(
                    output_model
                )
            else:
                self.periodic_batch_job_obj.sink_nodes[sink_node_id] = self.refresh_sink_node_with_extra_info(sink_node)
        for _ in self.periodic_batch_job_obj.transform_nodes:
            # 重新build transform节点,需要从db中获取最新的transform信息
            self.job_info_obj.from_processing_job_info_db(self.periodic_batch_job_obj.job_id)
            self.batch_info_obj.from_processing_batch_info_db(self.job_info_obj.job_config.processings[0])
            self.build_transform_nodes()
        return self.periodic_batch_job_obj
