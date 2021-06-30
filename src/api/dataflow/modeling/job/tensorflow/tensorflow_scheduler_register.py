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

from dataflow import pizza_settings
from dataflow.batch.api.api_helper import DatabusHelper
from dataflow.batch.periodic.param_info.periodic_batch_job_params import SourceNode
from dataflow.batch.periodic.scheduler.periodic_scheduler_register import PeriodicSchedulerRegister
from dataflow.modeling.job.tensorflow.tensorflow_batch_job_params import ModelSinkNode
from dataflow.pizza_settings import BASE_DATAFLOW_URL, MLSQL_NODE_LABEL
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class TensorFlowSchedulerRegister(PeriodicSchedulerRegister):
    def __init__(self, periodic_batch_job_params_obj):
        super(TensorFlowSchedulerRegister, self).__init__(periodic_batch_job_params_obj)
        self.type_id = "tensorflow"

    def register_jobnavi(self):
        if self.periodic_batch_job_params_obj.schedule_info.schedule_period == "hour":
            period_unit = "H"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "day":
            period_unit = "d"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "week":
            period_unit = "W"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "month":
            period_unit = "M"
        else:
            period_unit = ""

        period = {
            "timezone": pizza_settings.TIME_ZONE,
            "cron_expression": "",
            "frequency": self.periodic_batch_job_params_obj.schedule_info.count_freq,
            "period_unit": period_unit,
            "first_schedule_time": self.get_start_time(),
            "delay": "0H",
        }
        deploy_config = self.periodic_batch_job_params_obj.deploy_config.user_engine_conf
        if "worker" not in deploy_config:
            deploy_config["worker"] = pizza_settings.TENSORFLOW_K8S_WORKER_NUMBER
        if "cpu" not in deploy_config:
            deploy_config["cpu"] = pizza_settings.TENSORFLOW_K8S_WORKER_CPU
        if "memory" not in deploy_config:
            deploy_config["memory"] = pizza_settings.TENSORFLOW_K8S_WORKER_MEMORY
        extra_info = {
            "job_id": self.processing_id,
            "job_type": self.type_id,
            "run_mode": pizza_settings.RUN_MODE,
            "properties": {
                "namespace": pizza_settings.TENSORFLOW_NAMESPACE,
                "cluster.url": pizza_settings.TENSORFLOW_K8S_CLUSTER_URL,
                "api.token": pizza_settings.TENSORFLOW_K8S_API_TOKEN,
                "image.url": pizza_settings.TENSORFLOW_K8S_IMAGE_URL,
                "command.list": [
                    "sh",
                    "-x",
                    "/app/main.sh",
                    self.processing_id,
                    BASE_DATAFLOW_URL,
                    pizza_settings.RUN_MODE.lower(),
                    self.type_id,
                    self.__get_transform_script(),
                ],
            },
            "api_url": {"base_dataflow_url": BASE_DATAFLOW_URL},
        }

        logger.info(self.periodic_batch_job_params_obj.to_json())
        jobnavi_args = {
            "schedule_id": self.processing_id,
            "description": " Project {} submit by {}".format(self.processing_id, self.bk_username),
            "type_id": self.type_id,
            "period": period,
            "parents": self.get_parents(),
            "execute_before_now": self.is_restart,
            "active": False,
            "node_label": self.get_jobnavi_node_label(),
            "data_time_offset": self.get_data_time_offset(),
            "recovery": {
                "enable": self.periodic_batch_job_params_obj.recovery_info.recovery_enable,
                "interval_time": self.periodic_batch_job_params_obj.recovery_info.recovery_interval,
                "retry_times": self.periodic_batch_job_params_obj.recovery_info.retry_times,
            },
            "extra_info": str(json.dumps(extra_info)),
        }

        if self.existed_schedule_info:
            logger.info(jobnavi_args)
            self.jobnavi.update_schedule_info(jobnavi_args)
        else:
            logger.info(jobnavi_args)
            jobnavi_args["execute_before_now"] = False
            self.jobnavi.create_schedule_info(jobnavi_args)

        self.jobnavi.start_schedule(self.processing_id)

    def get_jobnavi_node_label(self):
        return MLSQL_NODE_LABEL

    def __get_transform_script(self):
        transform_node = self.periodic_batch_job_params_obj.transform_nodes[self.processing_id]
        return transform_node.processor_logic["user_package"]

    def get_parents(self):
        parents_args = []
        for source_node_id in self.periodic_batch_job_params_obj.source_nodes:
            source_node = self.periodic_batch_job_params_obj.source_nodes[source_node_id]
            if isinstance(source_node, SourceNode):
                result_table_meta = ResultTableHelper.get_result_table(source_node.id, related="data_processing")
                source_type = result_table_meta["processing_type"]
                if (
                    source_type == "batch"
                    and not self.is_tdw_data_source(self.periodic_batch_job_params_obj.batch_type, result_table_meta)
                ) or self.is_model_serve_mode_offline(source_node.id):
                    parents_args.append(self.handle_batch_upstream(source_node))
                elif DatabusHelper.is_rt_batch_import(source_node.id):
                    self.handle_import_tdw_to_hdfs_upstream(source_node)
                    parents_args.append(self.handle_batch_upstream(source_node))
            else:
                # parents内暂时不考虑模型
                pass
        return parents_args

    def get_data_time_offset(self):
        # todo:这里的逻辑是取任意一个输出计算data_time_offset即可，因为所有的输出值都是一样的，但实际不这个值应该是每个输出都不一样的
        # 目前的配置内，data_time_offset相关的配置未具体到某个输出，而是每个节点内统一的配置，
        # 同时多个data_time_offset在jobnavi侧该如何支持多个
        # 也需要进一步考虑，因此目前实现下仅取其中一个
        tmp_output_id = None
        for output_table_id in self.periodic_batch_job_params_obj.sink_nodes:
            sink_node_obj = self.periodic_batch_job_params_obj.sink_nodes[output_table_id]
            if isinstance(sink_node_obj, ModelSinkNode):
                continue
            tmp_output_id = output_table_id
            break
        if tmp_output_id:
            return self.periodic_batch_job_params_obj.sink_nodes[tmp_output_id].data_time_offset
        else:
            # 没有输出表，返回默认值
            return "0H"
