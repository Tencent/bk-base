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
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.job.jobnavi_register_v1 import JobnaviRegisterV1
from dataflow.modeling.job.job_config_driver import generate_workflow_config
from dataflow.shared.log import modeling_logger as logger


class ModelingJobNaviRegister(JobnaviRegisterV1):
    def __init__(self, job_id, created_by, is_restart=False):
        self.job_id = job_id
        modeling_job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
        # 调整参数的格式，使其可以适应当前的计算逻辑
        modleing_job_info_config = json.loads(modeling_job_info.job_config)
        modeling_submit_args = json.loads(modleing_job_info_config["submit_args"])
        new_submit_args = ModelingJobNaviRegister.__transform_new_to_old_submit_args(modeling_submit_args)
        modleing_job_info_config["submit_args"] = json.dumps(new_submit_args)
        modeling_job_info.job_config = json.dumps(modleing_job_info_config)
        super(ModelingJobNaviRegister, self).__init__(job_id, modeling_job_info, created_by, is_restart)

    @classmethod
    def __transform_new_to_old_submit_args(cls, new_submit_args):
        if "node" not in new_submit_args:
            # 已经是旧格式
            return new_submit_args
        node_info = new_submit_args["node"]
        dependence_info = new_submit_args["dependence"]
        schedule_info = node_info["schedule_info"]
        output_info = node_info["output"]
        node_extra_info = node_info["extra_info"]
        old_submit_args = {}

        # extra_info放入顶层，包括：static_data, batch_type与cluster
        old_submit_args.update(node_extra_info)

        # schedule_info放入顶层，包括:count_freq,schedule_period,delay,data_start,data_end与advanced
        old_submit_args["count_freq"] = schedule_info["count_freq"]
        old_submit_args["data_end"] = schedule_info["data_end"]
        old_submit_args["schedule_period"] = schedule_info["schedule_period"]
        old_submit_args["data_start"] = schedule_info["data_start"]
        old_submit_args["delay"] = schedule_info["delay"]
        old_submit_args["advanced"] = schedule_info["advanced"]

        # dependence信息放入result_tables，注意：accumulate的值是位于顶层的，但是却来源于dependence内的单个
        # 元素：为兼容目前逻辑，只要有一个是accumulate,其值即为true
        old_submit_args["accumulate"] = False
        old_submit_args["result_tables"] = {}
        for parent_table_name in dependence_info:
            parent_table_info = dependence_info[parent_table_name]
            old_submit_args["result_tables"][parent_table_name] = parent_table_info
            if parent_table_info["window_type"] == "accumulate":
                old_submit_args["accumulate"] = True

        # output信息写入output_schema与outputs，注意outputs是一个list指明结果表的data_set_id与data_set_type
        # 注意：outputs位于顶层
        old_submit_args["outputs"] = []
        old_submit_args["output_schema"] = {}
        logger.info("output:" + json.dumps(output_info))
        for result_table_name in output_info:
            result_table_info = output_info[result_table_name]
            fields = result_table_info["fields"]
            old_submit_args["output_schema"][result_table_name] = fields
            if "data_set_id" in result_table_info and "data_set_type" in result_table_info:
                output_info = {
                    "data_set_id": result_table_info["data_set_id"],
                    "data_set_type": result_table_info["data_set_type"],
                }
            else:
                output_info = {
                    "data_set_id": result_table_name,
                    "data_set_type": "result_table",
                }
            old_submit_args["outputs"].append(output_info)
        return old_submit_args

    def get_type_id(self):
        # 重写获取type_id的过程
        return "workflow"

    def create_jobnavi_extra_info(self):
        # 重新定义获取extra_info的函数
        # 在使用workflow提交的任务，extra_info内需要包含整个workflow的配置
        extra_info = generate_workflow_config(self.job_id)
        # 然后需要添加额外的信息
        extra_info["geog_area_code"] = self.geog_area_code
        return extra_info

    def get_jobnavi_node_label(self):
        # 重新获取node_label
        return pizza_settings.MLSQL_NODE_LABEL
