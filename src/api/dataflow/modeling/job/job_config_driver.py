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

from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.modeling.settings import PARSED_TASK_TYPE, TABLE_TYPE
from dataflow.pizza_settings import BASE_FLOW_URL, MLSQL_NODE_LABEL
from dataflow.shared.log import modeling_logger as logger

SUB_TASK_PREFIX = "subtask_"


def generate_workflow_config(job_id):
    job_info_list = ProcessingJobInfoHandler.get_proc_job_info_by_prefix(job_id)
    job_id_list = []
    for job_info in job_info_list:
        job_id_list.append(job_info.job_id)
    # 排序处理，并且将第一个移至最后一个
    job_id_list = sorted(job_id_list)
    first_item = job_id_list.pop(0)
    job_id_list.append(first_item)
    logger.info(job_id_list)
    return generate_workflow_config_by_list(job_id_list, run_mode="product")


def generate_workflow_config_by_list(job_id_list, run_mode="product"):
    # 根据job及拓扑信息获取整个workflow的配置文件
    workflow_config = {"params": {}, "config": {}, "node": {}}
    url = BASE_FLOW_URL
    url = url.rstrip("/").rstrip("flow")

    for job_index in range(0, len(job_id_list)):
        job_id = job_id_list[job_index]
        # 目前workflow的限制，所有node的名称不能以数字开头，所以要加入prefix
        sub_task_name = "{}_{}".format(SUB_TASK_PREFIX, job_id)
        task_run_info = {
            "job_id": job_id,
            "job_type": "spark_mllib",
            "run_mode": run_mode,
            "api_url": {"base_dataflow_url": url},
            "jobnavi": {},
        }
        sub_task_info = {
            "node_type": "subtask",
            "type_id": "spark_mllib",
            "children": [],
            "subtask_info": json.dumps(task_run_info),
            "node_label": MLSQL_NODE_LABEL,
        }
        workflow_config["node"][sub_task_name] = sub_task_info
        child_jobid_list = []
        if job_index + 1 < len(job_id_list):
            child_jobid_list.append(job_id_list[job_index + 1])
        for child_job_id in child_jobid_list:
            child_sub_task_name = "{}_{}".format(SUB_TASK_PREFIX, child_job_id)
            state_name = "state_{}_{}".format(sub_task_name, child_sub_task_name)
            state_info = {
                "node_type": "state",
                "state_type": "depend_rule",
                "children": [child_sub_task_name],
                "state_content": {"all_finished": [sub_task_name]},
            }
            sub_task_info["children"].append(state_name)
            workflow_config["node"][state_name] = state_info
    return workflow_config


def generate_complete_config(model_config, input_info, output_info, window_info, model_params):
    # 我们得到的model_config里， 包括了header和一系列的transform
    # 我们需要替换：
    #  1. source里的输入内容（input,fields等）
    #  2. 第一个transform里的processor.args的内容
    #  3. 从最后一个transform里解析出sink的内容

    # 替换配置文件中的通配符
    model_config_string = json.dumps(model_config)
    model_config_string = model_config_string.replace("__TRANSFORM_ID__", output_info["name"])
    model_config_string = model_config_string.replace("__DATA_SOURCE_ID__", input_info["name"])
    new_model_config = json.loads(model_config_string)

    # todo:后续如果一个节点有多个Job，需要从这里进行循环，对每个Job的配置进行处理
    # todo:目前的处理是简单化的，因为没有拆分Job的逻辑
    # 替换source中的内容
    new_model_config["source"][input_info["name"]]["input"] = input_info["info"]
    new_model_config["source"][input_info["name"]]["fields"] = input_info["fields"]

    new_model_config["source"][input_info["name"]]["window"] = window_info

    # 获取第一个及最后一个transform
    head, tail = fetch_head_and_tail(new_model_config["transform"])
    logger.info("head:" + json.dumps(head))
    logger.info("tail:" + json.dumps(tail))

    # 替换第一个transform中的参数信息
    # 获取用户输入参数信息
    # 首先处理interpreter的信息
    if head["task_type"] == PARSED_TASK_TYPE.MLSQL_QUERY.value:
        head_interpreter = head["interpreter"]
        for interpreter_item in head_interpreter:
            interpreter_value_list = head_interpreter[interpreter_item]["value"]
            new_interpreter_value_list = []
            for item in interpreter_value_list:
                if item in model_params:
                    new_interpreter_value_list.append(model_params[item])
            head_interpreter[interpreter_item]["value"] = new_interpreter_value_list
        # 然后处理其它args
        processor = head["processor"]
        processor_args = processor["args"]
        for arg in processor_args:
            if arg in model_params and arg not in head_interpreter:
                # 注：在interpreter的情况已经在上面处理了
                processor_args[arg] = model_params[arg]
    else:
        processor = head["processor"]
        processor_args = processor["args"]
        select_list = ""
        for item in model_params:
            select_list = select_list + "{} as {},".format(model_params[item], item)
        # 去除最后的逗号
        select_list = select_list.rstrip(",")
        table_name = input_info["name"]
        table_name = table_name[table_name.find("_") + 1 : len(table_name)] + "_" + table_name[0 : table_name.find("_")]
        sub_query_sql = "(select {select_list} from {table})".format(select_list=select_list, table=table_name)
        logger.info("sub query sql:" + sub_query_sql)
        processor_args["sql"] = processor_args["format_sql"].replace("__DATA_SOURCE_INPUT_SQL__", sub_query_sql)

    # 然后再替换从源表中直接取得的fields
    head_transform_fields = head["fields"]
    for field in head_transform_fields:
        if field["origin"] and field["origin"] in model_params:
            field["origin"] = model_params[field["origin"]]

    # 生成sink的内容：使用output_fileds(其实也来自于最后一个transform的fields)
    sink_info = {
        "description": output_info["alias"],
        "fields": output_info["fields"],
        "type": "data",
        "id": output_info["name"],
        "name": output_info["name"],
        "output": {
            "type": "hdfs",
            "mode": "overwrite",
            "format": "parquet",
            "table_type": TABLE_TYPE.RESULT_TABLE.value,
        },
    }
    new_model_config["sink"] = {output_info["name"]: sink_info}
    return new_model_config


def fetch_head_and_tail(transform_config):
    """
    按transform中的parents排序，获取没有依赖以及不被别人依赖的transform
    @param transform_config:
    {
        'transform_1': {
            'parents':['transform_2']
        },
        'transform_2': {}
    }
    @return:
    [{transform_2}, {transform_1}]
    """
    # 记录所有transform之间的“内部依赖”，即不包括对这些transform之外的依赖
    transform_depdence_map = {}
    transform_id_map = {transform_config[transform_id]["id"]: transform_id for transform_id in transform_config}
    for transform_id in transform_config:
        if transform_id in transform_depdence_map:
            depdence_info = transform_depdence_map[transform_id]
        else:
            depdence_info = {"parents": set(), "sons": set()}
            transform_depdence_map[transform_id] = depdence_info
        # 以下部分逻辑有些绕，因为transform内的key与每个transform内的id是不统一的，两者需要有个对应关系，即变量transform_id_map
        parents = transform_config[transform_id]["parents"]
        for parent in parents:
            if parent in transform_id_map:
                # actual_transform_id 记录的是真实的id，即每个transform实例内的id字段
                actual_transform_id = parent
                # origin_transform_id 记录的是transform中，每个transform实体对应的key
                origin_transform_id = transform_id_map[actual_transform_id]

                # 首先更新当前节点的parents信息
                depdence_info["parents"].add(origin_transform_id)

                # 同时更新parent对应的sons信息
                if origin_transform_id in transform_depdence_map:
                    parent_dependence_info = transform_depdence_map[origin_transform_id]
                else:
                    parent_dependence_info = {"parents": set(), "sons": set()}
                    transform_depdence_map[origin_transform_id] = parent_dependence_info
                parent_dependence_info["sons"].add(transform_id)
    logger.info(transform_depdence_map)
    head = None
    tail = None
    for transform_id in transform_depdence_map:
        if not transform_depdence_map[transform_id]["parents"]:
            # 没有父依赖
            head = transform_config[transform_id]
        if not transform_depdence_map[transform_id]["sons"]:
            # 没有被任何人依赖
            tail = transform_config[transform_id]
    return head, tail
