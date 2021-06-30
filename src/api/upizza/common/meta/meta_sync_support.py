#! -*- coding=utf-8 -*-
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

from __future__ import absolute_import, print_function, unicode_literals

import datetime
import json
import os
import time

from common.api import MetaApi
from common.exceptions import MetaSyncSupportError

particular_pk_dict = {
    "tdw_table": "table_id",
    "action_info": "action_name",
    "dataflow_node_info": "node_id",
    "scene_info": "scene_name",
    "visualization": "visualization_name",
    "visualization_component": "component_name",
    "algorithm_demo": "algorithm_demo_name",
    "dmm_model_indicator": "indicator_name",
    "result_table": "result_table_id",
    "modeling_step": "step_name",
    "algorithm_type": "algorithm_type",
    "model_info": "model_id",
    "dmm_calculation_function_config": "function_name",
    "action_type": "action_type",
    "data_transferring": "transferring_id",
    "dataflow_info": "flow_id",
    "operation": "operation_id",
    "cluster_group_config": "cluster_group_id",
    "training_strategy_template": "training_strategy_template_name",
    "dmm_model_instance": "instance_id",
    "evaluation": "evaluation_id",
    "model_experiment": "experiment_id",
    "model_experiment_node": "node_id",
    "basic_model": "basic_model_id",
    "dmm_model_info": "model_id",
    "dmm_model_calculation_atom": "calculation_atom_name",
    "action_visual_component": "component_name",
    "resource_group_info": "resource_group_id",
    "feature_type": "feature_type_name",
    "data_processing": "processing_id",
    "dmm_model_release": "version_id",
    "field_type_config": "field_type",
    "dmm_field_constraint_config": "constraint_id",
    "project_info": "project_id",
    "dmm_model_instance_table": "result_table_id",
    "belongs_to_config": "belongs_id",
    "feature": "feature_name",
    "aggregation_method": "agg_name",
    "job_status_config": "status_id",
    "basic_model_generator": "generator_name",
    "algorithm": "algorithm_name",
    "sample_set_fixation": "sample_set_id",
    "dmm_model_instance_indicator": "result_table_id",
}


def generate_ms(
    input_type,
    json_list=None,
    json_file=None,
    method="CREATE",
    db_name=None,
    table_name=None,
    primary_key=None,
    sync_now=False,
    gen_ms_file=True,
    affect_original=False,
    out_put_path=None,
    topic_name=None,
    **kwargs
):
    """
    生成ms文件

    :param input_type: 输入数据类型 (json, path)
    :param json_list: json格式列表, 数据由json_list参数带入;
    :param json_file: json格式列表文件地址, 数据由path参数带入, 文件名： dir_to_file/table_name.json

    :param method: 执行方法 (CREATE, UPDATE, DELETE)
    :param db_name: 数据来源db名称
    :param table_name: 数据来源表名
    :param primary_key: string 主键名称

    :param sync_now: 是否立即同步生成的ms信息
    :param gen_ms_file: 是否生成ms文件
    :param affect_original: 是否修改业务库

    :param out_put_path: ms文件产出目录
    :param topic_name: 生成ms文件的主题name

    :param kwargs:
    :return: json 包含同步情况、生成情况的ms格式数据返回
    """

    if not sync_now and not gen_ms_file:
        message = "please set run mode: sync_now or (only)gen_ms_file"
        raise MetaSyncSupportError(message_kv={"inner_error": message})

    if input_type == "json_list" and json_list:
        input_json_list = json_list
    elif input_type == "json_file" and json_file:
        if not os.path.isfile(json_file):
            message = 'file "{}" is not exists'.format(json_file)
            raise MetaSyncSupportError(message_kv={"inner_error": message})
        with open(json_file, "r") as fp:
            json_str = fp.read()
            input_json_list = json.loads(json_str)
        # (path_dir, file_name) = os.path.split(json_path)
        # (table_name, ext_type) = os.path.splitext(file_name)
    else:
        message = 'invalid input_type: {}, please choice in ["json_list", "path"]'.format(input_type)
        raise MetaSyncSupportError(message_kv={"inner_error": message})
    if not primary_key:
        primary_key = particular_pk_dict[table_name] if table_name in particular_pk_dict else "id"

    if not input_json_list or not isinstance(input_json_list, list):
        message = "invalid input json_list: {}".format(input_json_list)
        raise MetaSyncSupportError(message_kv={"inner_error": message})
    if not db_name or not table_name or not primary_key:
        message = "primary_key / db_name / table_name is invalid: db_name[{}] table_name[{}] primary_key[{}]".format(
            db_name, table_name, primary_key
        )
        raise MetaSyncSupportError(message_kv={"inner_error": message})

    json_list = input_json_list if input_json_list else []

    # changed_data
    sync_obj_list = []
    for item in json_list:
        if not item.get("updated_at", None) and "updated_at" in item:
            del item["updated_at"]
        primary_key_value = str(item[primary_key])
        item[primary_key] = int(primary_key_value) if primary_key_value.isdigit() else primary_key_value
        sync_obj_list.append(
            {
                "change_time": time.strftime(str("%Y-%m-%d %H:%M:%S"), time.localtime()),
                "changed_data": json.dumps(item, default=str),
                "primary_key_value": item[primary_key],
                "db_name": db_name,
                "table_name": table_name,
                "method": method,
            }
        )

    time_suffix = (datetime.datetime.now()).strftime("%Y%m%d-%H%M")
    out_put_path = out_put_path if out_put_path else "/tmp"
    topic_name = topic_name if topic_name else "{}-{}".format(db_name, table_name)
    ms_file_output_path = "/{}/{}_{}.ms".format(out_put_path, topic_name, time_suffix)
    if sync_obj_list and gen_ms_file:
        with open(ms_file_output_path, "w") as f:
            f.write(json.dumps(sync_obj_list))

    sync_ret = None
    if sync_obj_list and sync_now:
        sync_ret = send_sync_requests(ms_content_list=sync_obj_list, affect_original=affect_original)

    return dict(
        gen_ms_file=ms_file_output_path if gen_ms_file else None,
        sync_res=sync_ret if sync_now else None,
    )


def send_sync_requests(
    ms_file_path=None,
    ms_content_list=None,
    batch_cnt=20,
    affect_original=True,
    batch=False,
):
    """
    发送tag同步请求

    :param ms_file_path: ms文件路径
    :param ms_content_list: 同步tag内容
    :param batch_cnt: 批量同步条数
    :param affect_original: 是否修改配置库
    :param batch: 是否批量执行
    :return: boolean 同步结果
    """
    if not ms_file_path and not ms_content_list:
        message = "input some thing....: {}"
        raise MetaSyncSupportError(message_kv={"inner_error": message})
    if ms_content_list is None or not isinstance(ms_content_list, list):
        if ms_file_path and not os.path.isfile(ms_file_path):
            message = 'ms file "{}" is not exists'.format(ms_file_path)
            raise MetaSyncSupportError(message_kv={"inner_error": message})
        with open(ms_file_path, "r") as fp:
            json_str = fp.read()
            ms_content_list = json.loads(json_str)
    ms_chunked_lst = [ms_content_list[i : i + batch_cnt] for i in range(0, len(ms_content_list), batch_cnt)]

    for chunk_piece in ms_chunked_lst:
        # generate ms object
        ms_sync_obj = {
            "content_mode": "id",
            "db_operations_list": chunk_piece,
            "bk_username": "admin",
            "batch": batch,
            "affect_original": affect_original,
        }
        try:
            response = MetaApi.sync_hook(ms_sync_obj, raise_exception=True)
        except Exception as re:
            message = "request error, detail: {}".format(re)
            raise MetaSyncSupportError(message_kv={"inner_error": message})
        if not response.is_success():
            message = "sync status_code error，status_code: {}".format(response.message)
            raise MetaSyncSupportError(message_kv={"inner_error": message})
        if response.message != "ok":
            message = "sync error，detail: {}".format(response.errors)
            raise MetaSyncSupportError(message_kv={"inner_error": message})
        ret_obj = response.data
        print("sync success! ret is {}".format(ret_obj))
