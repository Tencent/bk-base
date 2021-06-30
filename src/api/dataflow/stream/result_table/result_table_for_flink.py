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

from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import SourceIllegal
from dataflow.stream.handlers import processing_job_info, processing_stream_info
from dataflow.stream.result_table.result_table_for_common import fix_filed_type, load_parents
from dataflow.stream.settings import STREAM_TYPE


def load_flink_chain(heads_str, tails_str, job_id=None):
    heads = heads_str.split(",")
    tails = tails_str.split(",")
    result_tables = {}
    to_be_loaded = list(tails)
    while to_be_loaded:
        loading = list(to_be_loaded)
        to_be_loaded = []
        for result_table_id in loading:
            # 若其它分支迭代过程中已构建当前实时 RT 信息，当前迭代过程可直接跳过
            if result_table_id in result_tables:
                continue
            result_table = load_flink_result_table(result_table_id, job_id=job_id)
            result_tables[result_table_id] = result_table
            # 数据源为清洗表或者计算结果表
            for parent_info in result_table["parents_info"]:
                # 父节点为head
                parent_id = parent_info["data_set_id"]
                tags = parent_info["tags"]
                if parent_id in heads:
                    parent_result_table = load_flink_result_table(parent_id, is_source=True, job_id=job_id)
                    result_tables[parent_id] = parent_result_table
                # 父节点不为 head 时且 tag 不为 static_join, 继续向前找
                elif "static_join" not in tags:
                    parent_list = [parent_id]
                    to_be_loaded.extend(parent_list)
    return result_tables


def load_flink_result_table(result_table_id, is_source=False, job_id=None):
    """
    获取rt的具体信息

    :param result_table_id:  rt id
    :param is_source: rt是否为数据源
    :param job_id: 结果表所属 job id
    :return:
    """
    table_info = MetaApiHelper.get_result_table(result_table_id, related=["fields", "data_processing", "storages"])
    # 传入uc的 table name 后面增加业务号
    result_table = {
        "id": table_info["result_table_id"],
        "name": "{}_{}".format(table_info["result_table_name"], table_info["bk_biz_id"]),
        "description": table_info["description"],
    }
    # set common field
    fields = []
    fixed_fields = []
    event_time = {
        "field": "dtEventTime",
        "type": "string",
        "origin": "",
        "description": "event time",
    }
    bkdata_par_offset = {
        "field": "bkdata_par_offset",
        "type": "string",
        "origin": "",
        "description": "bkdata par offset",
    }
    for field in table_info["fields"]:
        if "timestamp" == field["field_name"]:
            continue
        else:
            common_field = {
                "field": field["field_name"],
                "type": fix_filed_type(field["field_type"]),
                "origin": field["origins"] or "",
                "description": field["description"],
            }
            fields.append(common_field)

    if not is_source:
        # set parents
        processing_id = table_info["data_processing"]["processing_id"]
        processing_info = MetaApiHelper.get_data_processing_via_erp(processing_id)
        parents_info, data_parents = load_parents(processing_info)
        result_table["parents"] = data_parents
        result_table["parents_info"] = parents_info
        # set window and processor

        stream_info = processing_stream_info.get(processing_id)

        window_info = json.loads(stream_info.window)
        result_table["window"] = window_info
        result_table["processor"] = {
            "processor_type": stream_info.processor_type,
            "processor_args": stream_info.processor_logic,
        }
        # set storage
        result_table["storages"] = list(table_info["storages"].keys())
        # set input and output
        if "kafka" not in table_info["storages"]:
            result_table["output"] = {}
        else:
            kafka_info = table_info["storages"]["kafka"]["storage_channel"]
            conf = {}
            if job_id:
                job_info = processing_job_info.get(job_id)
                deploy_config = job_info.deploy_config
                if deploy_config:
                    conf = json.loads(deploy_config).get("sink", {}).get(result_table_id)
            result_table["output"] = {
                "type": "kafka",
                "cluster_domain": kafka_info["cluster_domain"],
                "cluster_port": str(kafka_info["cluster_port"]),
                "conf": conf,
            }

        # set fields
        fixed_fields.append(event_time)
        if stream_info.checkpoint_type == "offset":
            fixed_fields.append(bkdata_par_offset)

    else:
        if "kafka" not in table_info["storages"]:
            raise SourceIllegal()
        kafka_info = table_info["storages"]["kafka"]["storage_channel"]
        result_table["input"] = {
            "type": "kafka",
            "cluster_domain": kafka_info["cluster_domain"],
            "cluster_port": str(kafka_info["cluster_port"]),
        }
        # set fields
        fixed_fields.append(event_time)
        if table_info["processing_type"] == STREAM_TYPE:
            # 根据父rt找到它的input processing ，根据processing找到checkpoint type
            processing_id = table_info["data_processing"]["processing_id"]

            stream_info = processing_stream_info.get(processing_id)

            if stream_info.checkpoint_type == "offset":
                fixed_fields.append(bkdata_par_offset)
        else:
            fixed_fields.append(bkdata_par_offset)

    result_table["fields"] = fixed_fields + fields
    return result_table
