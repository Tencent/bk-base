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

from django.utils.translation import ugettext as _

import dataflow.shared.meta.processing.data_processing_helper as data_processing_driver
from dataflow.shared.handlers import processing_udf_info, processing_udf_job
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.api.api_helper import BksqlHelper, MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import ProcessingExistsError
from dataflow.stream.handlers import processing_job_info, processing_stream_info, processing_stream_job
from dataflow.stream.settings import SYSTEM_GENERATE_TYPE


def generate_outputs(processing_id, channel_cluster_config_id=None, storage_type=None):
    return [
        {
            "data_set_type": "result_table",
            "data_set_id": processing_id,
            "storage_cluster_config_id": None,
            "channel_cluster_config_id": channel_cluster_config_id,
            "storage_type": storage_type,
        }
    ]


def generate_result_table_input(data_set_id, storage_type, channel_cluster_config_id, tags=[]):
    return {
        "data_set_type": "result_table",
        "data_set_id": data_set_id,
        "channel_cluster_config_id": channel_cluster_config_id,
        "storage_type": storage_type,
        "storage_cluster_config_id": None,
        "tags": tags,
    }


def generate_raw_data_input(data_set_id, storage_type, channel_cluster_config_id):
    return {
        "storage_type": storage_type,
        "storage_cluster_config_id": None,
        "data_set_id": data_set_id,
        "data_set_type": "raw_data",
        "channel_cluster_config_id": channel_cluster_config_id,
    }


def generate_result_table_dummy_output(result_table_id):
    return {
        "data_set_type": "result_table",
        "data_set_id": result_table_id,
        "storage_cluster_config_id": None,
        "channel_cluster_config_id": None,
        "storage_type": None,
    }


def delete_processings(processing_id, with_data, contain_real_processing=True, is_direct_delete=True):
    # get stream id if have
    try:
        stream_id = processing_stream_info.get(processing_id).stream_id
    except Exception:
        stream_id = None
    remove_processing_list = [processing_id] if contain_real_processing else []
    collect_remove_processing(processing_id, remove_processing_list)
    # delete_all_processing_info(remove_processing_list)
    # 删除元数据记录
    logger.info("delete the data processing and result table " + str(remove_processing_list))
    remove_processings = []
    # 虚拟节点需要删除rt， 非虚拟节点则根据参数判断
    for remove_processing in remove_processing_list:
        # 删除udf信息
        processing_udf_info.delete(processing_id=remove_processing, processing_type="stream")
        processing_udf_job.delete(processing_id=remove_processing, processing_type="stream")

        if processing_id == remove_processing:
            remove_processings.append({"processing_id": remove_processing, "with_data": with_data})
        else:
            remove_processings.append({"processing_id": remove_processing, "with_data": True})
    if len(remove_processing_list) > 0:
        MetaApiHelper.bulk_delete_data_processing(remove_processings)
        for remove_processing in remove_processing_list:
            processing_stream_info.delete(remove_processing)
    # 如果 processing_stream_info 中没有这个stream id，说明此任务的最后一个节点已经被删除，这时候需要删除job记录
    try:
        if is_direct_delete and stream_id and not processing_stream_info.exists(stream_id=stream_id):
            processing_stream_job.delete(stream_id)
            processing_job_info.delete(stream_id)
            logger.info("succeed to delete job %s" % stream_id)
    except Exception:
        # 异常时不影响主流程
        logger.warning("failed to delete job info, the job id is %s" % stream_id)


def collect_remove_processing(processing_id, remove_processing_list):
    processing = MetaApiHelper.get_data_processing(processing_id)
    for one_input in processing["inputs"]:
        if one_input["data_set_type"] == "result_table":
            parent_result_table = MetaApiHelper.get_result_table(one_input["data_set_id"], related=["data_processing"])
            # 如果父节点为虚拟节点，则加入remove processing
            if parent_result_table["generate_type"] == SYSTEM_GENERATE_TYPE:
                parent_processing_id = parent_result_table["data_processing"]["processing_id"]
                remove_processing_list.append(parent_processing_id)
                collect_remove_processing(parent_processing_id, remove_processing_list)


def check_processing_exists(processing_id):
    if data_processing_driver.DataProcessingHelper.check_data_processing(processing_id):
        raise ProcessingExistsError(_("名称(%s)已存在" % processing_id))


def get_dimension_fields(sql):
    """
    通过 sql 解析获取维度字段列表

    @param sql: select field1,count(1) as cnt from tableA group by field1
    @return: [field1]
    """
    dimension_fields = []
    try:
        field_dimension_info = BksqlHelper.convert_dimension({"sql": sql})
        for one in field_dimension_info:
            if one["is_dimension"]:
                dimension_fields.append(one["field_name"])
    except Exception:
        logger.error("Failed to get dimension info by bksql.")
    return dimension_fields
