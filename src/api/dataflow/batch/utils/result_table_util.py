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
from copy import deepcopy

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


def parse_hdfs_params(rt_id):
    storage_response = ResultTableHelper.get_result_table_storage(rt_id, "hdfs")
    batch_logger.info("Try to get %s hdfs storages" % rt_id)
    batch_logger.info(storage_response)
    return build_hdfs_params(rt_id, storage_response)


def build_hdfs_params(rt_id, meta_storage_response):
    storage_params = {}
    storage_connection_info = json.loads(meta_storage_response["hdfs"]["storage_cluster"]["connection_info"])
    storage_params["physical_table_name"] = meta_storage_response["hdfs"]["physical_table_name"]
    storage_params["cluster_name"] = meta_storage_response["hdfs"]["storage_cluster"]["cluster_name"]
    storage_params["cluster_group"] = meta_storage_response["hdfs"]["storage_cluster"]["cluster_group"]
    storage_params["name_service"] = storage_connection_info["hdfs_url"]
    storage_params["data_type"] = meta_storage_response["hdfs"]["data_type"]
    if (
        meta_storage_response["hdfs"]["data_type"] is not None
        and str(meta_storage_response["hdfs"]["data_type"]).lower() == "iceberg"
    ):
        storage_params["storekit_hdfs_conf"] = StorekitHelper.get_hdfs_conf(rt_id)
    return storage_params


def parse_ignite_params(rt_id):
    storage_response = ResultTableHelper.get_result_table_storage(rt_id, "ignite")
    batch_logger.info("Try to get %s ignite storages" % rt_id)
    batch_logger.info(storage_response)
    storage_params = json.loads(storage_response["ignite"]["storage_cluster"]["connection_info"])
    storage_params["physical_table_name"] = storage_response["ignite"]["physical_table_name"]
    return storage_params


def parse_source_fields(rt_id):
    fields_response = ResultTableHelper.get_result_table_fields(rt_id)
    batch_logger.info("Try to get %s fields" % rt_id)
    batch_logger.info(fields_response)
    fields = []
    for field_response in fields_response:
        if field_response["field_name"] != "timestamp" and field_response["field_name"] != "offset":
            field = {
                "field": field_response["field_name"],
                "type": field_response["field_type"],
                "origin": rt_id,
                "description": field_response["field_name"],
            }
            fields.append(field)
    return fields


def is_model_serve_mode_offline(processing_id):
    return DataProcessingHelper.is_model_serve_mode_offline(processing_id)


# deprecated
def get_rt_job_submit_args_from_db(parent_rt_id):
    parent_job_info = ProcessingJobInfoHandler.get_proc_job_info(parent_rt_id)
    if parent_job_info.job_config is not None:
        job_config = json.loads(parent_job_info.job_config)
        if "submit_args" in job_config:
            parent_submit_args = json.loads(job_config["submit_args"])
            if "schedule_period" in parent_submit_args:
                return parent_submit_args
    batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(parent_rt_id)
    return json.loads(batch_info.submit_args)


# deprecated
def query_rt_period(parent_rt_id):
    job_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(parent_rt_id)
    return job_info.schedule_period


# deprecated
def get_batch_min_window_size(submit_args, result_table_id):
    if submit_args["accumulate"]:
        return 1, "hour"

    parent_windows = []
    unit = "hour"
    parent_result_tables = deepcopy(submit_args["result_tables"])

    is_self_dependency = False
    if "advanced" in submit_args and "self_dependency" in submit_args["advanced"]:
        is_self_dependency = submit_args["advanced"]["self_dependency"]

    if is_self_dependency:
        parent_result_tables[result_table_id] = {}
        parent_result_tables[result_table_id]["window_size"] = submit_args["count_freq"]
        parent_result_tables[result_table_id]["window_delay"] = submit_args["count_freq"]
        parent_result_tables[result_table_id]["window_size_period"] = submit_args["schedule_period"]

    for rt in parent_result_tables:
        window_size = int(parent_result_tables[rt]["window_size"])
        if parent_result_tables[rt]["window_size_period"] == "day":
            window_size = window_size * 24
        elif parent_result_tables[rt]["window_size_period"] == "week":
            window_size = window_size * 24 * 7
        elif parent_result_tables[rt]["window_size_period"] == "month":
            unit = "month"
        parent_windows.append(window_size)

    # 静态关联没有窗口，但是需要支持直连ignite，当直连ignite而没有其他hdfs表时，默认窗口为当前周期
    if len(parent_windows) == 0 and "static_data" in submit_args and len(submit_args["static_data"]) > 0:
        window_size = int(submit_args["count_freq"])
        if submit_args["schedule_period"] == "day":
            window_size = window_size * 24
        elif submit_args["schedule_period"] == "week":
            window_size = window_size * 24 * 7
        elif submit_args["schedule_period"] == "month":
            unit = "month"
        parent_windows.append(window_size)
    return min(parent_windows), unit
