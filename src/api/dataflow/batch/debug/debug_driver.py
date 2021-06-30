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
import time
import uuid
from datetime import datetime, timedelta
from urllib.parse import unquote

from common.bklanguage import BkLanguage
from common.local import get_request_username

import dataflow.batch.settings as settings
import dataflow.pizza_settings as pizza_settings
from dataflow.batch.debug.debugs import (
    delete_debug_error_log,
    delete_debug_metric_log,
    delete_debug_result_data_log,
    filter_debug_metric_log,
    get_debug_error_log_for_processing,
    get_debug_metric_log,
    get_debug_metric_log_for_processing,
    get_error_log,
    get_result_data_for_processing,
    insert_debug_error_log,
    insert_debug_metric_log,
    insert_debug_result_data_log,
    update_debug_metric_log,
    update_debug_metric_log_debug_end,
)
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper


def get_basic_info(debug_id):
    job_type = "batch"
    basic_debug_info = {"result_tables": {}, "debug_error": {}}
    # 从mysql中获取rt相关的debug信息
    rows = get_debug_metric_log(debug_id=debug_id, job_type=job_type)
    for row in rows:
        result_table_id = row["processing_id"]
        if result_table_id:
            output_total_count = int(row["output_total_count"] if row["output_total_count"] else 0)
            basic_debug_info["result_tables"][result_table_id] = {"output_total_count": output_total_count}
    error_info = get_error_log(debug_id=debug_id, job_type=job_type)
    if error_info and error_info[0]:
        result_table_id = error_info[0]["processing_id"]
        basic_debug_info["debug_error"] = {"error_result_table_id": result_table_id}
    return basic_debug_info


def get_node_info(args, debug_id, language=BkLanguage.CN):
    job_type = "batch"
    result_table_id = args.get("result_table_id")
    result_data_num = args.get("result_data_num") or 10
    debug_info = {
        "debug_errcode": {},
        "debug_metric": {"input_total_count": 0, "output_total_count": 0},
        "debug_data": {
            "result_data": [],
        },
    }
    # 从mysql中获取rt相关的调试metric信息
    rows = get_debug_metric_log_for_processing(debug_id=debug_id, job_type=job_type, processing_id=result_table_id)
    if rows:
        input_total_count = int(rows["input_total_count"] if rows["input_total_count"] else 0)
        output_total_count = int(rows["output_total_count"] if rows["output_total_count"] else 0)
        debug_info["debug_metric"]["input_total_count"] = input_total_count
        debug_info["debug_metric"]["output_total_count"] = output_total_count

    # 查询debug_error表
    error_info = get_debug_error_log_for_processing(debug_id=debug_id, job_type=job_type, processing_id=result_table_id)

    if error_info and error_info[0]:
        debug_info["debug_errcode"]["error_code"] = error_info[0]["error_code"]
        if language == BkLanguage.EN:
            debug_info["debug_errcode"]["error_message"] = error_info[0]["error_message_en"]
        else:
            debug_info["debug_errcode"]["error_message"] = error_info[0]["error_message"]
    # 查询计算结果明细数据
    rows = get_result_data_for_processing(
        result_data_num,
        debug_id=debug_id,
        job_type=job_type,
        processing_id=result_table_id,
    )
    if rows:
        debug_info["debug_data"]["result_data"] = [json.loads(row.result_data) for row in rows]
    # 丢弃数据明细目前不需要展示
    return debug_info


def create_debug(args):
    job_type = "batch"
    heads_str = args.get("heads")
    tails_str = args.get("tails")
    geog_area_code = args.get("geog_area_code")
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    debug_id = "debug_" + str(uuid.uuid4()).replace("-", "")
    created_by = get_request_username()
    job_id = submit_jobnavi(debug_id, created_by, heads_str, tails_str, geog_area_code, cluster=cluster_id)
    for processing_id in load_chain(heads_str, tails_str):
        insert_debug_metric_log(
            debug_id=debug_id,
            processing_id=processing_id,
            job_id=job_id,
            operator=created_by,
            job_type=job_type,
            debug_heads=heads_str,
            debug_tails=tails_str,
        )
    return debug_id


def load_chain(heads_str, tails_str):
    heads = heads_str.split(",")
    tails = tails_str.split(",")
    chain = []
    for tail in tails:
        chain.append(tail)
        load_processings(tail, heads, chain)
    return chain


def load_processings(processing_id, heads, chain):
    data_processing = DataProcessingHelper.get_dp_via_erp(processing_id)
    inputs = data_processing["inputs"]
    for input in inputs:
        parent_id = input["data_set_id"]
        if (
            input["data_set_type"] == "result_table"
            and parent_id not in chain
            and (parent_id not in heads or ("tags" in input and "static_join" not in input["tags"]))
        ):
            chain.append(parent_id)
            load_processings(parent_id, heads, chain)


def submit_jobnavi(
    debug_id,
    created_by,
    heads,
    tails,
    geog_area_code,
    cluster=settings.CLUSTER_GROUP_DEFAULT,
    queue=settings.DEBUG_QUEUE,
):
    jobnavi = JobNaviHelper(geog_area_code, cluster)
    extra_info = {
        "run_mode": pizza_settings.RUN_MODE,
        "makeup": False,
        "debug": True,
        "type": "batch_sql",
        "queue": queue,
        "heads": heads,
        "tails": tails,
    }
    if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
        extra_info["license"] = {
            "license_server_url": pizza_settings.LICENSE_SERVER_URL,
            "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
        }

    jobnavi_args = {
        "schedule_id": debug_id,
        "description": " Project {} submit by {}".format(debug_id, created_by),
        "type_id": "spark_sql",
        "active": True,
        "exec_oncreate": True,
        "extra_info": str(json.dumps(extra_info)),
    }
    res = jobnavi.create_schedule_info(jobnavi_args)
    return res


def stop_debug(debug_id, geog_area_code, cluster=settings.CLUSTER_GROUP_DEFAULT):
    job_id = filter_debug_metric_log(debug_id=debug_id)[0].job_id
    job_type = "batch"
    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    update_debug_metric_log_debug_end(now_time, debug_id=debug_id, job_id=job_id, job_type=job_type)
    jobnavi = JobNaviHelper(geog_area_code, cluster)
    data = jobnavi.get_execute_status(job_id)
    if data and data["status"] == "running":
        jobnavi.kill_execute(job_id)
    delete_expire_data()


def delete_expire_data():
    """
    删除过期数据，一天之前的

    :return: void
    """
    time_format = "%Y-%m-%d %H:%M:%S"
    day_format = "%Y%m%d"
    yesterday = datetime.today() + timedelta(-1)
    yesterday_format = yesterday.strftime(time_format)
    yesterday_format_day = yesterday.strftime(day_format)
    yesterday_timestamp = int((time.time() - 24 * 60 * 60) * 1000)
    delete_debug_metric_log(yesterday_format)
    delete_debug_error_log(yesterday_timestamp)
    delete_debug_result_data_log(yesterday_format_day)


def set_error_data(args, debug_id):
    job_id = args.get("job_id")
    job_type = "batch"
    result_table_id = args.get("result_table_id")
    error_code = args.get("error_code")
    error_message = args.get("error_message")
    error_message_en = args.get("error_message_en")
    debug_date = args.get("debug_date")
    insert_debug_error_log(
        debug_id=debug_id,
        job_id=job_id,
        job_type=job_type,
        processing_id=result_table_id,
        error_code=error_code,
        error_message=error_message,
        error_message_en=error_message_en,
        debug_date=debug_date,
    )


def update_metric_info(args, debug_id):
    input_total_count = args.get("input_total_count")
    output_total_count = args.get("output_total_count")
    job_id = args.get("job_id")
    result_table_id = args.get("result_table_id")
    job_type = args.get("job_type", "batch")

    update_debug_metric_log(
        debug_id,
        job_id,
        result_table_id,
        job_type=job_type,
        input_total_count=input_total_count,
        output_total_count=output_total_count,
    )


def set_result_data(args, debug_id):
    job_id = args.get("job_id")
    job_type = "batch"
    result_table_id = args.get("result_table_id")
    result_data = str(args.get("result_data"))
    result_data = json.loads(unquote(result_data).decode("UTF-8"))
    debug_date = args.get("debug_date")
    thedate = args.get("thedate")

    for one in result_data:
        insert_debug_result_data_log(
            debug_id=debug_id,
            job_id=job_id,
            job_type=job_type,
            processing_id=result_table_id,
            result_data=json.dumps(one),
            debug_date=debug_date,
            thedate=thedate,
        )
