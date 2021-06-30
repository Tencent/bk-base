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
import subprocess
import time
import uuid
from datetime import datetime, timedelta
from urllib.parse import unquote

from common.local import get_request_username
from django.utils.translation import ugettext_lazy as _

from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.log import stream_logger as logger
from dataflow.stream import settings as stream_conf
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.api.api_helper import StreamApiHelper
from dataflow.stream.exceptions.comp_execptions import (
    ComponentNotSupportError,
    FlinkStreamingError,
    GrowsBeyond64KBError,
)
from dataflow.stream.handlers import processing_job_info
from dataflow.stream.handlers.db_helper import get_meta_redis_conn
from dataflow.stream.handlers.debugs import *
from dataflow.stream.result_table.result_table_driver import load_chain
from dataflow.stream.settings import STREAM_JOB_CONFIG_DIR, ImplementType


def get_basic_info(debug_id):
    job_type = "stream"
    basic_debug_info = {"result_tables": {}, "debug_error": {}}
    job_id = filter_debug_metric_log(debug_id=debug_id)[0].job_id
    # 如有错误信息将错误信息写入db
    job_info = processing_job_info.get(job_id.split("__")[1])
    if "flink" == job_info.component_type:
        jobserver_config = json.loads(job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
        send_debug_job_exception_to_db(geog_area_code, cluster_id, job_id, debug_id, job_info.implement_type)
    # 从mysql中获取rt相关的debug信息
    rows = get_debug_metric_log(debug_id=debug_id, job_type=job_type)
    for row in rows:
        result_table_id = row["processing_id"]
        if result_table_id:
            input_total_count = int(row["input_total_count"] if row["input_total_count"] else 0)
            output_total_count = int(row["output_total_count"] if row["output_total_count"] else 0)

            filter_discard_count = int(row["filter_discard_count"] if row["filter_discard_count"] else 0)
            filter_discard_rate = (
                0.00 if input_total_count == 0 else float("%.2f" % (1.0 * filter_discard_count / input_total_count))
            )

            transformer_discard_count = int(row["transformer_discard_count"] if row["transformer_discard_count"] else 0)
            transformer_discard_rate = (
                0.00
                if input_total_count == 0
                else float("%.2f" % (1.0 * transformer_discard_count / input_total_count))
            )

            aggregator_discard_count = int(row["aggregator_discard_count"] if row["aggregator_discard_count"] else 0)
            aggregator_discard_rate = (
                0.00 if input_total_count == 0 else float("%.2f" % (1.0 * aggregator_discard_count / input_total_count))
            )

            warning_count = 0
            if filter_discard_rate == 1:
                warning_count += 1
            if transformer_discard_rate == 1:
                warning_count += 1
            if aggregator_discard_rate == 1:
                warning_count += 1

            basic_debug_info["result_tables"][result_table_id] = {
                "output_total_count": output_total_count,
                "warning_count": warning_count,
            }
    error_info = get_error_log(debug_id=debug_id, job_type=job_type)
    if error_info and error_info[0]:
        result_table_id = error_info[0]["processing_id"]
        basic_debug_info["debug_error"] = {"error_result_table_id": result_table_id}
    return basic_debug_info


def get_node_info(args, debug_id):
    job_type = "stream"
    result_table_id = args.get("result_table_id")
    result_data_num = args.get("result_data_num") or 10
    debug_info = {
        "debug_errcode": {},
        "debug_metric": {
            "input_total_count": 0,
            "output_total_count": 0,
            "filter_discard_count": 0,
            "filter_discard_rate": 0,
            "transformer_discard_count": 0,
            "transformer_discard_rate": 0,
            "aggregator_discard_count": 0,
            "aggregator_discard_rate": 0,
        },
        "debug_data": {
            "result_data": [],
            "discard_data": {"filter": [], "transformer": [], "aggregator": []},
        },
    }
    # 从mysql中获取rt相关的调试metric信息
    rows = get_debug_metric_log_for_processing(debug_id=debug_id, job_type=job_type, processing_id=result_table_id)
    if rows:
        input_total_count = int(rows["input_total_count"] if rows["input_total_count"] else 0)
        output_total_count = int(rows["output_total_count"] if rows["output_total_count"] else 0)
        debug_info["debug_metric"]["input_total_count"] = input_total_count
        debug_info["debug_metric"]["output_total_count"] = output_total_count

        filter_discard_count = int(rows["filter_discard_count"] if rows["filter_discard_count"] else 0)
        debug_info["debug_metric"]["filter_discard_count"] = filter_discard_count
        filter_discard_rate = (
            0.00 if input_total_count == 0 else float("%.2f" % (1.0 * filter_discard_count / input_total_count))
        )
        debug_info["debug_metric"]["filter_discard_rate"] = filter_discard_rate

        transformer_discard_count = int(rows["transformer_discard_count"] if rows["transformer_discard_count"] else 0)
        debug_info["debug_metric"]["transformer_discard_count"] = transformer_discard_count
        transformer_discard_rate = (
            0.00 if input_total_count == 0 else float("%.2f" % (1.0 * transformer_discard_count / input_total_count))
        )
        debug_info["debug_metric"]["transformer_discard_rate"] = transformer_discard_rate

        aggregator_discard_count = int(rows["aggregator_discard_count"] if rows["aggregator_discard_count"] else 0)
        debug_info["debug_metric"]["aggregator_discard_count"] = aggregator_discard_count
        aggregator_discard_rate = (
            0.00 if input_total_count == 0 else float("%.2f" % (1.0 * aggregator_discard_count / input_total_count))
        )
        debug_info["debug_metric"]["aggregator_discard_rate"] = aggregator_discard_rate

        debug_info["debug_metric"]["metric_info"] = [
            _("数据过滤丢弃率%s%%") % (filter_discard_rate * 100),
            _("数据转换丢弃率%s%%") % (transformer_discard_rate * 100),
            _("数据聚合丢弃率%s%%") % (aggregator_discard_rate * 100),
        ]

        warning_info = []
        if filter_discard_rate == 1:
            warning_info.append(_("【警告】数据过滤丢弃率为%s%%") % (filter_discard_rate * 100))
        if transformer_discard_rate == 1:
            warning_info.append(_("【警告】数据转换丢弃率为%s%%") % (transformer_discard_rate * 100))
        if aggregator_discard_rate == 1:
            warning_info.append(_("【警告】数据聚合丢弃率为%s%%") % (aggregator_discard_rate * 100))
        debug_info["debug_metric"]["warning_info"] = warning_info

    # 查询debug_error表
    error_info = get_debug_error_log_for_processing(debug_id=debug_id, job_type=job_type)

    if error_info and error_info[0]:
        debug_info["debug_errcode"]["error_code"] = error_info[0]["error_code"]
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
    job_type = "stream"
    heads_str = args.get("heads")
    tails_str = args.get("tails")
    geog_area_code = args.get("geog_area_code")
    metric_kafka_server = DatamanageHelper.get_metric_kafka_server(geog_area_code)
    debug_id = "debug_" + str(uuid.uuid4()).replace("-", "")
    # debug id + job id合并成调试的job name
    job_id = debug_id + "__" + args.get("job_id")
    job_info = processing_job_info.get(args.get("job_id"))
    processing_ids = []
    if job_info.implement_type != ImplementType.CODE.value:
        for result_table_id, result_table in list(load_chain(heads_str, tails_str).items()):
            if "parents" in result_table and result_table["parents"]:
                processing_ids.append(result_table_id)
    else:
        processing_ids.extend(tails_str.split(","))
    for processing_id in processing_ids:
        insert_debug_metric_log(
            debug_id=debug_id,
            processing_id=processing_id,
            job_id=job_id,
            operator=get_request_username(),
            job_type=job_type,
            debug_heads=heads_str,
            debug_tails=tails_str,
        )
    submit_async_debugger(debug_id, job_info, heads_str, tails_str, metric_kafka_server)
    return debug_id


def stop_debug(debug_id):
    job_id = filter_debug_metric_log(debug_id=debug_id)[0].job_id
    job_type = "stream"
    is_debug = True
    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    job_info = processing_job_info.get(job_id.split("__")[1])
    component_type = job_info.component_type.split("-")[0]
    if "flink" == component_type:
        debug_cluster_name = stream_conf.DEBUG_CLUSTER_VERSIONS.get(job_info.implement_type)
        cancel_job(str(job_id), debug_cluster_name, is_debug)
    elif "storm" == job_info.component_type:
        kill_storm_debug_job(debug_id)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % job_info.component_type)
    update_debug_metric_log_end_time(now_time, debug_id=debug_id, job_id=job_id, job_type=job_type)
    delete_expire_data()


def kill_storm_debug_job(debug_id):
    redis_client = get_meta_redis_conn()
    redis_client.set(debug_id, "to-kill")


def set_error_data(args, debug_id):
    job_id = args.get("job_id")
    job_type = "stream"
    result_table_id = args.get("result_table_id")
    error_code = args.get("error_code")
    error_message = args.get("error_message")
    debug_date = args.get("debug_date")
    insert_debug_error_log(
        debug_id=debug_id,
        job_id=job_id,
        job_type=job_type,
        processing_id=result_table_id,
        error_code=error_code,
        error_message=error_message,
        debug_date=debug_date,
    )


def update_metric_info(args, debug_id):
    input_total_count = args.get("input_total_count")
    output_total_count = args.get("output_total_count")
    filter_discard_count = args.get("filter_discard_count")
    transformer_discard_count = args.get("transformer_discard_count")
    aggregator_discard_count = args.get("aggregator_discard_count")
    job_id = args.get("job_id")
    result_table_id = args.get("result_table_id")
    job_type = args.get("job_type", "stream")

    update_debug_metric_log(
        debug_id,
        job_id,
        result_table_id,
        job_type=job_type,
        input_total_count=input_total_count,
        output_total_count=output_total_count,
        filter_discard_count=filter_discard_count,
        transformer_discard_count=transformer_discard_count,
        aggregator_discard_count=aggregator_discard_count,
    )


def set_result_data(args, debug_id):
    null = None
    true = True
    false = False
    job_id = args.get("job_id")
    job_type = "stream"
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

    import time

    yesterday_timestamp = int(round(time.mktime(time.strptime(yesterday_format, time_format)) * 1000))

    delete_debug_metric_log(yesterday_format)
    delete_debug_error_log(yesterday_timestamp)
    delete_debug_result_data_log(yesterday_format_day)


def submit_async_debugger(debug_id, job_info, heads, tails, metric_kafka_server):
    args = {
        "debug_id": debug_id,
        "job_id": debug_id + "__" + job_info.job_id,
        "heads": heads,
        "tails": tails,
        "metric_kafka_server": metric_kafka_server,
        "implement_type": job_info.implement_type,
    }

    command_line = generate_command_line(args)

    component_type_without_version = job_info.component_type.split("-")[0]

    # flink code 判断需要去除版本
    if "flink" == component_type_without_version:
        submit_flink_debugger(command_line)
    elif "storm" == job_info.component_type:
        submit_storm_debugger(command_line)
    else:
        logger.exception(job_info)
        raise ComponentNotSupportError("the component %s is not supported." % job_info.component_type)


def submit_flink_debugger(command_line):
    debug_log = STREAM_JOB_CONFIG_DIR + "/flink_debug.log"
    subprocess.Popen(
        "nohup python -m dataflow.stream.debug.flink_debugger debug {} &> {} &".format(command_line, debug_log),
        shell=True,
        close_fds=True,
        cwd=stream_conf.PIZZA_DIR,
    )
    logger.info("Flink debug job created: %s" % command_line)


def submit_storm_debugger(command_line):
    debug_log = STREAM_JOB_CONFIG_DIR + "/storm_debug.log"
    subprocess.Popen(
        "nohup python -m dataflow.stream.debug.storm_debugger debug {} &> {} &".format(command_line, debug_log),
        shell=True,
        close_fds=True,
        cwd=stream_conf.PIZZA_DIR,
    )
    logger.info("Storm debug job created: %s" % command_line)


def generate_command_line(args):
    """
    拼接debug参数
    :param args: 参数
    :return: 拼接后的debug参数
    """
    parts = []
    parts.extend(["--heads %s" % args["heads"]])
    parts.extend(["--tails %s" % args["tails"]])
    parts.extend(["--debug-id %s" % args["debug_id"]])
    parts.extend(["--job-id %s" % args["job_id"]])
    parts.extend(["--metric-kafka-server %s" % args["metric_kafka_server"]])
    parts.extend(["--implement-type %s" % args["implement_type"]])
    command_line = " ".join(parts)
    return command_line


def cancel_job(job_id, cluster_name, is_debug):
    StreamApiHelper.cancel_job({"job_id": job_id, "cluster_name": cluster_name, "is_debug": is_debug})


def send_debug_job_exception_to_db(geog_area_code, cluster_id, job_id, debug_id, implement_type):
    jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(geog_area_code, cluster_id)
    # debug yarn session的名字为debug_standard
    execute_id = jobnavi_stream_helper.get_execute_id(stream_conf.DEBUG_CLUSTER_VERSIONS.get(implement_type))
    exceptions = jobnavi_stream_helper.get_job_exceptions(execute_id, job_id)
    if exceptions is None or exceptions["root-exception"] is None or exceptions["root-exception"] == "":
        return
    if "grows beyond 64 KB" in str(exceptions["root-exception"]):
        error_args = {
            "job_id": job_id,
            "result_table_id": "all",
            "error_code": GrowsBeyond64KBError.code,
            "error_message": GrowsBeyond64KBError.MESSAGE,
            "debug_date": int(time.time()),
        }
    else:
        error_args = {
            "job_id": job_id,
            "result_table_id": "all",
            "error_code": FlinkStreamingError.code,
            "error_message": FlinkStreamingError.MESSAGE,
            "debug_date": int(time.time()),
        }
    set_error_data(error_args, debug_id)
