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

import argparse
import datetime
import functools
import json
import sys
import time

from conf.dataapi_settings import API_URL

import dataflow.stream.api.http as http
from dataflow.stream.settings import (
    DEBUG_CLUSTER_VERSIONS,
    ONE_CODE_USER_MAIN_CLASS,
    STREAM_USED_API_URL,
    ImplementType,
)

MAX_DEBUGGING_SECONDS = 300


def main():
    parser = argparse.ArgumentParser()
    root_cli = parser.add_subparsers()
    # debug 模式
    debug_cli = root_cli.add_parser("debug")
    debug_cli.set_defaults(handler=functools.partial(handle_debug, debug_cli))
    debug_cli.add_argument("--debug-id")
    debug_cli.add_argument("--job-id")
    debug_cli.add_argument("--heads")
    debug_cli.add_argument("--tails")
    debug_cli.add_argument("--metric-kafka-server")
    debug_cli.add_argument("--implement-type")
    debug_cli.add_argument("--from-head", action="store_true")
    debug_cli.add_argument("--from-tail", action="store_true")
    debug_cli.add_argument("--enable-storage", action="store_false")
    debug_cli.add_argument("--code_tag", default="master")
    debug_cli.add_argument("--checkpoint-manager", default="dummy")

    args = parser.parse_args()
    args.handler(args)


def handle_debug(parser, args):
    print("The debug args is: %s" % args)
    debug_config = generate_debug_config(args)
    # start debug job
    submit_job(debug_config, args.debug_id, args.job_id, True)
    start_time_mark = time.time()
    while (time.time() - start_time_mark) < MAX_DEBUGGING_SECONDS:
        time.sleep(30)
        if is_killed_job():
            print("the debug job %s is finished." % job_id)
            sys.exit(0)
    # stop debug job
    cancel_job(job_id, True)


def load_result_tables(args):
    result_tables = get_result_table_chain(args.heads, args.tails)
    return result_tables


def get_transform_result_tables(head_ids, result_tables):
    transform = {}
    for result_table_id, result_table in list(result_tables.items()):
        if result_table_id not in head_ids:
            transform_result_table = result_table.copy()
            transform[result_table_id] = transform_result_table
    return transform


def generate_debug_config(args):
    if not args.heads or not args.tails:
        raise Exception("missing --heads --tails")
    head_ids = args.heads.split(",")
    global job_id
    job_id = str(args.job_id)
    jar_name = get_code_version()["jar_name"]
    job_args = {
        "job_id": job_id,
        "job_type": "flink",
        "run_mode": "debug",
        "api_url": {"base_dataflow_url": "http://%s/v3/dataflow/" % API_URL},
    }
    submit_args = {
        "parallelism": 1,
        "jar_files": jar_name,
        "task_manager_memory": 2048,
        "deploy_mode": "yarn-session",
    }
    if args.implement_type != ImplementType.CODE.value:
        result_tables = load_result_tables(args)
        udf = get_udf_info(list(get_transform_result_tables(head_ids, result_tables).keys()))
        udf_path = []
        for func_info in udf["config"]:
            udf_path.append(func_info["hdfs_path"])
        job_args["jobnavi"] = {"udf_path": udf_path}
        submit_args.update({"cluster_name": DEBUG_CLUSTER_VERSIONS.get(ImplementType.SQL.value)})
    else:
        job_info = get_flink_code_info(job_id.split("__")[1])
        if not ("processings" in job_info and len(job_info["processings"]) == 1):
            raise Exception("flink code must have one and only one processing.")
        submit_args.update(
            {
                "cluster_name": DEBUG_CLUSTER_VERSIONS.get(ImplementType.CODE.value),
                "programming_language": "java",
                "use_savepoint": False,
                "user_main_class": ONE_CODE_USER_MAIN_CLASS,
                "code": json.loads(job_info["processings"][0]["processor_logic"])["code"],
            }
        )
    debug_config = {"job_args": job_args, "submit_args": submit_args}
    return debug_config


def get_udf_info(processing_ids):
    udf_infos = []
    udf_names = []
    for processing_id in processing_ids:
        udf_info = get_processing_udf_info(processing_id)
        for udf in udf_info:
            if udf["name"] not in udf_names:
                udf_infos.append(udf)
            udf_names.append(udf["name"])
    return {"source_data": [], "config": udf_infos}


def is_killed_job():
    job_status = sync_status(job_id, True)
    if "ACTIVE" == job_status.get(job_id):
        return False
    else:
        return True


def get_flink_code_info(job_id):
    data = {"related": "processings"}
    _, result = http.get(STREAM_USED_API_URL.get("get_job_info").format(job_id=job_id), data)
    print("[api] get job info result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("call the api get job failed." + str(result))


def get_result_table_chain(heads, tails):
    """
    获取全链路信息
    :return:  info
    """
    data = {"heads": heads, "tails": tails}
    _, result = http.get(STREAM_USED_API_URL.get("get_result_table_chain"), data)
    print("[api] get_result_table_chain result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Call the api get_result_table_chain failed." + str(result))


def get_processing_udf_info(processing_id):
    """
    获取processing的udf信息

    :return: udf info
    """
    _, result = http.get(STREAM_USED_API_URL.get("get_udf_info").format(processing_id=processing_id))
    print("[api] get_processing_udf_info result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Call the api get_processing_udf_info failed." + str(result))


def submit_job(config, debug_id, job_id, is_debug=False):
    """
    提交flink任务
    :param config: 任务配置
    :return: result
    """
    data = {"conf": config, "is_debug": is_debug}
    # 打印提交时间
    print("Start: ", datetime.datetime.fromtimestamp(time.time()))
    _, result = http.post(STREAM_USED_API_URL.get("submit_job").format(job_id=job_id), data)
    print("[api] submit_to_flink result is %s" % result)
    print("End: ", datetime.datetime.fromtimestamp(time.time()))
    if result and result["result"]:
        return result["data"]
    elif result and result["code"] == "1571022":
        set_error_data(job_id, debug_id)
    else:
        raise Exception("Call the api submit_to_flink failed." + str(result))


def set_error_data(job_id, debug_id):
    args = {
        "job_id": job_id,
        "result_table_id": "all",
        "error_code": 1571022,
        "error_message": "Stream processing resources are insufficient. Please contact the Data System administrator.",
        "debug_date": int(time.time()),
    }
    _, result = http.post(STREAM_USED_API_URL.get("set_error_data").format(debug_id=debug_id), args)
    print("[api] set error data result is %s" % result)
    if not result["result"]:
        raise Exception("Call the api submit_to_flink failed." + str(result))


def sync_status(job_id, is_debug):
    """
    同步作业状态
    :param job_id: job id
    :param cluster_name:  cluster name ex: debug
    :param is_debug: is debug or not
    :return: result
    """
    data = {"is_debug": is_debug, "operate_info": json.dumps({"operate": "start"})}
    _, result = http.get(STREAM_USED_API_URL.get("sync_status").format(job_id=job_id), data)
    print("[api] sync_job_status result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Call the api sync_job_status failed." + str(result))


def cancel_job(job_id, is_debug):
    """
    cancel job
    :param job_id: job id
    :param cluster_name:  cluster name ex: debug
    :param is_debug: is debug or not
    :return: result
    """
    data = {"is_debug": is_debug}
    _, result = http.post(STREAM_USED_API_URL.get("cancel_job").format(job_id=job_id), data)
    print("[api] cancel_job result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Call the api cancel_job failed." + str(result))


def get_code_version():
    """
    get jar version
    :return: jar name
    """
    data = {"is_debug": True}
    _, result = http.get(STREAM_USED_API_URL.get("get_code_version").format(job_id=job_id), data)
    print("[api] get_submit_jar_name result is %s" % result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Call the api get_submit_jar_name failed." + str(result))


if __name__ == "__main__":
    main()
