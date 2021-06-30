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

import base64
import json
import time
import uuid

from dataflow.batch import settings
from dataflow.batch.exceptions.comp_execptions import (
    BatchIllegalArgumentError,
    BatchInteractiveServerTimeoutException,
    BatchUnexpectedException,
    CodeCheckBannedCodeError,
    CodeCheckSyntaxError,
    JobNaviNoValidScheduleError,
)
from dataflow.batch.settings import CODECHECK_BLACKLIST_GROUP
from dataflow.batch.utils import resource_util
from dataflow.shared.codecheck.codecheck_helper import CodeCheckHelper
from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger


def create_server_and_session(args):
    server_id = args["server_id"]
    server_create_res = create_interactive_server(server_id, args)
    is_server_failed = False
    for i in range(0, 15):
        time.sleep(2)
        try:
            batch_logger.info("{}({}) require retrieve status".format(server_id, server_create_res))
            server_res = retrieve_server_status(server_id, args)
            if server_res["server_status"] == "started":
                res = create_spark_session(server_id, args)
                res["exec_id"] = server_create_res
                return res
            elif server_res == "failed":
                is_server_failed = True
                break
        except Exception as e:
            batch_logger.info("{}({}) retrieve server status failed".format(server_id, server_create_res))
            batch_logger.warning(e)
    batch_logger.info("{}({}) failed to start, start to delete schedule".format(server_id, server_create_res))
    stop_server(server_id, args)
    if is_server_failed:
        raise BatchInteractiveServerTimeoutException("Error: server {} start failed".format(server_id))
    else:
        raise BatchUnexpectedException("Error: server {} start timeout".format(server_id))


def create_interactive_server(server_id, args):
    batch_logger.info("Create Livy server with args {}".format(args))
    geog_area_code = args["geog_area_code"]
    jobnavi = __get_jobnavi(geog_area_code)
    if "node_label" in args:
        node_label = args["node_label"]
    else:
        if "cluster_group" in args:
            node_label, _ = resource_util.get_jobnavi_label(geog_area_code, args["cluster_group"], "interactive")
        else:
            node_label, _ = resource_util.get_default_jobnavi_label(
                geog_area_code, "interactive", component_type="spark"
            )
    bk_user = ""
    if "bk_user" in args:
        bk_user = args["bk_user"]
    return __register_jobnavi(server_id, geog_area_code, jobnavi, node_label, bk_user)


def retrieve_server_status(server_id, args):
    geog_area_code = args["geog_area_code"]
    return __send_jobnavi_event(server_id, geog_area_code, "server_status", None)


def create_spark_session(server_id, args):
    geog_area_code = args["geog_area_code"]
    bk_user = args["bk_user"]
    cluster_group = settings.LIVY_DEFAULT_CLUSTER_GROUP

    if "cluster_group" in args:
        cluster_group = args["cluster_group"]

    cluster_name = __get_livy_cluster(cluster_group, geog_area_code)

    spark_app_name = "livy-session-{}".format(server_id)

    engine_conf = {
        "spark.app.name": spark_app_name,
        "spark.yarn.queue": cluster_name,
        "spark.bkdata.user": bk_user,
        "spark.bkdata.geog_area_code": geog_area_code,
    }

    if "bk_project_id" in args:
        engine_conf["spark.bkdata.project.id"] = str(args["bk_project_id"])

    if "engine_conf" in args:
        for k in args["engine_conf"]:
            engine_conf[k] = args["engine_conf"][k]

    event_info = {"kind": "pyspark", "conf": engine_conf}

    if "preload_py_files" in args:
        event_info["preload_py_files"] = args["preload_py_files"]

    if "preload_files" in args:
        event_info["preload_files"] = args["preload_files"]

    if "preload_jars" in args:
        event_info["preload_jars"] = args["preload_jars"]
    return __send_jobnavi_event(server_id, geog_area_code, "create_session", event_info)


def get_session_status(server_id, args):
    geog_area_code = args["geog_area_code"]
    return __send_jobnavi_event(server_id, geog_area_code, "session_status", None)


def run_pyspark_code(server_id, args):
    geog_area_code = args["geog_area_code"]
    code = args["code"]
    if "blacklist_group_name" in args:
        blacklist_group_name = args["blacklist_group_name"]
    else:
        blacklist_group_name = None

    # here is the code check
    if "context" in args:
        context = ""
        for item in args["context"]:
            context = context + item
            if not item.endswith("\n"):
                context = context + "\n"
        __code_check(context, blacklist_group_name)

    param = {"code": code}
    return __send_jobnavi_event(server_id, geog_area_code, "run_code", param)


def get_code_status(code_id, server_id, args):
    geog_area_code = args["geog_area_code"]

    param = {"code_id": code_id}
    return __send_jobnavi_event(server_id, geog_area_code, "code_status", param)


def cancel_code(code_id, server_id, args):
    geog_area_code = args["geog_area_code"]

    param = {"code_id": code_id}
    return __send_jobnavi_event(server_id, geog_area_code, "code_cancel", param)


def kill_server(server_id, args):
    geog_area_code = args["geog_area_code"]
    jobnavi = __get_jobnavi(geog_area_code)
    exec_id = __get_jobnavi_execute_id(server_id, jobnavi, check_preparing=True)
    return jobnavi.kill_execute(exec_id)


def delete_server_schedule(server_id, args):
    geog_area_code = args["geog_area_code"]
    jobnavi = __get_jobnavi(geog_area_code)
    batch_logger.info("Delete Livy server with args {}".format(args))
    response = jobnavi.get_schedule_info(server_id)
    if response is None:
        return None
    elif response["type_id"] == "spark_interactive_python_server":
        return jobnavi.delete_schedule(server_id)
    else:
        raise BatchIllegalArgumentError("This api can only be used to delete spark_interactive_python_server job")


def stop_server(server_id, args):
    geog_area_code = args["geog_area_code"]
    try:
        kill_server(server_id, args)
        for i in range(0, 10):
            time.sleep(1)
            jobnavi = __get_jobnavi(geog_area_code)
            if not __check_preparing_instance(server_id, jobnavi) and not __check_running_instance(server_id, jobnavi):
                batch_logger.info(
                    "Found no running and preparing instance, start to delete schedule {}".format(server_id)
                )
                res = delete_server_schedule(server_id, args)
                return res
    except JobNaviNoValidScheduleError as e:
        batch_logger.warning(e)
        batch_logger.info("Start to delete schedule {}".format(server_id))
        res = delete_server_schedule(server_id, args)
        return res
    raise BatchUnexpectedException("Failed to destory livy server")


def __send_jobnavi_event(server_id, geog_area_code, event_name, param_dict):
    jobnavi = __get_jobnavi(geog_area_code)
    exec_id = __get_jobnavi_execute_id(server_id, jobnavi)
    jobnavi_args = {
        "event_name": event_name,
        "execute_id": exec_id,
        "tags": [geog_area_code],
    }

    if param_dict is None:
        param_dict = {}

    # here is to add some metrics info
    param_dict["custom_livy_event_send_time"] = int(time.time() * 1000.0)
    param_dict["custom_livy_event_id"] = str(uuid.uuid4())
    param_str = json.dumps(param_dict)

    jobnavi_args["event_info"] = param_str

    event_id = jobnavi.send_event(jobnavi_args)
    resp = __get_event_response(event_id, jobnavi, event_name, param_dict["custom_livy_event_id"])

    resp_json = json.loads(resp)

    # here is to add some metrics info
    resp_json["custom_livy_event_response_time"] = int(time.time() * 1000.0)
    batch_logger.info(resp_json)
    return resp_json


def __code_check(code, blacklist_name=None):
    code_language = "python"
    check_content = base64.urlsafe_b64encode(code)
    parser_group_name = None
    if blacklist_name is None:
        blacklist_group_name = CODECHECK_BLACKLIST_GROUP
    else:
        blacklist_group_name = blacklist_name
    check_flag = True
    res = CodeCheckHelper.verify_code(
        code_language,
        check_content,
        blacklist_group_name,
        check_flag,
        parser_group_name,
    )
    code_lines = code.splitlines()
    if res["parse_status"] == "success":
        if "parse_result" in res and len(res["parse_result"]) > 0:
            errors = []
            exception_str = "发现禁止加载的类:"
            for item in res["parse_result"]:
                error = {
                    "error_line": code_lines[item["line_no"] - 1],
                    "line_number": item["line_no"],
                }
                errors.append(error)
                exception_str = exception_str + " {}(line {}),".format(item["import_name"], item["line_no"])
            exception_str = exception_str[:-1]
            raise CodeCheckBannedCodeError(exception_str, errors=errors)
    elif res["parse_status"] == "fail":
        errors = []
        if "parse_result" in res and len(res["parse_result"]) > 0:
            errors = []
            for item in res["parse_result"]:
                error = {
                    "error_line": code_lines[item["line_no"] - 1],
                    "line_number": item["line_no"],
                }
                errors.append(error)
        raise CodeCheckSyntaxError(res["parse_message"], errors=errors)
    else:
        raise BatchUnexpectedException("Code check unknown fault")


def __register_jobnavi(schedule_id, geog_area_code, jobnavi, node_label, user_id):
    extra_info = {
        "api_url": settings.dataapi_settings.API_URL,
        "geog_area_code": geog_area_code,
    }

    jobnavi_args = {
        "schedule_id": schedule_id,
        "type_id": "spark_interactive_python_server",
        "description": "Project {} submitted by {}".format(schedule_id, user_id),
        "recovery": {
            "enable": False,
        },
        "exec_oncreate": True,
        "extra_info": str(json.dumps(extra_info)),
        "max_running_task": 1,
        "node_label": node_label,
    }

    batch_logger.info("Register schedule with {}".format(jobnavi_args))
    res = jobnavi.create_schedule_info(jobnavi_args)
    return res


def __jobnavi_execute(schedule_id, jobnavi):
    jobnavi_args = {
        "schedule_id": schedule_id,
    }
    return jobnavi.execute_schedule(jobnavi_args)


def __get_jobnavi_execute_id(schedule_id, jobnavi, check_preparing=False):
    if check_preparing:
        status = "preparing"
        res = jobnavi.get_execute_result_by_status(schedule_id, status)
        if res is None:
            raise BatchUnexpectedException("Jobnavi failed to response")
        if len(res) >= 1 and res[0]["execute_info"]["id"]:
            batch_logger.info("The cluster name {} result is {}".format(schedule_id, str(res)))
            return res[0]["execute_info"]["id"]

    status = "running"
    res = jobnavi.get_execute_result_by_status(schedule_id, status)
    if res is None:
        raise BatchUnexpectedException("Jobnavi failed to response")
    if len(res) >= 1 and res[0]["execute_info"]["id"]:
        batch_logger.info("The cluster name {} result is {}".format(schedule_id, str(res)))
        return res[0]["execute_info"]["id"]
    raise JobNaviNoValidScheduleError("Can't find valid livy server instance for {}".format(schedule_id))


def __check_running_instance(schedule_id, jobnavi):
    status = "running"
    res = jobnavi.get_execute_result_by_status(schedule_id, status)
    if len(res) >= 1:
        batch_logger.info("Found running instance {} result is {}".format(schedule_id, str(res)))
        return True
    return False


def __check_preparing_instance(schedule_id, jobnavi):
    status = "preparing"
    res = jobnavi.get_execute_result_by_status(schedule_id, status)
    if len(res) >= 1:
        batch_logger.info("Found preparing {} result is {}".format(schedule_id, str(res)))
        return True
    return False


def __get_jobnavi(geog_area_code):
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    return JobNaviHelper(geog_area_code, cluster_id)


def __get_event_response(event_id, jobnavi, info="", custom_livy_event_id=""):
    for i in range(0, 10):
        res = jobnavi.get_event(event_id)
        if res["is_processed"]:
            if res["process_success"]:
                return res["process_info"]
            else:
                raise BatchUnexpectedException(res["process_info"])
        time.sleep(1)
    raise BatchUnexpectedException(
        "Jobnavi Event {}({}) response time out (custom_id: {})".format(info, event_id, custom_livy_event_id)
    )


def __get_livy_cluster(cluster_group, geog_area_code):
    queue_name, _ = resource_util.get_yarn_queue_name(geog_area_code, cluster_group, "interactive")
    return queue_name


def __check_cluster_config(cluster_name):
    return processing_cluster_config.where(component_type="livy", cluster_name=cluster_name).exists()
