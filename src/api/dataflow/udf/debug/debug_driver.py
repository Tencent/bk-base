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
import os
import shutil
import time
import uuid
from datetime import datetime

from common.local import get_request_username
from conf.dataapi_settings import API_URL
from django.db.models import Max
from django.utils.translation import ugettext as _

from dataflow import pizza_settings
from dataflow.batch.config.hdfs_config import ConfigHDFS
from dataflow.shared.flow.flow_helper import FlowHelper
from dataflow.shared.handlers import processing_version_config
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import udf_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.stream.result_table.result_table_for_common import fix_filed_type
from dataflow.stream.utils.FtpServer import FtpServer
from dataflow.udf.exceptions.comp_exceptions import (
    DebugJobRunError,
    DebugJobSumitError,
    DebugSqlError,
    ParseSqlError,
    UdfExpectedException,
    YarnResourcesLackError,
)
from dataflow.udf.functions.function_driver import FunctionHandler
from dataflow.udf.handlers import bksql_function_debug_log, bksql_function_dev_config, debug_handler
from dataflow.udf.helper.api_helper import BksqlHelper, MetaApiHelper
from dataflow.udf.settings import (
    BATCH_DEBUG_QUEUE,
    DEBUG_GET_FLINK_EXCEPTION_TIMEOUT,
    DEBUG_RESOURCE_ID,
    DEBUG_SUBMIT_JOB_TIMEOUT,
    FUNC_DEV_VERSION,
    METRIC_KAFKA_SERVER,
    METRIC_KAFKA_TOPIC,
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    UC_TIME_ZONE,
    UDF_PYTHON_SCRIPT_PATH,
    UDF_WORKSPACE,
    CalculationType,
)


def create_debug(func_name, calculation_types, sql, result_table_id):
    parse_re = _parse_sql_function(sql)
    if len(parse_re) != 1 or func_name not in parse_re:
        raise DebugSqlError(func_name)
    func_path = os.path.abspath(os.path.join(UDF_WORKSPACE, func_name)).encode("utf8")
    bkdata_udf_project = os.path.abspath(os.path.join(UDF_WORKSPACE, "bkdata_udf_project")).encode("utf8")
    # rm func_name work space
    shutil.rmtree(func_path, True)
    # copy udf project to func_name work space
    shutil.copytree(bkdata_udf_project, func_path)
    uuid6 = str(uuid.uuid4()).replace("-", "")[:6]
    debug_id = func_name + "-" + uuid6
    bksql_function_dev_config.update_version_config(
        func_name,
        FUNC_DEV_VERSION,
        debug_id=debug_id,
        support_framework=",".join(calculation_types),
    )
    # check debug sql
    function_config = bksql_function_dev_config.get(debug_id=debug_id, version="dev")
    for calculation_type in calculation_types:
        if CalculationType.stream.name == calculation_type:
            _get_flink_transform(sql, function_config)
        elif CalculationType.batch.name == calculation_type:
            _get_spark_transform(sql, function_config, result_table_id)
    # inset debug_id
    return debug_id


def _parse_sql_function(sql, check_params=False):
    """

    :param sql:
    :param check_params:
    :return:
    """
    params = {"sql": sql, "properties": {"check_params": check_params}}
    return BksqlHelper.parse_sql_function(params)


def auto_generate_data(result_table_id):
    table_info = MetaApiHelper.get_result_table(result_table_id, related=["fields"])
    value = []
    schema = []
    for field in table_info["fields"]:
        if field["field_name"] == "timestamp":
            continue
        one_schema = {
            "field_name": field["field_name"],
            "field_type": field["field_type"],
        }
        schema.append(one_schema)
        value.append(_generate_data(field["field_type"]))

    return {"schema": schema, "value": [value]}


def _add_event_time(debug_data):
    new_schema = [{"field_name": "dtEventTime", "field_type": "string"}]
    for one_schema in debug_data["schema"]:
        new_schema.append(one_schema)
    new_values = []
    for row in debug_data["value"]:
        new_row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        for one_data in row:
            new_row.append(one_data)
        new_values.append(new_row)
    return {"schema": new_schema, "value": new_values}


def start_debug(calculation_type, debug_id, debug_data, result_table_id, sql):
    geog_area_code = TagHelper.get_geog_area_code_by_rt_id(result_table_id)
    cluster_id = JobNaviHelper.get_jobnavi_cluster(calculation_type)
    debug_data = _add_event_time(debug_data)
    # get function config
    function_config = bksql_function_dev_config.get(debug_id=debug_id, version="dev")
    args = {
        "debug_id": debug_id,
        "sql": sql,
        "source_result_table_id": result_table_id,
        "debug_data": json.dumps(debug_data),
        "created_by": get_request_username(),
        "description": "start to debug %s" % function_config.__name__,
    }
    bksql_function_debug_log.save(**args)
    if calculation_type == CalculationType.batch.name:
        _start_batch_debug(
            geog_area_code,
            cluster_id,
            function_config,
            debug_id,
            debug_data,
            result_table_id,
            sql,
        )
    elif calculation_type == CalculationType.stream.name:
        _start_stream_debug(
            geog_area_code,
            cluster_id,
            function_config,
            debug_id,
            debug_data,
            result_table_id,
            sql,
        )
    else:
        raise Exception("Not support calculation type %s" % calculation_type)


def _generate_data(field_type):
    if field_type == "string" or field_type == "text":
        return "this is a string."
    elif field_type == "int":
        return 10
    elif field_type == "long":
        return 100
    elif field_type == "float":
        return float(0.02)
    elif field_type == "double":
        return float(5222.2)
    else:
        raise Exception("Not support the type %s" % field_type)


def _get_udf_info(geog_area_code, debug_data, function_config):
    source_data = []
    field_names = []
    for one_schema in debug_data["schema"]:
        field_names.append(one_schema["field_name"])
    for one_value in debug_data["value"]:
        source_data.append(dict(list(zip(field_names, one_value))))
    # 打包时统一所有地域上传，调试时根据调试 result table 获取对应的地域 hdfs cluster
    one_of_cluster = FtpServer.get_ftp_server_cluster_group(geog_area_code)
    config_hdfs = ConfigHDFS(one_of_cluster)
    config = [
        {
            "language": function_config.func_language,
            "name": function_config.__name__,
            "type": function_config.func_udf_type,
            "hdfs_path": "%s/app/udf/%s/dev/%s.jar"
            % (
                config_hdfs.get_url(),
                function_config.__name__,
                function_config.__name__,
            ),
            "local_path": os.path.join(UDF_PYTHON_SCRIPT_PATH, "udf_jar"),
        }
    ]
    return {"source_data": source_data, "config": config}


def _get_spark_transform(sql, function_config, source_result_table_id):
    parse_args = {
        "sql": sql,
        "properties": {
            "spark.result_table_name": "debug_%s" % function_config.__name__,
            "spark.bk_biz_id": 591,
            "spark.input_result_table": [source_result_table_id],
            "spark.dataflow_udf_env": "dev",
            "spark.dataflow_udf_function_name": function_config.__name__,
        },
    }
    parsed_result = BksqlHelper.parse_spark_sql(parse_args)
    return {
        parsed_result[0]["id"]: {
            "id": parsed_result[0]["id"],
            "name": parsed_result[0]["name"],
            "fields": _get_spark_transform_fields(parsed_result[0]["fields"]),
            "processor": {
                "processor_type": "sql",
                "processor_args": parsed_result[0]["sql"],
            },
            "parents": [source_result_table_id],
        }
    }


def _get_spark_transform_fields(transform_fields):
    fields = []
    for field in transform_fields:
        fields.append({"field": field["field_name"], "type": field["field_type"], "origin": ""})
    return fields


def _get_batch_params():
    return {
        "makeup": False,
        "queue": BATCH_DEBUG_QUEUE,
        "run_mode": "udf_debug",
        "debug": True,
        "type": "batch_sql",
    }


def _get_batch_info(debug_id):
    return {
        "params": _get_batch_params(),
        "job_name": debug_id,
        "job_id": debug_id,
        "run_mode": "udf_debug",
        "job_type": "spark_sql",
    }


def _generate_job_name(calculation_type, job_id):
    return "{}-udf-{}".format(calculation_type, job_id)


def _start_batch_debug(
    geog_area_code,
    cluster_id,
    function_config,
    debug_id,
    debug_data,
    source_result_table_id,
    sql,
):
    debug_id = "batch-udf-" + debug_id
    job_info = {
        "run_mode": "udf_debug",
        "queue": BATCH_DEBUG_QUEUE,
        "info": _get_batch_info(debug_id),
        "nodes": {
            "source": _get_source_node(source_result_table_id, debug_data, "batch"),
            "transform": _get_spark_transform(sql, function_config, source_result_table_id),
        },
        "params": _get_batch_params(),
        "udf": _get_udf_info(geog_area_code, debug_data, function_config),
    }
    if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
        job_info["license"] = {
            "license_server_url": pizza_settings.LICENSE_SERVER_URL,
            "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
        }

    jobnavi_args = {
        "schedule_id": debug_id,
        "description": "udf debug Project %s" % debug_id,
        "type_id": "spark_sql",
        "active": True,
        "exec_oncreate": True,
        "extra_info": str(json.dumps(job_info)),
    }
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi.delete_schedule(debug_id)
    logger.info("submit batch debug job and args is " + str(jobnavi_args))
    execute_id = jobnavi.create_schedule_info(jobnavi_args)
    _get_jobnavi_execute_result(jobnavi, execute_id, debug_id)


def _start_stream_debug(
    geog_area_code,
    cluster_id,
    function_config,
    common_debug_id,
    debug_data,
    source_result_table_id,
    sql,
):
    """
    启动实时 UDF 调试任务
    @param geog_area_code:
    @param cluster_id:
    @param function_config:
    @param common_debug_id: 通用 debug_id， 实时和离线 debug_id 共用该值作为后缀
    @param debug_data:
    @param source_result_table_id:
    @param sql:
    @return:
    """
    one_of_cluster = FunctionHandler(function_config.__name__).list_clusters()[0]
    config_hdfs = ConfigHDFS(one_of_cluster)
    job_info = {
        "job_id": common_debug_id,
        "job_type": "flink",
        "run_mode": "udf_debug",
        "api_url": {"base_dataflow_url": "http://%s/v3/dataflow/" % API_URL},
        "jobnavi": {
            "udf_path": [
                "%s/app/udf/%s/dev/%s.jar"
                % (
                    config_hdfs.get_url(),
                    function_config.__name__,
                    function_config.__name__,
                )
            ]
        },
    }
    # delete the debug id's debug result data
    debug_handler.delete_debug_result_by_debug_id(common_debug_id, "stream")
    logger.info("submit stream job and args is  " + str(job_info))
    _submit_stream_debug_by_jobnavi(
        geog_area_code,
        cluster_id,
        _generate_job_name("stream", common_debug_id),
        job_info,
    )


def _submit_stream_debug_by_jobnavi(geog_area_code, cluster_id, job_name, job_info):
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    execute_data = jobnavi.get_execute_result_by_status(DEBUG_RESOURCE_ID, "running")
    execute_id = execute_data[0]["execute_info"]["id"]
    event_info = {
        "job_name": job_name,
        "parallelism": 1,
        "jar_file_name": _get_stream_jar(),
        "argument": str(json.dumps(job_info)),
    }
    if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
        event_info["license"] = str(
            json.dumps(
                {
                    "license_server_url": pizza_settings.LICENSE_SERVER_URL,
                    "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
                }
            )
        )
    args = {
        "event_name": "run_job",
        "execute_id": execute_id,
        "event_info": str(json.dumps(event_info)),
    }
    event_id = jobnavi.send_event(args)
    logger.info("[run stream udf debug] execute id {} and event id is {}".format(execute_id, event_id))
    _get_jobnavi_event_result(jobnavi, event_id, cluster_id, geog_area_code, execute_id, job_name)


def _get_jobnavi_event_result(jobnavi, event_id, cluster_id, geog_area_code, execute_id, job_name):
    for i in range(DEBUG_SUBMIT_JOB_TIMEOUT):
        data = jobnavi.get_event(event_id)
        logger.info("[get_run_cancel_job_result] event id {} and result data is {}".format(event_id, data))

        if data["is_processed"] and data["process_success"]:
            return data["process_success"]
        elif (
            data["is_processed"]
            and not data["process_success"]
            and data["process_info"]
            and is_json(data["process_info"])
            and json.loads(data["process_info"])["code"]
            and json.loads(data["process_info"])["code"] == 1571022
        ):
            raise YarnResourcesLackError()
        elif data["is_processed"] and not data["process_success"]:
            search_words = job_name + "_error"
            raise DebugJobSumitError(get_submit_exception_info(cluster_id, geog_area_code, execute_id, search_words))
        time.sleep(1)
    raise DebugJobRunError()


def get_submit_exception_info(cluster_id, geog_area_code, execute_id, search_words):
    file_size_result = FlowHelper.get_flow_job_submit_log_file_size(
        execute_id, cluster_id=cluster_id, geog_area_code=geog_area_code
    )
    file_size = file_size_result["file_size"]
    exception_message = None
    begin = 0
    end = begin + 20 * 1024  # 20KB
    while not exception_message and end < file_size:
        submit_log = FlowHelper.get_flow_job_submit_log(
            execute_id,
            begin,
            end,
            search_words=search_words,
            cluster_id=cluster_id,
            geog_area_code=geog_area_code,
        )
        if "log_data" in submit_log and submit_log["log_data"]:
            log_data = submit_log["log_data"][0]
            if "inner_log_data" in log_data and log_data["inner_log_data"] and len(log_data["inner_log_data"]) > 0:
                exception_message = log_data["inner_log_data"][0]["log_content"].replace("&lt;br&gt;", "\n")
                break
        begin = end
        end = begin + 20 * 1024 if begin + 20 * 1024 < file_size else file_size
    return exception_message


def is_json(string):
    try:
        json.loads(string)
    except ValueError:
        return False
    return True


def _get_jobnavi_execute_result(jobnavi, execute_id, debug_id):
    try:
        for i in range(DEBUG_SUBMIT_JOB_TIMEOUT):
            data = jobnavi.get_execute_status(execute_id)
            logger.info("submit batch udf debug execute id is " + execute_id)
            if not data or data["status"] == "running" or data["status"] == "preparing":
                time.sleep(1)
            elif data["status"] == "failed":
                error_info = _get_error_info(debug_id, CalculationType.batch.name)
                if error_info:
                    raise DebugJobSumitError(error_info)
                else:
                    raise DebugJobRunError()
            elif data["status"] == "finished":
                break
    except Exception as e:
        logger.exception(e)
        raise e


def _get_error_info(debug_id, job_type):
    error_info = debug_handler.get_debug_error_log_for_processing(debug_id=debug_id, job_type=job_type)
    if error_info and error_info[0]:
        return error_info[0]["error_message"]
    else:
        return None


def _get_flink_transform(sql, function_config):
    parse_args = {
        "sql": sql,
        "properties": {
            "flink.result_table_name": "debug_%s" % function_config.__name__,
            "flink.bk_biz_id": 591,
            "flink.system_fields": [
                {
                    "field": "dtEventTime",
                    "type": "string",
                    "origins": "",
                    "description": "time",
                }
            ],
            "flink.window_type": "tumbling" if function_config.func_udf_type == "udaf" else None,
            "flink.static_data": None,
            "flink.window_length": 0,
            "flink.waiting_time": 0,
            "flink.count_freq": 5 if function_config.func_udf_type == "udaf" else 0,
            "flink.expired_time": 0,
            "flink.session_gap": 0,
            "flink.env": "dev",
            "flink.function_name": function_config.__name__,
        },
    }
    parsed_result = BksqlHelper.parse_flink_sql(parse_args)
    if len(parsed_result) != 1:
        raise ParseSqlError()
    transform_result_table = parsed_result[0]
    bk_biz_id, table_name = transform_result_table["id"].split("_", 1)
    transform_result_table["name"] = table_name + "_" + bk_biz_id
    return {parsed_result[0]["id"]: parsed_result[0]}


def _get_stream_params():
    return {
        "deploy_mode": None,
        "cluster_name": None,
        "resources": 1,
        "jar_name": _get_stream_jar(),
        "deploy_config": None,
    }


def _get_stream_jar():
    version = processing_version_config.where(component_type="flink", branch="master").aggregate(Max("version"))[
        "version__max"
    ]
    return "master-%s.jar" % version


def _get_stream_info(debug_id):
    return {
        "job_id": debug_id,
        "debug_id": debug_id,
        "job_name": debug_id,
        "project_version": "1",
        "job_type": "flink",
        "run_mode": "udf_debug",
        "time_zone": UC_TIME_ZONE,
        "metric_kafka_server": METRIC_KAFKA_SERVER,
        "metric_kafka_topic": METRIC_KAFKA_TOPIC,
        "checkpoint_redis_host": None,
        "checkpoint_redis_port": 0,
        "checkpoint_redis_password": None,
        "checkpoint_redis_sentinel_host": None,
        "checkpoint_redis_sentinel_port": 0,
        "checkpoint_redis_sentinel_name": None,
        "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
        "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
        "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
        "params": _get_stream_params(),
    }


def _get_source_node(result_table_id, debug_data, calculation_type):
    bk_biz_id, table_name = result_table_id.split("_", 1)
    fields = []
    for tmp_field in debug_data["schema"]:
        one_field = {
            "field": tmp_field["field_name"],
            "type": fix_filed_type(tmp_field["field_type"]),
            "origin": "",
            "description": tmp_field["field_name"],
        }
        fields.append(one_field)
    if calculation_type == "stream":
        input_info = {"type": "string"}
    else:
        input_info = None
    source = {
        result_table_id: {
            "id": result_table_id,
            "input": input_info,
            "name": table_name + "_" + bk_biz_id,
            "description": result_table_id,
            "fields": fields,
        }
    }
    return source


def get_result_data(debug_id):
    def remove_dt_filed(stream_result_data):
        if "dtEventTime" in stream_result_data:
            del stream_result_data["dtEventTime"]
        return stream_result_data

    batch_debug_id = "batch-udf-" + debug_id
    stream_rows = debug_handler.get_debug_result_data_by_debug_id(debug_id, "stream")
    batch_rows = debug_handler.get_debug_result_data_by_debug_id(batch_debug_id, "batch")
    stream_result_datas = []
    batch_result_datas = []
    if stream_rows:
        stream_result_datas = list(remove_dt_filed(json.loads(row.result_data)) for row in stream_rows)
    if batch_rows:
        batch_result_datas = [json.loads(json.loads(row.result_data)) for row in batch_rows]
    return {"stream": stream_result_datas, "batch": batch_result_datas}


def generate_job_config(job_id, job_type):
    udf_debug_log = bksql_function_debug_log.get(debug_id=job_id)
    function_config = bksql_function_dev_config.get(debug_id=job_id, version="dev")
    if job_type == "flink":
        geog_area_code = TagHelper.get_geog_area_code_by_rt_id(udf_debug_log.source_result_table_id)
        job_config = {
            "job_id": job_id,
            "job_name": _generate_job_name("stream", job_id),
            "job_type": "flink",
            "run_mode": "udf_debug",
            "time_zone": UC_TIME_ZONE,
            "checkpoint": {
                "start_position": "from_head",
                "manager": "dummy",
                "checkpoint_redis_host": None,
                "checkpoint_redis_port": 0,
                "checkpoint_redis_password": None,
                "checkpoint_redis_sentinel_host": None,
                "checkpoint_redis_sentinel_port": 0,
                "checkpoint_redis_sentinel_name": None,
            },
            "metric": {
                "metric_kafka_server": None,
                "metric_kafka_topic": None,
            },
            "debug": {
                "debug_id": job_id,
                "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
                "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
                "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
            },
            "nodes": {
                "source": _get_source_node(
                    udf_debug_log.source_result_table_id,
                    json.loads(udf_debug_log.debug_data),
                    "stream",
                ),
                "transform": _get_flink_transform(udf_debug_log.sql, function_config),
            },
            "udf": _get_udf_info(geog_area_code, json.loads(udf_debug_log.debug_data), function_config),
        }
        return job_config
    else:
        raise UdfExpectedException(_("不支持的 API 调用，调试异常，请联系管理员."))


def get_runtime_exception(calculation_types, debug_id, result_table_id):
    """
    获取任务运行时异常
    """
    if CalculationType.batch.name in calculation_types:
        raise DebugJobRunError()  # todo 离线udf运行时异常
    if CalculationType.stream.name in calculation_types:
        geog_area_code = TagHelper.get_geog_area_code_by_rt_id(result_table_id)
        cluster_id = JobNaviHelper.get_jobnavi_cluster(CalculationType.stream.name)
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        execute_data = jobnavi.get_execute_result_by_status(DEBUG_RESOURCE_ID, "running")
        execute_id = execute_data[0]["execute_info"]["id"]
        event_info = {"job_name": _generate_job_name("stream", debug_id)}
        args = {
            "event_name": "job_exception",
            "execute_id": execute_id,
            "event_info": str(json.dumps(event_info)),
        }
        event_id = jobnavi.send_event(args)
        logger.info("[get udf debug job runtime error] execute id {} and event id is {}".format(execute_id, event_id))
        for i in range(DEBUG_GET_FLINK_EXCEPTION_TIMEOUT):
            data = jobnavi.get_event(event_id)
            logger.info("[get udf debug job runtime error result] event id {} and result is {}".format(event_id, data))
            if not data["is_processed"]:
                time.sleep(1)
            elif data["process_success"]:
                return _parse_stream_runtime_exception(data["process_info"])
            else:
                return None
    else:
        return None


def _parse_stream_runtime_exception(process_info):
    if is_json(process_info) and "root-exception" in json.loads(process_info):
        return json.loads(process_info)["root-exception"]
    else:
        return None
