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

from conf.dataapi_settings import (
    IP_REDIS_V4_GSLB_RESULT_TABLE_ID,
    IP_REDIS_V4_TGEO_BASE_NETWORK_RESULT_TABLE_ID,
    IP_REDIS_V4_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID,
    IP_REDIS_V6_GSLB_RESULT_TABLE_ID,
    IP_REDIS_V6_TGEO_BASE_NETWORK_RESULT_TABLE_ID,
    IP_REDIS_V6_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID,
    IPV4_RESULT_TABLE_ID,
    IPV6_RESULT_TABLE_ID,
)
from django.utils.translation import ugettext as _

import dataflow.shared.handlers.processing_udf_info as processing_udf_info
import dataflow.shared.handlers.processing_udf_job as processing_udf_job
from dataflow.batch.api.api_helper import BksqlHelper
from dataflow.batch.exceptions.comp_execptions import (
    BatchIllegalArgumentError,
    BatchInfoNotFoundException,
    BatchSQLNodeCheckException,
    SparkSQLCheckException,
)
from dataflow.batch.handlers.dataflow_control_list import DataflowControlListHandler
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.utils import batch_db_util, bksql_util
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.lineage.lineage_helper import LineageHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.udf.functions import function_driver

batch_reserved_fields = {"dtEventTime": "string", "dtEventTimeStamp": "timestamp"}
batch_forbidden_fields = {"thedate"}


def make_result_table(args, udfs):
    prop = args["dict"]
    # 目前仅支持一个输出 todo
    output = args["outputs"][0]
    table_name = output["table_name"]
    sql = args["sql"]
    parent_tables = args["result_tables"]
    result_table_id = str(output["bk_biz_id"]) + "_" + table_name
    count_freq_unit = "H" if prop["schedule_period"] == "hour" else "d"
    result_table = {
        "bk_biz_id": output["bk_biz_id"],
        "project_id": args["project_id"],
        "result_table_id": result_table_id,
        "result_table_name": table_name,
        "result_table_name_alias": prop["description"],
        "processing_type": "batch",
        "count_freq": prop["count_freq"],
        "count_freq_unit": count_freq_unit,
        "description": prop.get("description") or "{}{}".format(_("离线计算-"), table_name),
    }
    # TODO:不在黑名单, （包括tdw任务）, 走新spark-sql协议
    if DataflowControlListHandler.is_in_bksql_spark_sql_black_list(result_table_id) is False:
        batch_logger.info("result_table({}): {}".format(result_table_id, "走新spark-sql协议。"))
        spark_sql_rtn = __get_bksql_spark_sql(args, result_table, sql, udfs)
        result_table["fields"] = spark_sql_rtn["fields"]
        result_table["created_by"] = args["bk_username"]
        # 修改SQL
        args["sql"] = spark_sql_rtn["sql"]

        dimensions_fields = bksql_util.retrieve_dimension_fields(sql)
        for tmp_field in result_table["fields"]:
            tmp_field["is_dimension"] = True if tmp_field["field_name"] in dimensions_fields else False
        return result_table


def __get_bksql_spark_sql(args, result_table, sql, udfs):
    parent_result_table = list(args["result_tables"].keys())
    if "static_data" in args:
        parent_result_table = parent_result_table + args["static_data"]
    spark_sql_properties = {
        "spark.result_table_name": result_table["result_table_id"],
        "spark.bk_biz_id": result_table["bk_biz_id"],
        "spark.input_result_table": parent_result_table,
        "spark.dataflow_udf_env": "product",
    }
    dataflow_udf_function_name = ""
    if udfs:
        dataflow_udf_function_name = ",".join(udf["name"] for udf in udfs)
    spark_sql_properties["spark.dataflow_udf_function_name"] = dataflow_udf_function_name

    self_dependency_properties = {}
    is_self_dependency_enable = False
    if "advanced" in args["dict"]:
        is_self_dependency_enable = args["dict"]["advanced"]["self_dependency"]
        self_dependency_config = args["dict"]["advanced"]["self_dependency_config"]
    if is_self_dependency_enable:
        self_dependency_properties = {
            "table_name": result_table["result_table_id"],
            "table_fields": self_dependency_config["fields"],
        }
    spark_sql_properties["spark.self_dependency_config"] = self_dependency_properties

    spark_sql_rtns = BksqlHelper.spark_sql(sql, spark_sql_properties)
    batch_logger.info(spark_sql_rtns)
    if spark_sql_rtns and len(spark_sql_rtns) > 0:
        spark_sql_rtn = spark_sql_rtns[0]
        return spark_sql_rtn
    else:
        batch_logger.error("bksql_spark_sql_error:bksql_解析错误.    {}".format(spark_sql_rtns))
        raise SparkSQLCheckException("SQL校验失败:{}".format(spark_sql_rtns))


def make_mapping_data_processing(args):
    prop = args["dict"]
    processing_id = args["processing_id"]
    processing_input = []
    for parent_name in list(args["result_tables"].keys()):
        if args["dict"]["batch_type"] == "tdw_jar" or args["dict"]["batch_type"] == "tdw":
            parent_storage_cluster_config_id = ResultTableHelper.get_result_table_storage(parent_name, "tdw")["tdw"][
                "storage_cluster"
            ]["storage_cluster_config_id"]
        else:
            parent_storage_cluster_config_id = ResultTableHelper.get_result_table_storage(parent_name, "hdfs")["hdfs"][
                "storage_cluster"
            ]["storage_cluster_config_id"]
        processing_input.append(
            {
                "data_set_type": "result_table",
                "data_set_id": parent_name,
                "storage_cluster_config_id": parent_storage_cluster_config_id,
                "storage_type": "storage",
                "tags": [],
            }
        )

    if "static_data" in args:
        for static_data in args["static_data"]:
            static_data_cluster_config_id = ResultTableHelper.get_result_table_storage(static_data, "ignite")["ignite"][
                "storage_cluster"
            ]["storage_cluster_config_id"]
            processing_input.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": static_data,
                    "storage_cluster_config_id": static_data_cluster_config_id,
                    "storage_type": "storage",
                    "tags": ["static_join"],
                }
            )

    processings_args = {
        "project_id": args["project_id"],
        "processing_id": processing_id,
        "processing_alias": processing_id,
        "processing_type": "batch",
        "created_by": args["bk_username"],
        "description": prop["description"],
        "inputs": processing_input,
        "outputs": [{"data_set_type": "result_table", "data_set_id": processing_id}],
        "tags": args["tags"],
    }
    # data_processing outputs支持多输出
    outputs = []
    for output in args["outputs"]:
        table_name = output["table_name"]
        result_table_id = str(output["bk_biz_id"]) + "_" + table_name
        outputs.append({"data_set_type": "result_table", "data_set_id": result_table_id})
    processings_args["outputs"] = outputs
    return processings_args


def __add_extra(result_tables):
    """
    "result_tables": {
        "104_XXX": {
         "end_fallback_window": 0,
         "start_fallback_window": 1,
         "accumulate": false
        }
    }
    """
    for result_table_id in result_tables:
        result_table = ResultTableHelper.get_result_table(result_table_id)
        result_tables[result_table_id]["type"] = result_table["processing_type"]
        result_tables[result_table_id]["is_managed"] = result_table["is_managed"] if "is_managed" in result_table else 1


def save_batch_processing_info(args, outputs=None, result_tables_schema=None):
    args["batch_id"] = args["processing_id"]
    __add_extra(args["result_tables"])
    # 添输出RT的schema信息
    args["dict"]["output_schema"] = result_tables_schema
    args["dict"]["result_tables"] = args["result_tables"]
    if "static_data" in args:
        static_data = filter_static_data(args["static_data"])
        args["dict"]["static_data"] = static_data
    if outputs:
        args["dict"]["outputs"] = outputs
    return ProcessingBatchInfoHandler.save_proc_batch_info(args)


def update_processing_batch_info(args, processing_id, outputs=None, result_tables_schema=None):
    args["processing_id"] = processing_id
    args["batch_id"] = processing_id
    __add_extra(args["result_tables"])
    # 添输出RT的schema信息
    args["dict"]["output_schema"] = result_tables_schema
    args["dict"]["result_tables"] = args["result_tables"]
    if "static_data" in args:
        static_data = filter_static_data(args["static_data"])
        args["dict"]["static_data"] = static_data
    if outputs:
        args["dict"]["outputs"] = outputs
    return ProcessingBatchInfoHandler.update_proc_batch_info(args)


def delete_udf_related_info(processing_id):
    processing_udf_job.delete(job_id=processing_id, processing_type="batch")
    processing_udf_info.delete(processing_id=processing_id, processing_type="batch")


def parse_udf(sql, project_id):
    geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
    return function_driver.parse_sql(sql, geog_area_code)


def filter_static_data(static_data_list):
    special_static_data = IPV4_RESULT_TABLE_ID + IPV6_RESULT_TABLE_ID
    special_static_data = (
        special_static_data
        + IP_REDIS_V4_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID
        + IP_REDIS_V6_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID
    )
    special_static_data = (
        special_static_data
        + IP_REDIS_V4_TGEO_BASE_NETWORK_RESULT_TABLE_ID
        + IP_REDIS_V6_TGEO_BASE_NETWORK_RESULT_TABLE_ID
    )
    special_static_data = special_static_data + IP_REDIS_V4_GSLB_RESULT_TABLE_ID + IP_REDIS_V6_GSLB_RESULT_TABLE_ID
    static_data = deepcopy(static_data_list)
    for data in static_data:
        if data in special_static_data:
            static_data.remove(data)
    return static_data


def handle_static_data_and_storage_type(args):
    result_table = {}
    if "static_data" in args:
        for static_rt in args["static_data"]:
            result_table[static_rt] = {"storage_type": "ignite"}
            result_table[static_rt]["window_size"] = -1
            result_table[static_rt]["window_delay"] = 0
            result_table[static_rt]["window_size_period"] = "hour"
    __add_extra(result_table)

    if "result_tables" in args:
        for rt in args["result_tables"]:
            result_table[rt] = args["result_tables"][rt]
            result_table[rt]["storage_type"] = "hdfs"
    return result_table


def get_processing_batch_info(processing_id):
    if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
        return ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
    else:
        raise BatchInfoNotFoundException(message=_("不存在的data_processing_id(%s)" % processing_id))


def get_processing_job_info(job_id):
    return ProcessingJobInfoHandler.get_proc_job_info(job_id)


def __get_count_freq(count_freq, schedule_period):
    if schedule_period == "day":
        return count_freq * 24, "hour"
    elif schedule_period == "week":
        return count_freq * 24 * 7, "hour"
    elif schedule_period == "month":
        return count_freq, "month"
    else:
        return count_freq, "hour"


def valid_parent_param(args):
    # 检查父表，判断子表的统计频率是否为父表的倍数
    parents = args["result_tables"]
    processing_id = args["processing_id"]

    start_time_hash_set = set()
    if parents:
        for parent_id in list(parents.keys()):
            result_table = ResultTableHelper.get_result_table(parent_id, related="data_processing")
            result_table_type = result_table["processing_type"]
            if result_table_type == "batch" and is_managed(result_table):
                if "data_processing" in result_table and "processing_id" in result_table["data_processing"]:
                    parent_processing_id = result_table["data_processing"]["processing_id"]
                else:
                    parent_processing_id = parent_id

                parent_batch_info = get_processing_batch_info(parent_processing_id)
                __valid_parent_frequency(
                    processing_id,
                    args["dict"]["count_freq"],
                    args["dict"]["schedule_period"],
                    parent_id,
                    parent_batch_info.count_freq,
                    parent_batch_info.schedule_period,
                )
                submit_args = json.loads(parent_batch_info.submit_args)

                is_self_dependency = False
                if "advanced" in submit_args and "self_dependency" in submit_args["advanced"]:
                    is_self_dependency = submit_args["advanced"]["self_dependency"]

                # 确保当前节点多个分支的父节点的启动时间应该保持一致
                if "advanced" in submit_args and "start_time" in submit_args["advanced"] and not is_self_dependency:
                    start_time_hash_set.add(submit_args["advanced"]["start_time"])
                else:
                    start_time_hash_set.add(None)

                if len(start_time_hash_set) > 1:
                    raise BatchIllegalArgumentError("上游节点离线结果表开始时间需要保持一致")

            elif result_table_type == "batch" and not is_managed(result_table):
                __valid_parent_frequency(
                    processing_id,
                    args["dict"]["count_freq"],
                    args["dict"]["schedule_period"],
                    parent_id,
                    result_table["count_freq"],
                    result_table["count_freq_unit"],
                    False,
                )


def valid_child_param(args):
    MAX_DEPTH = -1
    processing_id = args["processing_id"]
    # 检查子表，判断子表的统计频率是否为父表的倍数
    lineage = LineageHelper.get_lineage("result_table", processing_id, MAX_DEPTH, "OUTPUT")
    if lineage:
        nodes = lineage["nodes"]
        for node in list(nodes.keys()):
            if nodes[node]["type"] == "data_processing":
                child_id = nodes[node]["processing_id"]
                result_table = ResultTableHelper.get_result_table(child_id, related="data_processing")
                result_table_type = result_table["processing_type"]
                if result_table_type == "batch" and is_managed(result_table):
                    if "data_processing" in result_table and "processing_id" in result_table["data_processing"]:
                        child_processing_id = result_table["data_processing"]["processing_id"]
                    else:
                        child_processing_id = child_id

                    batch_info = get_processing_batch_info(child_processing_id)

                    # 上游为离线计算节点数据作为下游的静态关联节点时不进行校验
                    submit_args = json.loads(batch_info.submit_args)
                    if (
                        "static_data" in submit_args
                        and processing_id in submit_args["static_data"]
                        and processing_id not in submit_args["result_tables"]
                    ):
                        return
                    __valid_child_frequency(
                        processing_id,
                        args["dict"]["count_freq"],
                        args["dict"]["schedule_period"],
                        child_id,
                        batch_info.count_freq,
                        batch_info.schedule_period,
                    )


def valid_current_param(args):
    frequency_unit = args["dict"]["schedule_period"]
    frequency = args["dict"]["count_freq"]
    delay = args["dict"]["delay"]
    __valid_current_delay(frequency, frequency_unit, delay, "hour")
    __valid_current_frequency(frequency, frequency_unit)

    parents = args["result_tables"]
    # 验证父表窗口单位需与周期单位一致
    for parent_id in parents:
        if not args["dict"]["accumulate"]:
            __valid_current_window(
                frequency,
                frequency_unit,
                parents[parent_id]["window_size"],
                parents[parent_id]["window_size_period"],
            )


def __valid_current_window(frequency, frequency_unit, parent_window, parent_window_unit):
    if parent_window < frequency:
        raise BatchSQLNodeCheckException(_("窗口长度必须大于或等于统计频率"))
    if str(frequency_unit).lower() != str(parent_window_unit).lower():
        raise BatchSQLNodeCheckException(_("验证父表窗口单位需与周期单位一致"))


def __valid_parent_frequency(
    current_id,
    current_frequency,
    current_frequency_unit,
    parent_id,
    parent_frequency,
    parent_frequency_unit,
    is_managed=True,
):

    count_freq, freq_unit = __get_count_freq(current_frequency, current_frequency_unit)
    if is_managed:
        parent_count_freq, parent_freq_unit = __get_count_freq(parent_frequency, parent_frequency_unit)
        is_valided = True
        if freq_unit == parent_freq_unit:
            if not count_freq % parent_count_freq == 0:
                is_valided = False
        elif freq_unit == "month" and parent_freq_unit == "hour":
            month_hour_list = get_month_hour_list()
            for item in month_hour_list:
                if not item % parent_count_freq == 0:
                    is_valided = False
        else:
            is_valided = False

        if not is_valided:
            raise BatchSQLNodeCheckException(
                message=_(
                    "子节点(%(processing_id)s)周期必须为父节点(%(parent_id)s)周期的倍数"
                    % {"processing_id": current_id, "parent_id": parent_id}
                )
            )
    else:
        if not parent_frequency_unit:
            pass
        elif "H" == parent_frequency_unit:
            parent_count_freq = parent_frequency
            if not count_freq % parent_count_freq == 0:
                raise BatchSQLNodeCheckException(
                    message=_(
                        "子节点(%(processing_id)s)周期必须为父节点(%(parent_id)s)周期的倍数"
                        % {"processing_id": current_id, "parent_id": parent_id}
                    )
                )
        elif "d" == parent_frequency_unit:
            parent_count_freq = parent_frequency * 24
            if not count_freq % parent_count_freq == 0:
                raise BatchSQLNodeCheckException(
                    message=_(
                        "子节点(%(processing_id)s)周期必须为父节点(%(parent_id)s)周期的倍数"
                        % {"processing_id": current_id, "parent_id": parent_id}
                    )
                )


def __valid_child_frequency(
    current_id,
    current_frequency,
    current_frequency_unit,
    parent_id,
    child_frequency,
    child_frequency_unit,
):

    count_freq, freq_unit = __get_count_freq(current_frequency, current_frequency_unit)
    child_count_freq, child_freq_unit = __get_count_freq(child_frequency, child_frequency_unit)

    is_valided = True
    if freq_unit == child_freq_unit:
        if not child_count_freq % count_freq == 0:
            is_valided = False
    elif child_freq_unit == "month" and freq_unit == "hour":
        month_hour_list = get_month_hour_list()
        for item in month_hour_list:
            if not item % count_freq == 0:
                is_valided = False
    else:
        is_valided = False

    if not is_valided:
        raise BatchSQLNodeCheckException(
            message=_(
                "子节点(%(child_id)s)周期必须为父节点(%(processing_id)s)周期的倍数"
                % {"child_id": parent_id, "processing_id": current_id}
            )
        )


def __valid_current_delay(current_frequency, current_frequency_unit, delay, delay_unit):

    if delay_unit not in ["hour", "day"]:
        raise BatchSQLNodeCheckException(_("延迟单位必须是小时或者天"))

    if str(delay_unit).lower() == "day":
        delay = delay * 24

    if current_frequency_unit in ["hour", "day"]:
        if delay >= 24:
            raise BatchSQLNodeCheckException(_("必须小于 24 小时"))
    else:
        # TODO: fixed_delay 目前是按小时、天计，与调度频率进行比较需要月按某个具体天数转成小时，目前保守按 28 计
        # week, month
        if (current_frequency_unit == "week" and delay > 24 * 7 * current_frequency) or (
            current_frequency_unit == "month" and delay > 24 * 28 * current_frequency
        ):
            raise BatchSQLNodeCheckException(_("必须小于一个统计频率周期"))


def __valid_current_frequency(current_frequency, current_frequency_unit):
    if str(current_frequency_unit).lower() == "hour":
        valid_arr = [1, 2, 3, 4, 6, 12]
        if current_frequency not in valid_arr:
            raise BatchSQLNodeCheckException(_("统计频率目前仅支持 %s") % valid_arr)


def check_batch_param(args):
    if args["scheduling_type"] != "batch":
        raise BatchSQLNodeCheckException(_("仅支持batch校验"))
    current_frequency = args["scheduling_content"]["count_freq"]
    current_frequency_unit = args["scheduling_content"]["schedule_period"]
    __valid_current_frequency(current_frequency, current_frequency_unit)
    is_accumulate = True if args["scheduling_content"]["window_type"] == "accumulate_by_hour" else False

    if not is_accumulate:
        delay = args["scheduling_content"]["fixed_delay"]
        delay_unit = args["scheduling_content"]["delay_period"]
        __valid_current_delay(current_frequency, current_frequency_unit, delay, delay_unit)
        if args["scheduling_content"]["dependency_config_type"] == "custom":
            parent_windows = args["scheduling_content"]["custom_config"]
            for node in parent_windows:
                node_window = parent_windows[node]
                __valid_current_window(
                    current_frequency,
                    current_frequency_unit,
                    node_window["window_size"],
                    node_window["window_size_period"],
                )
        else:
            __valid_current_window(
                current_frequency,
                current_frequency_unit,
                args["scheduling_content"]["unified_config"]["window_size"],
                args["scheduling_content"]["unified_config"]["window_size_period"],
            )
    else:
        delay = args["scheduling_content"]["delay"]
        delay_unit = "hour"
        __valid_current_delay(current_frequency, current_frequency_unit, delay, delay_unit)

    if "from_nodes" in args:
        for node in args["from_nodes"]:
            if node["scheduling_type"] == "batch":
                __valid_parent_frequency(
                    "",
                    current_frequency,
                    current_frequency_unit,
                    "",
                    node["scheduling_content"]["count_freq"],
                    node["scheduling_content"]["schedule_period"],
                )

    if "to_nodes" in args:
        for node in args["to_nodes"]:
            if node["scheduling_type"] == "batch":
                __valid_child_frequency(
                    "",
                    current_frequency,
                    current_frequency_unit,
                    "",
                    node["scheduling_content"]["count_freq"],
                    node["scheduling_content"]["schedule_period"],
                )


def get_month_hour_list():
    return [31 * 24, 30 * 24, 29 * 24, 28 * 24]


def is_managed(result_table):
    return result_table["is_managed"] == 1


def update_udf(args, udfs):
    processing_udf_info.delete(processing_id=args["processing_id"])
    save_udf(args, udfs)


def save_udf(args, udfs):
    for udf in udfs:
        processing_udf_info.save(
            processing_id=args["processing_id"],
            processing_type="batch",
            udf_name=udf["name"],
            udf_info=json.dumps(udf),
        )


def get_udf(processing_id):
    udfs = processing_udf_info.where(processing_id=processing_id)
    return udfs


def get_valid_result_table_schema(result_tables, processing_id):
    """
    验证字段信息，并返回最终的表的schema信息。
    """
    job_result_tables = None
    if processing_id:
        processing_job_info = get_processing_job_info(processing_id)
        if processing_job_info:
            job_config = processing_job_info.job_config
            submit_args = json.loads(json.loads(job_config)["submit_args"])
            job_result_tables = submit_args["output_schema"] if "output_schema" in submit_args else None
            # TODO 需要使用元数据做校验，或者之前执行的SQL做校验
    result_tables_schema = {}
    batch_logger.info("valid_result_tables_fields-1:" + json.dumps(result_tables))
    batch_logger.info("valid_result_tables_fields-2:" + json.dumps(job_result_tables))
    for result_table in result_tables:
        result_table_id = result_table["result_table_id"]
        # 深拷贝副本，用于存储存储schema
        new_fields = deepcopy(result_table["fields"])
        old_fields = []
        if job_result_tables:
            if result_table_id in job_result_tables:
                old_fields = job_result_tables[result_table_id]
                if not old_fields:
                    old_fields = []
                # l、类型校验
                __valid_fields_type(new_fields, old_fields)
        # 构造存储的schema
        result_tables_schema[result_table_id] = __make_result_tables_fields(new_fields, old_fields)
    return result_tables_schema


def __make_result_tables_fields(new_fields, old_fields):
    """
    构建存储的schema
    1、如果新的字段中，比旧的字段少，则在存储中标识这个字段不可用。
    """
    field_index = 0
    result_tables_fields = []
    fields_dict = {}
    for field in new_fields:
        field["is_enabled"] = 1
        field["field_index"] = field_index
        field_index = field_index + 1
        fields_dict[field["field_name"]] = field
        result_tables_fields.append(field)

    for old_field in old_fields:
        if old_field["field_name"] not in fields_dict:
            old_field["is_enabled"] = 0
            old_field["field_index"] = field_index
            field_index = field_index + 1
            fields_dict[old_field["field_name"]] = old_field
            result_tables_fields.append(old_field)
    return result_tables_fields


def __valid_fields_type(new_fields, old_fields):
    """
    不允许修改类型
    """
    for field in new_fields:
        for old_field in old_fields:
            if field["field_name"] == old_field["field_name"]:
                if __get_data_type(field["field_type"]) != __get_data_type(old_field["field_type"]):
                    raise SparkSQLCheckException(
                        _("结果字段 %(field_name)s 的类型为 %(field_type)s 与存储的类型 %(old_field_type)s 不一致。")
                        % {
                            "field_name": field["field_name"],
                            "field_type": field["field_type"],
                            "old_field_type": old_field["field_type"],
                        }
                    )


def __get_data_type(data_type):
    """
    获取数据类型，统一类型的不同描述
    :return:
    """
    if "integer" == data_type:
        data_type = "int"
    if "bigint" == data_type:
        data_type = "long"
    return data_type


def update_batch_job_advance_config(processing_id, params):
    batch_db_util.update_batch_advance_config(processing_id, params)
