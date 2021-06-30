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
import uuid

from dataflow.batch.api.api_helper import BksqlHelper
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.job.jobnavi_register_modeling import ModelingJobNaviRegister
from dataflow.modeling.settings import PARSED_TASK_TYPE, TABLE_TYPE
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.udf.functions import function_driver


def register_schedule(job_id, schedule_time, created_by, is_restart=False):
    # 离线已将相关操作重新封装整理，因此这里可以进行很大的简化
    jobnavi_register = ModelingJobNaviRegister(job_id, created_by, is_restart)
    return jobnavi_register.register_jobnavi(schedule_time)


def get_output_info(node_info):
    # 注意，这里是从前端传递的内容解析出输出字段
    # 此时真正的物理表可能还不存在 ，所以这里不能使用ResultTableHelper等相关的请求来获取请求的storage等信息
    output_table = None
    output_fields = []
    output_alias = None
    for table_id in node_info["output"]:
        output_table = table_id
        for field in node_info["output"][table_id]["fields"]:
            logger.info(field)
            output_field = {
                "field": field["field_name"],
                "type": field["field_type"],
                "description": field["field_alias"],
                "origin": [],
            }
            output_fields.append(output_field)
        output_alias = node_info["output"][table_id]["table_alias"]
    return {"name": output_table, "fields": output_fields, "alias": output_alias}


def get_input_info(dependence_info):
    input_table = None
    input_fileds = []
    input = {}

    for table_id in dependence_info:
        input_table = table_id
        result_table_fields = ResultTableHelper.get_result_table_fields(input_table)
        for filed_info in result_table_fields:
            input_table_field = {
                "field": filed_info["field_name"],
                "type": filed_info["field_type"],
                "origin": "",
                "description": filed_info["field_alias"],
            }
            input_fileds.append(input_table_field)
        result_table_storage = ResultTableHelper.get_result_table_storage(input_table, "hdfs")["hdfs"]
        input["type"] = "hdfs"
        input["format"] = result_table_storage["data_type"]
        result_table_connect = json.loads(result_table_storage["storage_cluster"]["connection_info"])
        input["path"] = "{hdfs_url}/{hdfs_path}".format(
            hdfs_url=result_table_connect["hdfs_url"],
            hdfs_path=result_table_storage["physical_table_name"],
        )
        input["table_type"] = TABLE_TYPE.RESULT_TABLE.value
        if input["format"] == "iceberg":
            iceberg_hdfs_config = StorekitHelper.get_hdfs_conf(input_table)
            iceberg_config = {
                "physical_table_name": result_table_storage["physical_table_name"],
                "hdfs_config": iceberg_hdfs_config,
            }
            input["iceberg_config"] = iceberg_config
    return {"name": input_table, "fields": input_fileds, "info": input}


def get_window_info(input_table, dependence_info, node_info):
    # 由于计算真正的数据路径时需要用到当前节点的周期配置（schedule_info）以及依赖表的配置(dependence)
    # 所以这里将两者合并在一起成为window_info，每个依赖表都有一个window信息
    # 所以在window的信息中有两部分，一部分是每个source都不一样的dependence 以及每个 source内值都一样的schedule_info
    # schedule_info表示的是当前节点的调试信息，与父任务无关，这里需要注意理解
    window_info = {}
    window_info.update(dependence_info[input_table])
    window_info.update(node_info["schedule_info"])
    return window_info


def update_process_and_job(table_name, processor_logic, submit_args):
    batch_processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_prefix(table_name)
    for processing_info in batch_processing_info:
        # 上述两者要更新加Processing
        ProcessingBatchInfoHandler.update_proc_batch_info_logic(processing_info.processing_id, processor_logic)
        ProcessingBatchInfoHandler.update_proc_batch_info_submit_args(processing_info.processing_id, submit_args)

    processing_job_info_list = ProcessingJobInfoHandler.get_proc_job_info_by_prefix(table_name)
    for processing_job_info in processing_job_info_list:
        job_config = json.loads(processing_job_info.job_config)
        job_config["submit_args"] = json.dumps(submit_args)
        job_config["processor_logic"] = json.dumps(processor_logic)
        ProcessingJobInfoHandler.update_proc_job_info_job_config(processing_job_info.job_id, job_config)
    return table_name


def get_sub_query_task(sql, sub_sql, target_entity_name, geog_area_code):
    """
    将子查询的相关信息封装为一个临时的task
    @param sql: 源mlsql
    @param sub_sql: 子查询
    @param target_entity_name: mlsql生成实例名称
    @param geog_area_code: area code
    @return: 临时的任务信息
    """
    uuid_str = str(uuid.uuid4())
    processing_id = target_entity_name + "_" + uuid_str[0 : uuid_str.find("-")]
    # 解析所有用到的udf
    udf_data = function_driver.parse_sql(sub_sql, geog_area_code)
    logger.info("udf data result:" + json.dumps(udf_data))
    udf_name_list = []
    for udf in udf_data:
        udf_name_list.append(udf["name"])

    # 解析用到的所有表
    sub_query_table_names = ModelingApiHelper.get_table_names_by_mlsql_parser({"sql": sql})
    logger.info("sub query table names:" + json.dumps(sub_query_table_names))

    spark_sql_propertiies = {
        "spark.input_result_table": sub_query_table_names,
        "spark.bk_biz_id": target_entity_name[0 : target_entity_name.find("_")],
        "spark.dataflow_udf_function_name": ",".join(udf_name_list),
        "spark.result_table_name": processing_id,
    }

    # 使用sparksql解析子查询的输出
    spark_sql_parse_result_list = BksqlHelper.spark_sql(sub_sql, spark_sql_propertiies)
    logger.info("spark sql result:" + json.dumps(spark_sql_parse_result_list))
    spark_sql_parse_result = spark_sql_parse_result_list[0]
    sub_query_fields = spark_sql_parse_result["fields"]
    sparksql_query_sql = spark_sql_parse_result["sql"]
    tmp_sub_query_fields = []
    for field in sub_query_fields:
        tmp_sub_query_fields.append(
            {
                "field": field["field_name"],
                "type": field["field_type"],
                "description": field["field_alias"],
                "index": field["field_index"],
                "origins": [""],
            }
        )
    # 解析子查询的输入中用到的所有列
    sql_columns_result = ModelingApiHelper.get_columns_by_mlsql_parser({"sql": sub_sql})
    logger.info("sql column result:" + json.dumps(sql_columns_result))
    processor = {
        "args": {
            "sql": sparksql_query_sql,
            "format_sql": sql_columns_result["sql"],  # 含有通配字符的sql
        },
        "type": "untrained-run",
        "name": "tmp_processor",
    }

    # 解析子查询用到的数据区间（目前暂无应用）
    sql_query_source_result = ModelingApiHelper.get_mlsql_query_source_parser({"sql": sub_sql})
    logger.info("sql query source result:" + json.dumps(sql_query_source_result))
    processor["args"]["time_range"] = sql_query_source_result

    # todo:根据所有输入表检查输入列是否存在,去掉不存在的输入列（这里认为不存在的即为经过as或其它重新命名得到的列）这里可以进一步优化
    sql_all_columns = sql_columns_result["columns"]
    sql_exist_columns = []
    for table in sub_query_table_names:
        table_fields = ResultTableHelper.get_result_table_fields(table)
        for field in table_fields:
            sql_exist_columns.append(field["field_name"])
    processor["args"]["column"] = list(set(sql_all_columns).intersection(set(sql_exist_columns)))

    tmp_subquery_task = {
        "table_name": processing_id,
        "fields": tmp_sub_query_fields,
        "parents": sub_query_table_names,
        "processor": processor,
        "interpreter": [],
        "processing_id": processing_id,
        "udfs": udf_data,
        "task_type": PARSED_TASK_TYPE.SUB_QUERY.value,
    }
    return tmp_subquery_task
