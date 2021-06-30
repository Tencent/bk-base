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

from dataflow.batch.api.api_helper import BksqlHelper
from dataflow.batch.exceptions.comp_execptions import SparkSQLCheckException
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.udf.functions import function_driver


def parse_udf(sql, project_id):
    geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
    return function_driver.parse_sql(sql, geog_area_code)


def call_sparksql(sql, input_tables, output_table, udfs, is_self_dependency=False, output_schema=None):
    spark_sql_properties = __construct_sparksql_param(
        input_tables,
        output_table,
        udfs,
        is_self_dependency=is_self_dependency,
        output_schema=output_schema,
    )

    spark_sql_rtns = BksqlHelper.spark_sql(sql, spark_sql_properties)
    batch_logger.info(spark_sql_rtns)
    if spark_sql_rtns and len(spark_sql_rtns) > 0:
        spark_sql_rtn = spark_sql_rtns[0]
        return spark_sql_rtn
    else:
        batch_logger.error("bksql_spark_sql_error:bksql_解析错误.    {}".format(spark_sql_rtns))
        raise SparkSQLCheckException("SQL校验失败:{}".format(spark_sql_rtns))


def call_one_time_sparksql(sql, input_tables, output_table, udfs):
    spark_sql_properties = __construct_sparksql_param(
        input_tables, output_table, udfs, is_self_dependency=False, output_schema=None
    )

    spark_sql_rtns = BksqlHelper.one_time_spark_sql(sql, spark_sql_properties)
    batch_logger.info(spark_sql_rtns)
    if spark_sql_rtns and len(spark_sql_rtns) > 0:
        spark_sql_rtn = spark_sql_rtns[0]
        return spark_sql_rtn
    else:
        batch_logger.error("bksql_spark_sql_error:bksql_解析错误.    {}".format(spark_sql_rtns))
        raise SparkSQLCheckException("SQL校验失败:{}".format(spark_sql_rtns))


def retrieve_dimension_fields(sql):
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
        batch_logger.error("Failed to get dimension info by bksql.")
    return dimension_fields


def __construct_sparksql_param(input_tables, output_table, udfs, is_self_dependency, output_schema):
    bk_biz_id = int(str(output_table).split("_")[0])

    spark_sql_properties = {
        "spark.result_table_name": output_table,
        "spark.bk_biz_id": bk_biz_id,
        "spark.input_result_table": input_tables,
        "spark.dataflow_udf_env": "product",
    }

    dataflow_udf_function_name = ""
    if udfs:
        dataflow_udf_function_name = ",".join(udf["name"] for udf in udfs)
    spark_sql_properties["spark.dataflow_udf_function_name"] = dataflow_udf_function_name

    self_dependency_properties = {}

    if is_self_dependency:
        self_dependency_properties = {
            "table_name": output_table,
            "table_fields": output_schema,
        }

    spark_sql_properties["spark.self_dependency_config"] = self_dependency_properties
    return spark_sql_properties
