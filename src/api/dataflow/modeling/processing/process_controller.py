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

from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.exceptions.comp_exceptions import SingleSqlParseError
from dataflow.modeling.processing.bulk_params import DataflowProcessingBulkParams, DatalabProcessingBulkParams
from dataflow.modeling.processing.modeling_processing import DataflowTableOperator, DatalabTableOperator, TableOperator
from dataflow.modeling.processing.tensorflow.tensorflow_batch_operator import DataflowTensorFlowOperator


class ModelingProcessController(object):
    _processing_id = None

    def __init__(self, processing_id):
        self._processing_id = processing_id

    @classmethod
    def get_table_operator(cls, params):
        use_scenario = "datalab"
        if "use_scenario" in params:
            use_scenario = params["use_scenario"]
        else:
            params["use_scenario"] = use_scenario
        if use_scenario == "datalab":
            bulk_params = DatalabProcessingBulkParams(params)
            operator = DatalabTableOperator(bulk_params)
        elif use_scenario == "dataflow":
            component_type = params["component_type"]
            if component_type == "spark_mllib":
                bulk_params = DataflowProcessingBulkParams(params)
                operator = DataflowTableOperator(bulk_params)
            else:
                operator = DataflowTensorFlowOperator(params)
        return operator

    @classmethod
    def create_bulk_processing(cls, params):
        # return ModelingJobPipeLine.create_model_processing(params)
        operator = cls.get_table_operator(params)
        return operator.create_processing()

    @classmethod
    def parse_mlsql_tables(cls, sql_list):
        (
            read_table,
            write_table,
            read_model,
            write_model,
        ) = TableOperator.parse_sql_table(sql_list)
        return {
            "read": {"result_table": read_table, "model": read_model},
            "write": {"result_table": write_table, "model": write_model},
        }

    @classmethod
    def parse_ddl_tables(cls, sql_list):
        # ddl中对表有影响的操作有两种，drop及truncate,分别记录
        # ddl中不会包括对表数据的读取操作
        result = {"delete": {}, "write": {}}
        drop_table, drop_model, write_table = TableOperator.parse_ddl_table(sql_list)
        if drop_table:
            result["delete"]["result_table"] = drop_table
        if drop_model:
            result["delete"]["model"] = drop_model
        if write_table:
            result["write"]["result_table"] = write_table
        return result

    def multi_update_process(self, params):
        operator = self.get_table_operator(params)
        return operator.update_processing()

    def multi_delete_process(self, params):
        operator = self.get_table_operator(params)
        operator.delete_processing(self._processing_id, params["with_data"])
        # TableOperator.delete_processing(self._processing_id, params['with_data'])

    @classmethod
    def parse_ddl_sql(cls, sql):
        sql_parse_args = {"sql": sql, "properties": {"mlsql.only_parse_table": False}}
        mlsql_parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
        if mlsql_parse_result["result"]:
            return {"content": mlsql_parse_result["content"]}
        else:
            raise SingleSqlParseError(message_kv={"content": mlsql_parse_result["content"]})
