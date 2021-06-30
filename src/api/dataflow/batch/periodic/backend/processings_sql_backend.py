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

from dataflow.batch.exceptions.comp_execptions import BatchUnsupportedOperationError
from dataflow.batch.periodic.backend.abstract_processings_backend import AbstractProcessingsBackend
from dataflow.batch.utils import bksql_util
from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper


class ProcessingsSqlBackend(AbstractProcessingsBackend):
    def __init__(self, periodic_sql_batch_info_param_obj):
        """
        :param periodic_processing_param_obj:
        :type periodic_processing_param_obj:
        dataflow.batch.periodic.param_info.periodic_batch_info_params.PeriodicSqlBatchInfoParam
        """
        super(ProcessingsSqlBackend, self).__init__(periodic_sql_batch_info_param_obj)

    def create_processings(self):
        processing_id = self.periodic_processing_param_obj.processing_id

        udfs = bksql_util.parse_udf(
            self.periodic_processing_param_obj.processor_logic["sql"],
            self.periodic_processing_param_obj.project_id,
        )
        fields = self.check_sql_and_get_fields(udfs)
        fields = self.add_extra_field(fields)

        result_tables = self.create_meta_result_table(fields)
        processings_args = self.create_data_processing(result_tables)
        DataProcessingHelper.set_data_processing(processings_args)

        try:
            self.save_to_processing_udf_info(udfs)
            self.save_to_processings_batch_info()
        except Exception as e:
            batch_logger.exception(e)
            DataProcessingHelper.delete_data_processing(processing_id, with_data=True)
            raise e
        return {"result_table_ids": [processing_id], "processing_id": processing_id}

    def update_processings(self):
        processing_id = self.periodic_processing_param_obj.processing_id

        udfs = bksql_util.parse_udf(
            self.periodic_processing_param_obj.processor_logic["sql"],
            self.periodic_processing_param_obj.project_id,
        )
        fields = self.check_sql_and_get_fields(udfs)
        fields = self.add_extra_field(fields)

        result_tables = self.create_meta_result_table(fields)
        processings_args = self.create_data_processing(result_tables)
        DataProcessingHelper.update_data_processing(processings_args)

        try:
            self.update_to_processing_udf_info(udfs)
            self.update_to_processings_batch_info()
        except Exception as e:
            batch_logger.exception(e)
            DataProcessingHelper.delete_data_processing(processing_id, with_data=True)
            raise e
        return {"result_table_ids": [processing_id], "processing_id": processing_id}

    def save_to_processing_udf_info(self, udfs):
        if udfs:
            for udf in udfs:
                processing_udf_info.save(
                    processing_id=self.periodic_processing_param_obj.processing_id,
                    processing_type="batch",
                    udf_name=udf["name"],
                    udf_info=json.dumps(udf),
                )

    def save_to_processings_batch_info(self):
        self.periodic_processing_param_obj.save_to_processing_batch_info_db()

    def update_to_processing_udf_info(self, udfs):
        processing_udf_info.delete(processing_id=self.periodic_processing_param_obj.processing_id)
        self.save_to_processing_udf_info(udfs)

    def update_to_processings_batch_info(self):
        self.periodic_processing_param_obj.update_to_processing_batch_info_db()

    def check_sql_and_get_fields(self, udfs):
        input_result_tables = []
        for parent_window in self.periodic_processing_param_obj.input_result_tables:
            input_result_tables.append(parent_window.result_table_id)

        sql = self.periodic_processing_param_obj.processor_logic["sql"]

        spark_sql_response = bksql_util.call_sparksql(
            sql,
            input_result_tables,
            self.periodic_processing_param_obj.output_result_tables[0].result_table_id,
            udfs,
            self.periodic_processing_param_obj.is_self_dependency,
            self.periodic_processing_param_obj.self_dependency_fields,
        )

        self.periodic_processing_param_obj.processor_logic["sql"] = spark_sql_response["sql"]

        result_table_fields = spark_sql_response["fields"]
        dimensions_fields = bksql_util.retrieve_dimension_fields(sql)
        for tmp_field in result_table_fields:
            tmp_field["is_dimension"] = True if tmp_field["field_name"] in dimensions_fields else False
        return result_table_fields

    def add_extra_field(self, result_table_fields):
        current_length = len(result_table_fields)

        for tmp_field in result_table_fields:
            if tmp_field["field_name"].lower() == "_starttime_" or tmp_field["field_name"].lower() == "_endtime_":
                raise BatchUnsupportedOperationError("Found unsupported column name {}".format(tmp_field["field_name"]))

        start_time_field = {
            "field_type": "string",
            "field_alias": "_startTime_",
            "field_name": "_startTime_",
            "is_dimension": False,
            "field_index": current_length,
        }
        result_table_fields.append(start_time_field)
        end_time_field = {
            "field_type": "string",
            "field_alias": "_endTime_",
            "field_name": "_endTime_",
            "is_dimension": False,
            "field_index": current_length + 1,
        }
        result_table_fields.append(end_time_field)
        return result_table_fields
