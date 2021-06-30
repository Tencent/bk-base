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

from django.utils.translation import ugettext_lazy as _

from dataflow.batch.exceptions.comp_execptions import BatchIllegalStatusError
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class AbstractProcessingsBackend(object):
    def __init__(self, periodic_processing_param_obj):
        """
        :param periodic_processing_param_obj:
        :type periodic_processing_param_obj:
        dataflow.batch.periodic.param_info.periodic_batch_info_params.PeriodicBatchInfoParams
        """
        self.periodic_processing_param_obj = periodic_processing_param_obj

    def create_data_processing(self, result_tables):
        processing_input = []
        for input_result_table in self.periodic_processing_param_obj.input_result_tables:
            parent_name = input_result_table.result_table_id
            if not input_result_table.is_static:
                parent_storage_cluster_config_id = ResultTableHelper.get_result_table_storage(parent_name, "hdfs")[
                    "hdfs"
                ]["storage_cluster"]["storage_cluster_config_id"]
                processing_input.append(
                    {
                        "data_set_type": "result_table",
                        "data_set_id": parent_name,
                        "storage_cluster_config_id": parent_storage_cluster_config_id,
                        "storage_type": "storage",
                        "tags": [],
                    }
                )
            else:
                static_data_cluster_config_id = ResultTableHelper.get_result_table_storage(parent_name, "ignite")[
                    "ignite"
                ]["storage_cluster"]["storage_cluster_config_id"]
                processing_input.append(
                    {
                        "data_set_type": "result_table",
                        "data_set_id": parent_name,
                        "storage_cluster_config_id": static_data_cluster_config_id,
                        "storage_type": "storage",
                        "tags": ["static_join"],
                    }
                )

        processing_output = []
        for output_result_table in self.periodic_processing_param_obj.output_result_tables:
            tmp_output = {
                "data_set_type": "result_table",
                "data_set_id": output_result_table.result_table_id,
            }
            processing_output.append(tmp_output)

        processings_args = {
            "project_id": self.periodic_processing_param_obj.project_id,
            "processing_id": self.periodic_processing_param_obj.processing_id,
            "processing_alias": self.periodic_processing_param_obj.processing_id,
            "processing_type": "batch",
            "result_tables": result_tables,
            "created_by": self.periodic_processing_param_obj.bk_username,
            "description": self.periodic_processing_param_obj.description,
            "inputs": processing_input,
            "outputs": processing_output,
            "tags": self.periodic_processing_param_obj.tags,
        }
        return processings_args

    def create_meta_result_table(self, fields):
        result_tables = []
        for output_result_table in self.periodic_processing_param_obj.output_result_tables:
            result_table = {
                "bk_biz_id": output_result_table.bk_biz_id,
                "project_id": self.periodic_processing_param_obj.project_id,
                "result_table_id": output_result_table.result_table_id,
                "result_table_name": output_result_table.table_name,
                "result_table_name_alias": self.periodic_processing_param_obj.description,
                "processing_type": "batch",
                "count_freq": self.periodic_processing_param_obj.count_freq,
                "count_freq_unit": self.periodic_processing_param_obj.schedule_period,
                "description": output_result_table.table_name,
                "fields": fields,
                "tags": self.periodic_processing_param_obj.tags,
            }
            result_tables.append(result_table)
        return result_tables

    def delete_processings(self, with_data):
        processing_id = self.periodic_processing_param_obj.processing_id
        if ProcessingBatchJobHandler.is_proc_batch_job_active(processing_id):
            raise BatchIllegalStatusError(message=_("离线(%s)作业正在运行") % processing_id)
        DataProcessingHelper.delete_data_processing(processing_id, with_data=with_data)
        ProcessingBatchInfoHandler.delete_proc_batch_info(processing_id)
