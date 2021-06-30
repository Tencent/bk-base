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

from common.local import get_request_username
from django.utils.translation import ugettext_lazy as _

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.periodic.backend.processings_sql_backend import ProcessingsSqlBackend
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.processing.modeling_processing import DatalabTableOperator
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class TensorFlowCodeProcessingsBackend(ProcessingsSqlBackend):
    def __init__(self, periodic_tf_batch_info_param_obj):
        super(TensorFlowCodeProcessingsBackend, self).__init__(periodic_tf_batch_info_param_obj)

    def create_processings(self):
        processing_id = self.periodic_processing_param_obj.processing_id
        # 检查重名
        if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            raise Exception(_(u"Processing已经存在:{}".format(processing_id)))
        # 完成表与DP的创建
        result_tables = self.create_processings_rt_params(None)
        result_models = self.create_processing_model_params()
        processings_args = self.create_data_processing(result_tables)

        dp_created = False
        storage_created = False
        model_created = False
        batch_created = False
        try:
            # 表及DP的创建
            DataProcessingHelper.set_data_processing(processings_args)
            dp_created = True
            # 关联存储
            self.create_result_table_storages()
            storage_created = True
            # 完成模型信息的创建
            self.save_processing_model(result_models)
            model_created = True
            self.save_to_processings_batch_info()
            batch_created = True
        except Exception as e:
            logger.exception(e)
            # 删除dp
            TensorFlowCodeProcessingsBackend.delete_procesing_info(
                processing_id,
                delete_dp=dp_created,
                delete_storage=storage_created,
                delete_model=model_created,
                delete_batch=batch_created,
                with_data=True,
            )
            raise e
        result_map = {
            "result_table_ids": [table["result_table_id"] for table in result_tables],
            "processing_id": processing_id,
            "result_model_ids": [model["name"] for model in result_models],
        }
        return result_map

    def create_processings_rt_params(self, fields):
        result_tables = []
        for output_result_table in self.periodic_processing_param_obj.output_result_tables:
            new_fields = self.add_extra_field(output_result_table.fields)
            result_table = {
                "bk_biz_id": output_result_table.bk_biz_id,
                "project_id": self.periodic_processing_param_obj.project_id,
                "result_table_id": output_result_table.result_table_id,
                "result_table_name": output_result_table.table_name,
                "result_table_name_alias": output_result_table.description,
                "processing_type": "batch",
                "count_freq": self.periodic_processing_param_obj.count_freq,
                "count_freq_unit": self.periodic_processing_param_obj.schedule_period,
                "description": output_result_table.table_name,
                "fields": new_fields,
                "tags": self.periodic_processing_param_obj.tags,
            }
            result_tables.append(result_table)
        return result_tables

    def create_result_table_storages(self):
        for output_result_table in self.periodic_processing_param_obj.output_result_tables:
            if output_result_table.need_create_storage:
                # 获取存储信息
                cluster_name = NodeUtils.get_storage_cluster_name(self.periodic_processing_param_obj.project_id, "hdfs")
                storage_info = {
                    "result_table_id": output_result_table.result_table_id,
                    "cluster_name": cluster_name,
                    "cluster_type": "hdfs",
                    "expires": -1,
                    "storage_config": "{}",
                    "generate_type": "system",
                    "data_type": "iceberg",
                }
                # 为表创建存储信息
                StorekitHelper.create_physical_table(**storage_info)
                StorekitHelper.prepare(output_result_table.result_table_id, "hdfs")

    def create_processing_model_params(self):
        result_models = []
        for model_info in self.periodic_processing_param_obj.output_models:
            model = DatalabTableOperator.format_model_info(model_info.model_name, model_info.model_alias)
            result_models.append(model)
        return result_models

    def save_processing_model(self, model_params):
        for model in model_params:
            DatalabTableOperator.create_model_info(
                self.periodic_processing_param_obj.project_id, model, "", get_request_username()
            )

    def update_processing_model(self, model_params):
        # 先获取当前processing现有的输出模型,然后删除多余的，增加新增的
        processing_id = self.periodic_processing_param_obj.processing_id
        processing_info = ProcessingBatchInfoHandler.get_batch_info_by_processing_id(processing_id)
        submit_args = json.loads(processing_info.submit_args)
        output_models = submit_args["output_models"]
        # 用户输入的新的模型信息
        new_model_map = {model["name"]: model for model in model_params}
        # 旧有原有的模型信息
        old_model_map = {model["model_name"]: model for model in output_models}
        # 删除旧有的，如无权限，则报错
        for model_name in new_model_map:
            if model_name not in old_model_map:
                # 不存在，需要新建，这里如果已经有同名模型，会报错
                DatalabTableOperator.create_model_info(
                    self.periodic_processing_param_obj.project_id, new_model_map[model_name], "", get_request_username()
                )
        for model_name in old_model_map:
            if model_name not in new_model_map:
                # 原来的没有，删除，这里没有权限的话，会报错
                ModelingUtils.delete_model(model_name, None, {"bk_username": get_request_username()})

    def update_processings(self):
        processing_id = self.periodic_processing_param_obj.processing_id
        # 更新rt与dp
        result_tables = self.create_processings_rt_params(None)
        processings_args = self.create_data_processing(result_tables)
        DataProcessingHelper.update_data_processing(processings_args)
        # 完成模型信息的创建
        result_models = self.create_processing_model_params()
        self.update_processing_model(result_models)
        try:
            self.update_to_processings_batch_info()
        except Exception as e:
            logger.exception(e)
            DataProcessingHelper.delete_data_processing(processing_id, with_data=True)
            raise e
        result_map = {
            "result_table_ids": [table["result_table_id"] for table in result_tables],
            "processing_id": processing_id,
            "result_model_ids": [model["name"] for model in result_models],
        }
        return result_map

    def delete_processings(self, with_data):
        super(TensorFlowCodeProcessingsBackend, self).delete_processings(with_data)
        # 同时要清除模型
        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(
            self.periodic_processing_param_obj.processing_id
        )
        submit_args = json.loads(processing_info.submit_args)
        output_models = submit_args["output_models"]
        for model_name in output_models:
            ModelingUtils.delete_model(model_name, None, {"bk_username": get_request_username()})

    @staticmethod
    def delete_procesing_info(
        processing_id, delete_storage=True, delete_dp=True, delete_model=True, delete_batch=True, with_data=True
    ):
        if DataProcessingHelper.get_dp_via_erp(processing_id):
            # dp存在时进行删除
            if delete_storage:
                # 调用storekit删除表的存储
                TensorFlowCodeProcessingsBackend.delete_rt_storage(processing_id)
            if delete_dp:
                # 删除dp & 删除rt
                if DataProcessingHelper.get_dp_via_erp(processing_id):
                    # DP存在
                    DataProcessingHelper.delete_data_processing(processing_id, with_data)

        if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            # 最后删除processinng
            if delete_model:
                # 删除模型
                TensorFlowCodeProcessingsBackend.delete_models(processing_id)
            if delete_batch:
                ProcessingBatchInfoHandler.delete_proc_batch_info(processing_id)

    @staticmethod
    def delete_rt_storage(processing_id):
        if DataProcessingHelper.get_dp_via_erp(processing_id):
            # dp存在
            data_processing = DataProcessingHelper.get_data_processing(processing_id)
            output_table_list = data_processing["outputs"]
            for output_table in output_table_list:
                table_id = output_table["data_set_id"]
                if DataProcessingHelper.is_result_table_exist(table_id):
                    # 表存在,删除存储
                    DatabusHelper.delete_data_storages(table_id, "hdfs", "batch")

    @staticmethod
    def delete_models(processing_id):
        # 先清理生成的模型
        processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        submit_args = json.loads(processing_batch_info.submit_args)
        output_models = submit_args["output_models"]
        for model in output_models:
            model_info = MLSqlModelInfoHandler.fetch_model_by_name(model["model_name"])
            if model_info:
                ModelingUtils.delete_model(model["model_name"], None, {"bk_username": get_request_username()})
