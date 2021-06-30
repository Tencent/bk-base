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

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.basic_model.basic_model_serializer import ModelSerializer
from dataflow.modeling.exceptions.comp_exceptions import DropModelError, DropModelNotExistsError
from dataflow.modeling.models import MLSqlModelInfo, MLSqlStorageModel
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper


class MLSqlModelInfoHandler(object):
    @classmethod
    def filter(cls, **kwargs):
        return MLSqlModelInfo.objects.filter(**kwargs)

    @classmethod
    def create_model_storage(cls, params):
        return MLSqlStorageModel.objects.create(
            expires=params["expires"],
            active=params["active"],
            priority=params["priority"],
            generate_type=params["generate_type"],
            physical_table_name=params["physical_table_name"],
            storage_config=json.dumps(params["storage_config"]),
            storage_cluster_config_id=params["storage_cluster_config_id"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
            description=params["description"],
            data_type=params["data_type"],
        )

    @classmethod
    def create_mlsql_model_info(cls, params):
        MLSqlModelInfo.objects.create(
            model_name=params["model_name"],
            model_alias=params["model_alias"],
            model_framework=params["model_framework"],
            model_type=params["model_type"],
            project_id=params["project_id"],
            algorithm_name=params["algorithm_name"],
            train_mode=params["train_mode"],
            sensitivity=params["sensitivity"],
            active=params["active"],
            status=params["status"],
            created_by=params["created_by"],
            modified_by=params["modified_by"],
            model_storage_id=params["model_storage_id"],
            description=params["description"],
            input_standard_config=params["input_standard_config"],
            output_standard_config=params["output_standard_config"],
        )

    @classmethod
    def fetch_model_by_name(cls, model_name):
        model = MLSqlModelInfo.objects.filter(model_name=model_name)
        if not model.exists():
            return {}
        else:
            serializer = ModelSerializer(model, many=True)
            model_info = serializer.data[0]
            return model_info

    @classmethod
    def delete_model_by_name(cls, model_name):
        model = MLSqlModelInfo.objects.filter(model_name=model_name)
        model.delete()

    @classmethod
    def update_model(cls, model_name, status="developing", active=1):
        MLSqlModelInfo.objects.filter(model_name=model_name).update(status=status, active=active)

    @classmethod
    def fetch_all_models(cls):
        return MLSqlModelInfo.objects.all().order_by("-model_name")

    @classmethod
    def delete_model_by_permission(cls, model_name, created_by):
        model_info = MLSqlModelInfoHandler.fetch_model_by_name(model_name)
        if not model_info:
            # 模型不存在
            return_message = "模型不存在"
            raise DropModelError(message_kv={"content": return_message})
        else:
            logger.info(created_by)
            logger.info(model_info["created_by"])
            if created_by != model_info["created_by"]:
                return_message = "权限不足"
                raise DropModelError(message_kv={"content": return_message})
            else:
                MLSqlModelInfoHandler.delete_model_by_name(model_name)
                if DataProcessingHelper.get_data_processing_via_erp(model_name):
                    # DP存在则删除
                    DataProcessingHelper.delete_data_processing_with_user(model_name, bk_username=created_by)
                if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(model_name):
                    # Processing存在则删除
                    ProcessingBatchInfoHandler.delete_proc_batch_info(model_name)
                return_message = "模型已删除"
        return return_message

    @classmethod
    def delete_model_by_project(cls, model_name, project_id, notebook_id, cell_id, if_exists=False):
        model_info = MLSqlModelInfoHandler.fetch_model_by_name(model_name)
        if not model_info:
            # 模型不存在
            return_message = "模型不存在:" + model_name
            if not if_exists:
                # 如果直接drop,模型不存在的话是需要报错的，如果有if exists则不用
                raise DropModelNotExistsError(message_kv={"content": model_name}, errors={"name": model_name})
            else:
                return return_message
        else:
            if int(project_id) != int(model_info["project_id"]):
                return_message = "权限不足:" + model_name
                # 权限不足的情况下，不管是不是有if exists都需要报错,否则会影响后续执行
                raise DropModelError(message_kv={"content": return_message})
            else:
                # 先删除附属的系统表
                ModelingHelper.delete_parent_system_table(model_name, notebook_id, cell_id)
                MLSqlModelInfoHandler.delete_model_by_name(model_name)
                DataProcessingHelper.delete_data_processing(model_name)
                ProcessingBatchInfoHandler.delete_proc_batch_info(model_name)
                return_message = "模型已删除"
        return return_message

    @classmethod
    def filter_storage_model(cls, **kwargs):
        return MLSqlStorageModel.objects.filter(**kwargs)

    @classmethod
    def get_storage_info(cls, **kwargs):
        return MLSqlStorageModel.objects.get(id=MLSqlModelInfo.objects.get(**kwargs).model_storage_id)

    @classmethod
    def get(cls, **kwargs):
        return MLSqlModelInfo.objects.get(**kwargs)
