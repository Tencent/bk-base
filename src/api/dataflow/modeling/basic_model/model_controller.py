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

from common.exceptions import ApiRequestError, ValidationError

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.basic_model.basic_model_serializer import ModelSerializer
from dataflow.modeling.exceptions.comp_exceptions import TableNotExistsError
from dataflow.modeling.handler.algorithm import AlgorithmHandler
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.models import MLSqlModelInfo
from dataflow.modeling.utils.modeling_utils import ModelingUtils


class ModelingModelStorageController(object):
    def __init__(
        self,
        expires,
        active,
        priority,
        generate_type,
        physical_table_name,
        storage_config,
        storage_config_id,
        created_by,
        updated_by,
    ):
        self.expires = expires
        self.active = active
        self.priority = priority
        self.generate_type = generate_type
        self.physical_table_name = physical_table_name
        self.storage_config = storage_config
        self.storage_config_id = storage_config_id
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = self.physical_table_name
        self.id = 0

    def create(self):
        params = {
            "expires": self.expires,
            "active": self.active,
            "priority": self.priority,
            "generate_type": self.generate_type,
            "physical_table_name": self.physical_table_name,
            "storage_config": json.dumps(self.storage_config),
            "storage_cluster_config_id": self.storage_config_id,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "description": self.description,
            "data_type": "parquet",
        }
        model_storage = MLSqlModelInfoHandler.create_model_storage(params)
        self.id = model_storage.id


class ModelingModelController(object):
    queryset = MLSqlModelInfoHandler.fetch_all_models()
    serializer_class = ModelSerializer

    model_name = None

    def __init__(self, model_name, created_by=None):
        self.model_name = model_name
        self.created_by = created_by

    def create(self, args):
        MLSqlModelInfoHandler.create_mlsql_model_info(**args)

    def update(self, status=None, active=None):
        update_args = {"status": status, "active": active}
        model = MLSqlModelInfo.objects.filter(model_name=self.model_name)
        model.update(**update_args)

    def retrieve(self, active=None, status=None):
        request_body = {
            "model_name": self.model_name,
        }
        if active is not None:
            request_body["active"] = active
        if status is not None:
            request_body["status"] = status
        response = MLSqlModelInfoHandler.filter(**request_body)
        serializer = ModelSerializer(response, many=True)
        model_item_list = []
        for model_item in serializer.data:
            model_storage_id = model_item["model_storage_id"]
            storage_result = MLSqlModelInfoHandler.filter_storage_model(id=model_storage_id)
            for storage in storage_result:
                model_item["storage"] = {"path": json.loads(storage.storage_config)["path"]}
            model_item_list.append(model_item)
        if model_item_list:
            return model_item_list[0]
        else:
            return {}

    def destroy(self, active=None):
        return MLSqlModelInfoHandler.delete_model_by_permission(self.model_name, self.created_by)

    @classmethod
    def fetch_list(cls, bk_username, model_name):
        if bk_username:
            args = {"active": 1, "created_by": bk_username}

            result_models = MLSqlModelInfoHandler.filter(**args)
            serializer = ModelSerializer(result_models, many=True)
            return serializer.data
        elif model_name:
            basic_models = MLSqlModelInfoHandler.filter(model_name__in=model_name, active=1)
            result_models = []
            for model in basic_models:
                result_models.append(
                    {
                        "model_name": model.model_name,
                        "algorithm_name": model.algorithm_name,
                        "status": model.status,
                        "created_by": model.created_by,
                        "created_at": model.create_at,
                    }
                )
            return result_models
        else:
            raise ValidationError("The parameter must contain bk_username or model_name")


class ModelingDDLOperator(object):
    def __init__(self, table_id, notebook_id, cell_id, bk_username):
        self.table_id = table_id
        self.notebook_id = notebook_id
        self.cell_id = cell_id
        self.bk_username = bk_username
        self.notebook_info = {
            "notebook_id": self.notebook_id,
            "cell_id": self.cell_id,
            "bk_username": self.bk_username,
        }

    def truncate(self):
        try:
            ModelingUtils.truncate_result_tables(self.table_id, **self.notebook_info)
        except ApiRequestError:
            raise TableNotExistsError(message_kv={"name": self.table_id})
        except Exception as e:
            raise e

    def drop(self):
        try:
            ModelingUtils.delete_result_tables(self.table_id, **self.notebook_info)
        except ApiRequestError:
            raise TableNotExistsError(message_kv={"name": self.table_id})
        except Exception as e:
            raise e
        try:
            ProcessingBatchInfoHandler.delete_proc_batch_info(self.table_id)
        except Exception as e:
            raise e


"""
algorithm_name = models.CharField(primary_key=True, max_length=64)
    algorithm_alias = models.CharField(max_length=64)
    description = models.TextField(blank=True, null=True)
    algorithm_type = models.CharField(max_length=32, blank=True, null=True)
    generate_type = models.CharField(max_length=32)
    sensitivity = models.CharField(max_length=32)
    project_id = models.IntegerField(blank=True, null=True)
    run_env = models.CharField(max_length=64, blank=True, null=True)
    framework = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)


    algorithm_name = models.CharField(max_length=64)
    version = models.IntegerField()
    logic = models.TextField(blank=True, null=True)
    config = models.TextField()
    execute_config = models.TextField()
    properties = models.TextField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)
"""


class ModelingAlgorithmController(object):
    @classmethod
    def create(cls):
        algorithm_list = AlgorithmHandler.get_spark_algorithm()
        for algorithm in algorithm_list:
            params = {
                "algorithm_name": algorithm.algorithm_name,
                "algorithm_alias": algorithm.algorithm_alias,
                "description": algorithm.description,
                "algorithm_type": algorithm.algorithm_type,
                "generate_type": algorithm.generate_type,
                "sensitivity": algorithm.sensitivity,
                "project_id": algorithm.project_id,
                "run_env": algorithm.run_env,
                "framework": algorithm.framework,
                "created_by": algorithm.created_by,
                "created_at": algorithm.created_at,
                "updated_by": algorithm.updated_by,
            }
            AlgorithmHandler.save(**params)
            algorithm_version = AlgorithmVersionHandler.get_alorithm_by_name(algorithm.algorithm_name)
            version_params = {
                "id": algorithm_version.id,
                "algorithm_name": algorithm_version.algorithm_name,
                "version": algorithm_version.version,
                "logic": algorithm_version.logic,
                "config": algorithm_version.config,
                "execute_config": algorithm_version.execute_config,
                "properties": algorithm_version.properties,
                "created_by": algorithm_version.created_by,
                "created_at": algorithm_version.created_at,
                "updated_by": algorithm_version.updated_by,
                "description": algorithm_version.description,
            }
            AlgorithmVersionHandler.save(**version_params)
