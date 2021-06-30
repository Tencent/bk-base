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

from common.local import get_request_username
from common.transaction import auto_meta_sync
from django.db.models import Q

from dataflow.modeling.models import ModelInfo, StorageModel


class ModelInfoHandler(object):
    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def create_model_info(cls, params):
        ModelInfo.objects.create(
            model_id=params["model_id"],
            model_name=params["model_name"],
            model_alias=params["model_alias"],
            description=params["description"],
            project_id=params["project_id"],
            model_type=params["model_type"],
            sensitivity=params["sensitivity"],
            active=params["active"],
            properties=params["properties"],
            scene_name=params["scene_name"],
            status=params["status"],
            train_mode=params["train_mode"],
            sample_set_id=0,
            input_standard_config=params["input_standard_config"],
            output_standard_config=params["output_standard_config"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
            protocol_version=params["protocol_version"],
        )

    @classmethod
    def get_model_info_by_name(cls, model_name, active=None, status=None):
        request_body = {"model_name": model_name}
        if active is not None:
            request_body["active"] = active
        if status is not None:
            request_body["status"] = status
        response = ModelInfo.objects.filter(**request_body)
        result = []
        for item in response:
            item_map = {
                "model_id": item.model_id,
                "model_name": item.model_name,
                "model_type": item.model_type,
                "train_mode": item.train_mode,
                "sensitivity": item.sensitivity,
            }
            result.append(item_map)
        return result

    @classmethod
    def exists(cls, **kwargs):
        return ModelInfo.objects.filter(**kwargs).exists()

    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def save(cls, **kwargs):
        ModelInfo(**kwargs).save()

    @classmethod
    @auto_meta_sync(using="bkdata_modeling")
    def save_storage_model(self, params):
        StorageModel.objects.create(
            physical_table_name=params["physical_table_name"],
            expires=-1,
            storage_config="{}",
            storage_cluster_config_id=params["storage_cluster_config_id"],
            storage_cluster_name=params["storage_cluster_name"],
            storage_cluster_type=params["storage_cluster_type"],
            priority=0,
            generate_type="system",
            content_type="model",
            model_id=params["model_id"],
            created_by=params["created_by"],
            updated_by=params["updated_by"],
        )

    @classmethod
    def get(cls, **kwargs):
        return ModelInfo.objects.get(**kwargs)

    @classmethod
    def get_model_by_project(cls, project_id):
        # 返回可以应用的模型:所有的公共模型以及当前项目下当前用户创建的私有模型
        return ModelInfo.objects.filter(
            Q(sensitivity="public")
            | (Q(project_id=project_id) & Q(created_by=get_request_username()) & Q(sensitivity="private"))
        ).order_by("-created_at")

    @classmethod
    def get_update_model_by_project(cls, project_id):
        # 返回当前用户可以更新的模型:当前项目下所有公共模型以及用户创建的私有模型
        return ModelInfo.objects.filter(
            Q(project_id=project_id)
            & (Q(sensitivity="public") | (Q(created_by=get_request_username()) & Q(sensitivity="private")))
        ).order_by("-created_at")
