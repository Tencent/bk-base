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

from dataflow.shared.api.modules.model import ModelApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class ModelHelper(object):
    @staticmethod
    def get_model_version(model_id, model_version_id):
        request_params = {"model_id": model_id, "model_version_id": model_version_id}
        res = ModelApi.get_model_version(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_processing(processing_id):
        res = ModelApi.model_instances.retrieve({"processing_id": processing_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_processing(
        submitted_by,
        bk_biz_id,
        project_id,
        model_version_id,
        model_id,
        model_params,
        auto_upgrade,
        training_when_serving,
        training_from_instance,
        training_scheduler_params,
        serving_scheduler_params,
        serving_mode,
    ):
        """
        创建模型实例，从 dataflow 的角度是创建 processing
        @param submitted_by:
        @param bk_biz_id:
        @param project_id:
        @param model_version_id:
        @param model_id:
        @param model_params:
        @param auto_upgrade:
        @param training_when_serving:
        @param training_from_instance:
        @param training_scheduler_params:
        @param serving_scheduler_params:
        @param serving_mode:
        @return:
        """
        request_params = {
            "submitted_by": submitted_by,
            "biz_id": bk_biz_id,
            "project_id": project_id,
            "model_version_id": model_version_id,
            "model_id": model_id,
            "model_params": model_params,
            "auto_upgrade": auto_upgrade,
            "training_when_serving": training_when_serving,
            "training_from_instance": training_from_instance,
            "training_scheduler_params": training_scheduler_params,
            "serving_scheduler_params": serving_scheduler_params,
            "serving_mode": serving_mode,
        }
        res = ModelApi.model_instances.create(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_processing(
        processing_id,
        submitted_by,
        bk_biz_id,
        project_id,
        model_version_id,
        model_id,
        model_params,
        auto_upgrade,
        training_when_serving,
        training_from_instance,
        training_scheduler_params,
        serving_scheduler_params,
        serving_mode,
    ):
        """
        更新模型实例，从 dataflow 的角度是创建 processing
        @param processing_id:
        @param submitted_by:
        @param bk_biz_id:
        @param project_id:
        @param model_version_id:
        @param model_id:
        @param model_params:
        @param auto_upgrade:
        @param training_when_serving:
        @param training_from_instance:
        @param training_scheduler_params:
        @param serving_scheduler_params:
        @param serving_mode:
        @return:
        """
        request_params = {
            "processing_id": processing_id,
            "submitted_by": submitted_by,
            "biz_id": bk_biz_id,
            "project_id": project_id,
            "model_version_id": model_version_id,
            "model_id": model_id,
            "model_params": model_params,
            "auto_upgrade": auto_upgrade,
            "training_when_serving": training_when_serving,
            "training_from_instance": training_from_instance,
            "training_scheduler_params": training_scheduler_params,
            "serving_scheduler_params": serving_scheduler_params,
            "serving_mode": serving_mode,
        }
        res = ModelApi.model_instances.update(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_processing(processing_id, with_data=False):
        res = ModelApi.model_instances.delete(
            {
                "submitted_by": get_request_username(),
                "processing_id": processing_id,
                "with_data": with_data,
            }
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_models(
        request_fields,
        project_ids,
        is_foggy,
        is_public,
        is_last_available,
        model_status,
    ):
        """
        获取模型列表
        @param request_fields:
        @param project_ids:
        @param is_foggy:
        @param is_public:
        @param is_last_available: 输出最新的
        @param model_status:
        @return:
        """
        request_params = {
            "submitted_by": get_request_username(),
            "request_field": json.dumps(request_fields),
            "project_id_list": json.dumps(project_ids),
            "is_foggy": is_foggy,
            "is_public": is_public,
            "is_last_avaliable": is_last_available,
            "model_status": model_status,
        }
        res = ModelApi.models.list(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def start_job(submitted_by, processing_id):
        request_params = {"submitted_by": submitted_by, "processing_id": processing_id}
        res = ModelApi.model_instances.start(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def stop_job(submitted_by, processing_id):
        request_params = {"submitted_by": submitted_by, "processing_id": processing_id}
        res = ModelApi.model_instances.stop(request_params)
        res_util.check_response(res)
        return res.data
