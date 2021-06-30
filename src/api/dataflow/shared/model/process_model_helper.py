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

from dataflow.shared.api.modules.process_model import ProcessModelApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class ProcessModelHelper(object):
    @staticmethod
    def get_processing(processing_id):
        res = ProcessModelApi.servings.config({"processing_id": processing_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_processing(
        bk_biz_id,
        project_id,
        input_config,
        output_config,
        schedule_config,
        upgrade_config,
        serving_mode,
        model_release_id,
        sample_feedback_config,
        model_extra_config,
        node_name,
    ):
        """
        创建 processing
        @param bk_biz_id:
        @param project_id:
        @param input_config:
        @param output_config:
        @param schedule_config:
        @param upgrade_config:
        @param serving_mode:
        @param model_release_id:
        @param sample_feedback_config:
        @param model_extra_config:
        @param node_name:
        @return:
        """
        request_params = {
            "bk_biz_id": bk_biz_id,
            "project_id": project_id,
            "input_config": input_config,
            "output_config": output_config,
            "schedule_config": schedule_config,
            "upgrade_config": upgrade_config,
            "serving_mode": serving_mode,
            "model_release_id": model_release_id,
            "sample_feedback_config": sample_feedback_config,
            "model_extra_config": model_extra_config,
            "node_name": node_name,
        }
        res = ProcessModelApi.servings.create(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_processing(
        processing_id,
        bk_biz_id,
        project_id,
        input_config,
        output_config,
        schedule_config,
        upgrade_config,
        serving_mode,
        model_release_id,
        sample_feedback_config,
        model_extra_config,
        node_name,
    ):
        """
        @param processing_id:
        @param bk_biz_id:
        @param project_id:
        @param input_config:
        @param output_config:
        @param schedule_config:
        @param upgrade_config:
        @param serving_mode:
        @param model_release_id:
        @param sample_feedback_config:
        @param model_extra_config:
        @param node_name:
        @return:
        """
        request_params = {
            "processing_id": processing_id,
            "bk_biz_id": bk_biz_id,
            "project_id": project_id,
            "input_config": input_config,
            "output_config": output_config,
            "schedule_config": schedule_config,
            "upgrade_config": upgrade_config,
            "serving_mode": serving_mode,
            "model_release_id": model_release_id,
            "sample_feedback_config": sample_feedback_config,
            "model_extra_config": model_extra_config,
            "node_name": node_name,
        }
        res = ProcessModelApi.servings.update(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_processing(processing_id, with_data=False):
        res = ProcessModelApi.servings.delete({"processing_id": processing_id, "with_data": with_data})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def start_job(kwargs):
        res = ProcessModelApi.servings.start(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def stop_job(processing_id):
        request_params = {"processing_id": processing_id}
        res = ProcessModelApi.servings.stop(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def sync_status(processing_id, **kwargs):
        kwargs.update({"processing_id": processing_id})
        res = ProcessModelApi.servings.sync_status(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def force_kill(processing_id, timeout):
        kwargs = {"processing_id": processing_id, "timeout": timeout}
        res = ProcessModelApi.servings.force_kill(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data
