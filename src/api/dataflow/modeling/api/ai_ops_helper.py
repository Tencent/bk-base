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

from dataflow.shared.api.modules.ai_ops import AIOpsAPI
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class AIOpsHelper(object):
    @staticmethod
    def create_sample_set(params):
        res = AIOpsAPI.sample_set.create(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_sample_features(params):
        res = AIOpsAPI.sample_features.config(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_model_info(params):
        res = AIOpsAPI.model_info.create(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_model_project_storage_cluster(project_id, bk_username, is_processing=True):
        params = {"project_id": project_id, "bk_username": bk_username}
        if is_processing:
            # 获取处理集群信息
            res = AIOpsAPI.model_info.processing_clusters(params)
        else:
            # 获取存储集群信息
            res = AIOpsAPI.model_info.storage_clusters(params)
        res_util.check_response(res, self_message=False)
        cluster_data = res.data
        if len(cluster_data) != 0:
            for cluster_info in cluster_data:
                clusters = cluster_info["clusters"]
                for cluster_item in clusters:
                    return cluster_item
        return {}

    @staticmethod
    def is_model_experiment_exists(model_id, bk_username):
        params = {"bk_username": bk_username, "model_id": model_id}
        res = AIOpsAPI.model_experiment.list(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_model_experiment(params):
        res = AIOpsAPI.model_experiment.create(params)
        res_util.check_response(res, self_message=False)
        return res.data
