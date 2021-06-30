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

from dataflow.shared.api.modules.model_app import ModelAppApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class ModelAppHelper(object):
    @staticmethod
    def create_processing(project_id, processing_id, result_tables, model_config, submit_args, **kwargs):
        kwargs.update(
            {
                "project_id": project_id,
                "processing_id": processing_id,
                "result_tables": result_tables,
                "model_config": model_config,
                "submit_args": submit_args,
            }
        )
        res = ModelAppApi.processings.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_processing(project_id, processing_id, result_tables, sql, dict, **kwargs):
        kwargs.update(
            {
                "project_id": project_id,
                "processing_id": processing_id,
                "result_tables": result_tables,
                "sql": sql,
                "dict": dict,
            }
        )
        res = ModelAppApi.processings.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_processing(processing_id, with_data=False, delete_result_tables=[]):
        """
        删除 DP
        @param processing_id:
        @param with_data: 是否删除关联所有 RT
        @param delete_result_tables: 指定删除 DP 时应该删除的结果表列表
        @return:
        """
        res = ModelAppApi.processings.delete(
            {
                "processing_id": processing_id,
                "with_data": with_data,
                "delete_result_tables": delete_result_tables,
            }
        )
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_job(
        project_id,
        processing_id,
        code_version,
        cluster_group,
        cluster_name,
        deploy_mode,
        deploy_config,
        job_config,
        **kwargs
    ):
        # example = {
        #     "processing_id": "591_xxx_58",
        #     "code_version": "0.1.0",
        #     "cluster_group": "gem",         # 集群类型
        #     "cluster_name": "root.default", # 队列
        #     "deploy_mode": "yarn",
        #     "deploy_config": "{executor_memory:1024m}",
        #     "job_config": {},
        #     "project_id": 24
        # }
        kwargs.update(
            {
                "project_id": project_id,
                "code_version": code_version,
                "cluster_group": cluster_group,
                "cluster_name": cluster_name,
                "job_config": job_config,
                "deploy_mode": deploy_mode,
                "deploy_config": deploy_config,
                "processing_id": processing_id,
            }
        )
        res = ModelAppApi.jobs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def start_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = ModelAppApi.jobs.start(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = ModelAppApi.jobs.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = ModelAppApi.jobs.stop(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_job(job_id):
        request_params = {"job_id": job_id}
        res = ModelAppApi.jobs.delete(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
