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

from dataflow.shared.api.modules.stream import StreamApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.stream.handlers import processing_job_info, processing_stream_info


class StreamHelper(object):
    @staticmethod
    def create_processing(**request_params):
        res = StreamApi.processings.create(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_processing(**request_params):
        res = StreamApi.processings.update(request_params, timeout=300)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_processing(processing_id, with_data=False, delete_result_tables=[]):
        res = StreamApi.processings.delete(
            {
                "processing_id": processing_id,
                "with_data": with_data,
                "delete_result_tables": delete_result_tables,
            }
        )
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_stream_param(params):
        res = StreamApi.processings.check_stream_param(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_job(
        project_id, processings, code_version, component_type, cluster_group, deploy_config, job_config, **kwargs
    ):
        # example = {
        #     'project_id': '',
        #     "code_version": '',
        #     "component_type": "flink",
        #     "cluster_group": cluster_set,
        #     "job_config": {
        #         "heads": ','.join(head_rts),
        #         "tails": ','.join(tail_rts),
        #         "concurrency": concurrency,
        #         "offset": 0
        #     },
        #     "deploy_config": None,
        #     "processings": []
        # }
        kwargs.update(
            {
                "project_id": project_id,
                "code_version": code_version,
                "component_type": component_type,
                "cluster_group": cluster_group,
                "job_config": job_config,
                "deploy_config": deploy_config,
                "processings": processings,
            }
        )
        res = StreamApi.jobs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_job(project_id, processings, code_version, cluster_group, deploy_config, job_config, **kwargs):
        kwargs.update(
            {
                "project_id": project_id,
                "code_version": code_version,
                "cluster_group": cluster_group,
                "job_config": job_config,
                "deploy_config": deploy_config,
                "processings": processings,
            }
        )
        res = StreamApi.jobs.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_debug(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = StreamApi.debugs.stop(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def register_job(job_id, jar_name, geog_area_code, **kwargs):
        kwargs.update({"job_id": job_id, "jar_name": jar_name, "geog_area_code": geog_area_code})
        res = StreamApi.jobs.register(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_code_version(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = StreamApi.jobs.code_version(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def submit_job(conf, job_id, **kwargs):
        kwargs.update({"conf": conf, "job_id": job_id})
        res = StreamApi.jobs.submit(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def sync_status(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = StreamApi.jobs.sync_status(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def force_kill(job_id, timeout):
        kwargs = {"job_id": job_id, "timeout": timeout}
        res = StreamApi.jobs.force_kill(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def cancel_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = StreamApi.jobs.cancel(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def lock_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = StreamApi.jobs.lock(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def unlock_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = StreamApi.jobs.unlock(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_debug(heads, tails, job_id, **kwargs):
        kwargs.update({"heads": heads, "tails": tails, "job_id": job_id})
        res = StreamApi.debugs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_node_info(debug_id, result_table_id, **kwargs):
        kwargs.update({"debug_id": debug_id, "result_table_id": result_table_id})
        res = StreamApi.debugs.node_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_basic_info(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = StreamApi.debugs.basic_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_job_component_type(job_id):
        job_info = processing_job_info.get(job_id)
        return job_info.component_type

    @staticmethod
    def get_processing_component_type(processing_id):
        stream_info = processing_stream_info.get(processing_id)
        return stream_info.component_type

    @staticmethod
    def change_component_type(processings, component_type):
        """
        @param processings:
        @param component_type:
        @return:
        """
        request_params = {"processings": processings, "component_type": component_type}
        res = StreamApi.processings.change_component_type(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_job(job_id):
        request_params = {"job_id": job_id}
        res = StreamApi.jobs.delete(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_single_processing_job(**request_params):
        res = StreamApi.jobs.create_single_processing(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_single_processing(**request_params):
        res = StreamApi.jobs.update_single_processing(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def start(job_id):
        request_params = {"job_id": job_id}
        res = StreamApi.jobs.start(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
