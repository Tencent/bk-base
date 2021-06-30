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

from dataflow.shared.api.modules.flow import FlowAPI
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class FlowHelper(object):
    @staticmethod
    def get_flow_job_submit_log_file_size(exec_id, job_id=None, cluster_id=None, geog_area_code=None):
        params = {
            "job_id": job_id,
            "execute_id": exec_id,
            "cluster_id": cluster_id,
            "geog_area_code": geog_area_code,
        }
        res = FlowAPI.log.get_flow_job_submit_log_file_size(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_flow_job_submit_log(
        exec_id,
        begin,
        end,
        search_words=None,
        job_id=None,
        cluster_id=None,
        geog_area_code=None,
        enable_errors=False,
    ):
        params = {
            "job_id": job_id,
            "execute_id": exec_id,
            "begin": begin,
            "end": end,
            "search_words": search_words,
            "cluster_id": cluster_id,
            "geog_area_code": geog_area_code,
        }
        res = FlowAPI.log.get_flow_job_submit_log(params)
        res_util.check_response(res, enable_errors=enable_errors)
        return res.data

    @staticmethod
    def partial_update_flow_node(flow_id, node_id):
        params = {"flow_id": flow_id, "node_id": node_id}
        res = FlowAPI.node.partial_update(params)
        res_util.check_response(res)
        return res

    @staticmethod
    def op_flow_task(flow_id, action):
        params = {"flow_id": flow_id, "action": action}
        res = FlowAPI.flow.op_flow_task(params)
        res_util.check_response(res)
        return res
