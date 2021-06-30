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

from dataflow.uc.adapter.operation_client import get_job_operation_adapter
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException
from dataflow.uc.handlers import unified_computing_graph_info


class GraphJobController(object):
    def __init__(self, graph_id, job_id):
        self.graph_id = graph_id
        self.job_id = job_id
        self.job_operation = self.get_job_operation()

    def get_job_operation(self):
        graph_info = unified_computing_graph_info.get(self.graph_id)
        job_type = None
        for job_info in json.loads(graph_info.job_info_list):
            if job_info["job_id"] == self.job_id:
                job_type = job_info["job_type"]
                break
        if job_type is None:
            raise IllegalArgumentException("Not found the job id {} in the graph {}".format(self.job_id, self.graph_id))
        # 根据 job type 获取 operation adapter
        return get_job_operation_adapter(job_type)

    def start(self):
        return self.job_operation.start_job(self.job_id)

    def stop(self):
        return self.job_operation.stop_job(self.job_id)

    def get_operate_result(self, operate_info):
        return self.job_operation.get_operate_result(self.job_id, operate_info)
