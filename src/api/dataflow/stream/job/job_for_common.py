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

from dataflow.shared.log import stream_logger as logger
from dataflow.stream.exceptions.comp_execptions import CancelFlinkJobError, SubmitFlinkJobError


def get_execute_id_by_schedule_id(jobnavi_stream_helper, schedule_id, operator):
    # 获取cluster_name对应的execute id(对应yarn session)；cluster_name对应jobnavi中的schedule id
    cluster_is_exist = jobnavi_stream_helper.get_schedule(schedule_id)
    # 当schedule id不存在时 则任务不存在
    if not cluster_is_exist and operator == "start":
        raise SubmitFlinkJobError()
    if not cluster_is_exist and operator == "stop":
        raise CancelFlinkJobError()
    # 获取execute id，如果获取不到，则视为异常情况
    execute_id = jobnavi_stream_helper.get_execute_id(schedule_id)
    if not execute_id and operator == "start":
        raise SubmitFlinkJobError()
    if not execute_id and operator == "stop":
        raise CancelFlinkJobError()
    return execute_id


def get_job_status(job_id, jobnavi_stream_helper, execute_id):
    # 调用获取job状态的接口
    event_id = jobnavi_stream_helper.request_jobs_status(execute_id)
    # 根据event id获取任务状态
    jobs_status_list = jobnavi_stream_helper.list_jobs_status(event_id)
    if job_id in jobs_status_list:  # todo [new]  jobs_status_list job status contain running  restarting...
        job_status = {job_id: "ACTIVE"}
    else:
        job_status = {job_id: None}
    logger.info("[sync job status] the job status is %s" % str(job_status))
    return job_status
