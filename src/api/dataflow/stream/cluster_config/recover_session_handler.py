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

import time

from common.exceptions import ApiRequestError

from dataflow.pizza_settings import (
    FLINK_ADVANCED_CONTAINERS,
    FLINK_ADVANCED_JOB_MANAGER_MEMORY,
    FLINK_ADVANCED_SLOTS,
    FLINK_ADVANCED_TASK_MANAGER_MEMORY,
    FLINK_STANDARD_CONTAINERS,
    FLINK_STANDARD_JOB_MANAGER_MEMORY,
    FLINK_STANDARD_SLOTS,
    FLINK_STANDARD_TASK_MANAGER_MEMORY,
)
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.lock import Lock
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.stream.utils.resource_util import get_session_detail_list_for_recover
from dataflow.shared.utils.concurrency import concurrent_call_func
from dataflow.stream.cluster_config.cluster_config_driver import apply_flink_yarn_session
from dataflow.stream.job.job_for_flink import force_kill_flink_job
from dataflow.stream.job.monitor_job import query_unhealth_session
from dataflow.stream.settings import API_ERR_RETRY_TIMES, RECOVER_SESSION_MAX_COROUTINE_NUM


@Lock.func_lock(Lock.LockTypes.RECOVER_SESSION_CLUSTER)
def recover_session_cluster(session_list, geog_area_code, concurrency):
    if not concurrency:
        concurrency = RECOVER_SESSION_MAX_COROUTINE_NUM
    if not session_list:
        session_list = query_unhealth_session()
    # 根据session名称补全需要恢复的session集群配置信息
    recover_session_config_list = get_session_detail_list_for_recover(session_list)
    # recover session
    res_list = []
    func_info = []
    for session_config in recover_session_config_list:
        func_info.append([RecoverSessionHandler(session_config, geog_area_code).recover_session, {}])

    for segment in [func_info[i : i + concurrency] for i in range(0, len(func_info), concurrency)]:
        threads_res = concurrent_call_func(segment)
        for segment_index, res_info in enumerate(threads_res):
            if res_info:
                res_list.append(res_info)
    return res_list


class RecoverSessionHandler(object):
    def __init__(self, session_config, geog_area_code):
        self.session_config = session_config
        self.geog_area_code = geog_area_code
        self.cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")

    # sometimes the jobnavi's gunicorn may appear 502 response, just retry after sleep
    def retry_kill_flink_job_on_api_error(self):
        result = False
        for i in range(API_ERR_RETRY_TIMES):
            try:
                result = force_kill_flink_job(
                    self.geog_area_code,
                    self.cluster_id,
                    self.session_config["cluster_name"],
                    timeout=10,
                )
            except ApiRequestError as e1:
                logger.error(
                    "[recover session] force kill flink job %s call ApiRequestError exception, details is : %s"
                    % (self.session_config["cluster_name"], e1.message)
                )
            if result:
                return result
            time.sleep(1)
        return result

    def recover_session(self):
        # kill
        process_result = self.retry_kill_flink_job_on_api_error()
        # recover
        if process_result:
            params = {
                "component_type": "flink",
                "cluster_type": "yarn-session",
                "cluster_group": None,
                "cluster_label": None,
                "cluster_name": None,
                "version": None,
                "extra_info": {
                    "slot": 0,
                    "job_manager_memory": 0,
                    "task_manager_memory": 0,
                    "container": 0,
                },
                "geog_area_code": self.geog_area_code,
            }
            if self.session_config["cluster_label"] == "standard":
                params["extra_info"]["slot"] = FLINK_STANDARD_SLOTS
                params["extra_info"]["job_manager_memory"] = FLINK_STANDARD_JOB_MANAGER_MEMORY
                params["extra_info"]["task_manager_memory"] = FLINK_STANDARD_TASK_MANAGER_MEMORY
                params["extra_info"]["container"] = FLINK_STANDARD_CONTAINERS
            else:
                params["extra_info"]["slot"] = FLINK_ADVANCED_SLOTS
                params["extra_info"]["job_manager_memory"] = FLINK_ADVANCED_JOB_MANAGER_MEMORY
                params["extra_info"]["task_manager_memory"] = FLINK_ADVANCED_TASK_MANAGER_MEMORY
                params["extra_info"]["container"] = FLINK_ADVANCED_CONTAINERS
            params.update(self.session_config)
            apply_flink_yarn_session(params)
            return self.session_config["cluster_name"]
        else:
            return None
