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

from dataflow.shared.log import stream_logger as logger
from dataflow.stream.settings import API_ERR_RETRY_TIMES


class MonitorScheduleHandler(object):
    def __init__(self, job_id, jobnavi_stream_helper):
        self.job_id = job_id
        self.jobnavi_stream_helper = jobnavi_stream_helper

    # 检测spark job_id 是否在运行
    def check_per_spark_streaming_status(self):
        """
        如job_id对应的任务在YARN上状态异常，则返回该异常的job_id
        如job_id对应的任务在YARN上状态正常，则返回None
        :param job_id: 待检测的任务id
        :return: job_id 异常的任务id
        """
        try:
            cluster_is_exist = None
            execute_id = None
            data = None
            result = None
            jobs_status_list = None
            # sometimes the jobnavi's gunicorn may appear 502 response, just retry
            for i in range(API_ERR_RETRY_TIMES):
                try:
                    cluster_is_exist = self.jobnavi_stream_helper.get_schedule(self.job_id)
                    break
                except ApiRequestError as e1:
                    time.sleep(1)
                    logger.warning(
                        "[get schedule] 实时计算spark structured streaming任务[%s]监控API请求异常, detail:%s"
                        % (self.job_id, e1.message)
                    )
            if cluster_is_exist:
                execute_id = self.jobnavi_stream_helper.get_execute_id(self.job_id, timeout=API_ERR_RETRY_TIMES)
            if execute_id:
                data = self.jobnavi_stream_helper.get_execute_status(execute_id, timeout=API_ERR_RETRY_TIMES)
            if data and "status" in data and data["status"] == "running":
                result = self.jobnavi_stream_helper.get_cluster_app_deployed_status(
                    execute_id, timeout=API_ERR_RETRY_TIMES
                )
            if result:
                event_id = self.jobnavi_stream_helper.request_jobs_status(execute_id)
                jobs_status_list = self.jobnavi_stream_helper.list_jobs_status(event_id, timeout=API_ERR_RETRY_TIMES)
            if jobs_status_list and self.job_id in jobs_status_list:
                return None
        except Exception as e:
            logger.warning("实时计算spark structured streaming任务[{}]运行状态检测异常, detail:{}".format(self.job_id, e))
        return self.job_id
