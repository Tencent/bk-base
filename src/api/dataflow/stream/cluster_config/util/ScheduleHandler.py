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

from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.settings import API_ERR_RETRY_TIMES


class ScheduleHandler(object):
    def __init__(self, schedule_id, geog_area_code):
        self.schedule_id = schedule_id
        self.geog_area_code = geog_area_code
        self.cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")

    def list_status(self):
        """
        @return:
            {
                {
                    u'jid': u'5c201c8826bbefc9ce2ced7c81f7d035',
                    u'end-time': -1,
                    u'start-time': 1586945646288,
                    u'name': u'249_9c4ac1e3251e4ab3a4d76a73b9e51f6f',
                    u'last-modification': 1586945646301,
                    u'state': u'RUNNING',
                    u'tasks': {
                        u'scheduled': 0,
                        u'failed': 0,
                        u'reconciling': 0,
                        u'created': 0,
                        u'canceling': 0,
                        u'finished': 0,
                        u'canceled': 0,
                        u'running': 1,
                        u'total': 1,
                        u'deploying': 0
                    },
                    u'duration': 497240143
                }
            }
        """
        jobs_overview = self.send_event("jobs_overview")
        jobs_overview["schedule_id"] = self.schedule_id
        return jobs_overview

    def send_event(self, event_name):
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        execute_id = jobnavi_stream_helper.get_execute_id(self.schedule_id, API_ERR_RETRY_TIMES)
        # 调用获取 job 状态的接口
        event_id = jobnavi_stream_helper.send_event(execute_id, event_name)
        # 根据 event_id 获取 overview 信息
        overview = json.loads(jobnavi_stream_helper.list_jobs_status(event_id))
        return overview
