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

import random
import time
import uuid

from common.exceptions import ApiRequestError
from django.db import transaction
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.exceptions import FlowTaskError
from dataflow.flow.utils.language import Bilingual
from dataflow.flow.utils.timeformat import timestamp2str
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.log import flow_logger as logger

from . import BaseCustomCalculateTaskHandler


class BatchCustomCalculateNodeTask(BaseCustomCalculateTaskHandler):
    task_type = "batch"

    def build_start_custom_cal_context(self, flow_task_handler):
        return self.get_context()

    def build_stop_custom_cal_context(self, flow_task_handler):
        return self.get_context()

    def start_inner(self):

        # 重试 3 次，提高成功率
        index = 1
        max_times = 3

        custom_calculate_id = "custom_calculate_" + str(uuid.uuid1()).replace("-", "")
        while True:
            try:
                res = BatchHelper.create_custom_calculate(
                    custom_calculate_id,
                    ",".join(self.context["rerun_processings"]),
                    self.context["data_start"],
                    self.context["data_end"],
                    self.get_geog_area_code(),
                )
                for processing in res["jobs"]:
                    processing_id = processing["job_id"]
                    schedule_time = processing["schedule_time"]

                    self.log(
                        Bilingual(ugettext_noop("补算任务({processing_id})，调度时间({schedule_time})")).format(
                            processing_id=processing_id,
                            schedule_time=timestamp2str(schedule_time),
                        )
                    )
                self.add_context({"custom_calculate_id": custom_calculate_id})
                break
            except ApiRequestError as e:
                _msg = "第{n}次尝试创建补算任务失败，error={err}".format(n=index, err=e.message)
                logger.error(_msg)

                index += 1
                if index > max_times:
                    raise FlowTaskError(_("创建补算任务失败 - {err}").format(err=e.message))
                time.sleep(random.uniform(1, 2))

    def stop_inner(self):
        BatchHelper.stop_custom_calculate(self.context["custom_calculate_id"], self.get_geog_area_code())

    def start_ok_callback(self):
        super(BatchCustomCalculateNodeTask, self).start_ok_callback()

    def stop_ok_callback(self):
        super(BatchCustomCalculateNodeTask, self).stop_ok_callback()

    @property
    def is_done_processings(self):
        return self.get_context("is_done_processings", [])

    @property
    def check_times(self):
        return self.get_context("check_times", 0)

    def print_log(self, processing_id, schedule_time, status, info):
        if not info:
            self.log(
                Bilingual(ugettext_noop("补算任务({processing_id})，调度时间({schedule_time})，执行状态({status})")).format(
                    processing_id=processing_id,
                    schedule_time=timestamp2str(schedule_time),
                    status=status,
                )
            )
        else:
            self.log(
                Bilingual(
                    ugettext_noop("补算任务({processing_id})，调度时间({schedule_time})，执行状态({status})，原因({info})")
                ).format(
                    processing_id=processing_id,
                    schedule_time=timestamp2str(schedule_time),
                    status=status,
                    info=info,
                ),
                level="ERROR",
            )

    def print_logs(self, data):
        """
        批量插入日志
        @param data:
            [
                {
                    'processing_id': 'xx',
                    'schedule_time': 'xx',
                    'status': 'xx',
                    'info': 'xx'
                }
            ]
        @return:
        """
        logs = []
        for log in data:
            processing_id = log["processing_id"]
            info = log["info"]
            schedule_time = log["schedule_time"]
            status = log["status"]
            if not info:
                _log = Bilingual(ugettext_noop("补算任务({processing_id})，调度时间({schedule_time})，执行状态({status})")).format(
                    processing_id=processing_id,
                    schedule_time=timestamp2str(schedule_time),
                    status=status,
                )
                level = "INFO"
            else:
                _log = Bilingual(
                    ugettext_noop("补算任务({processing_id})，调度时间({schedule_time})，执行状态({status})，原因({info})")
                ).format(
                    processing_id=processing_id,
                    schedule_time=timestamp2str(schedule_time),
                    status=status,
                    info=info,
                )
                level = "ERROR"
            logs.append(
                {
                    "msg": _log,
                    "level": level,
                    "progress": self.flow_task_handler.progress,
                }
            )
        self.logs(logs)

    @transaction.atomic()
    def update_logs(self, jobs):
        if not jobs:
            return
        self.print_logs(jobs)
        added_done_processings = []
        for job in jobs:
            added_done_processings.append([job["processing_id"], job["schedule_time"]])
        is_done_processings = self.is_done_processings
        is_done_processings.extend(added_done_processings)
        self.add_context({"is_done_processings": is_done_processings})

    def check(self):
        if self.is_done:
            return True
        custom_calculate_id = self.context["custom_calculate_id"]
        res = BatchHelper.get_custom_calculate_basic_info(custom_calculate_id)
        self.add_context({"check_times": self.check_times + 1})
        finished_jobs = []
        for processing_job in res["jobs"]:
            processing_id = processing_job["job_id"]
            schedule_time = processing_job["schedule_time"]
            status = processing_job["status"]
            info = processing_job["info"]
            # 其它状态是最终状态
            if [processing_id, schedule_time] not in self.is_done_processings and status.lower() not in [
                "preparing",
                "running",
            ]:
                finished_jobs.append(
                    {
                        "processing_id": processing_id,
                        "schedule_time": schedule_time,
                        "status": status,
                        "info": info,
                    }
                )
        self.update_logs(finished_jobs)
        # 在轮询多次后，若状态依然未变(不存在至少一个已经结束的补算任务)，打印相关信息
        if self.check_times == 6 and not self.is_done_processings:
            self.log(Bilingual(ugettext_noop("当前补算任务可能耗时较长，请耐心等待...")))
        # 共三个状态 running finished killed
        if res["status"] not in ["running"]:
            self.is_done = True
        self.update_progress(res["jobs"])

    def update_progress(self, total_jobs):
        # 更新任务总进度
        if self.is_done:
            progress = 100
        else:
            progress = len(self.is_done_processings) * 100.0 / len(total_jobs) if total_jobs else 0
        self.add_context({"progress": progress})

    def show_finished_jobs(self):
        res = BatchHelper.get_custom_calculate_basic_info(self.get_context("custom_calculate_id"))
        finished_jobs = {}
        for processing_job in res["jobs"]:
            processing_id = processing_job["job_id"]
            processing_info = finished_jobs.get(processing_id, [])
            processing_info.append(processing_job)
            finished_jobs[processing_id] = processing_info
        data = []
        for processing_id, processing_info in list(finished_jobs.items()):
            processing_info.sort(key=lambda k: k.get("schedule_time"), reverse=False)
            for processing_job in processing_info:
                schedule_time = processing_job["schedule_time"]
                status = processing_job["status"]
                info = processing_job["info"]
                data.append(
                    {
                        "processing_id": processing_id,
                        "schedule_time": schedule_time,
                        "status": status,
                        "info": info,
                    }
                )
        self.print_logs(data)
