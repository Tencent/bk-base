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
from abc import abstractmethod

from jobnavi.HAProxy import get_ha_proxy
from jobnavi.jobnavi_logging import get_logger

logger = get_logger()
ha_proxy = get_ha_proxy()


class JobNaviTask(object):
    @abstractmethod
    def run(self, task_event):
        logger.warning("WARNING! Method 'run' may not implemented.")

    def get_event_listener(self, event_name):
        logger.warning("no " + str(event_name) + " listener.")


class EventListener(object):
    @abstractmethod
    def do_event(self, task_event):
        logger.info("call do event.")


class AbstractJobNaviTask(JobNaviTask):
    def run(self, task_event):
        self.task_event = task_event
        self.start_task(task_event)

    @abstractmethod
    def start_task(self, task_event):
        logger.warning("WARNING! Method 'start_task' may not implemented.")

    def do_schedule_savepoint(self, value):
        schedule_id = self.task_event.context.taskInfo.scheduleId
        param = {
            "operate": "save_schedule",
            "schedule_id": schedule_id,
            "savepoint": value,
        }
        ha_proxy.send_request("/savepoint", "post", param)

    def do_savepoint(self, value):
        schedule_id = self.task_event.context.taskInfo.scheduleId
        schedule_time = self.task_event.context.taskInfo.scheduleTime
        param = {
            "operate": "save_schedule",
            "schedule_id": schedule_id,
            "schedule_time": schedule_time,
            "savepoint": value,
        }
        ha_proxy.send_request("/savepoint", "post", param)

    def get_schedule_savepoint(self):
        schedule_id = self.task_event.context.taskInfo.scheduleId
        param = {
            "operate": "get_schedule",
            "schedule_id": schedule_id,
        }
        savepoint_result = ha_proxy.send_request("/savepoint", "post", param)
        if savepoint_result and savepoint_result["result"]:
            return savepoint_result["data"]
        else:
            logger.error("get savepoint error, return result is:" + str(savepoint_result))
            raise Exception("get savepoint error.")

    def get_savepoint(self):
        schedule_id = self.task_event.context.taskInfo.scheduleId
        schedule_time = self.task_event.context.taskInfo.scheduleTime
        param = {
            "operate": "get_schedule",
            "schedule_id": schedule_id,
            "schedule_time": schedule_time,
        }
        savepoint_result = ha_proxy.send_request("/savepoint", "post", param)
        if savepoint_result and savepoint_result["result"]:
            return savepoint_result["data"]
        else:
            logger.error("get savepoint error, return result is:" + str(savepoint_result))
            raise Exception("get savepoint error.")


class TestJobNaviTask(JobNaviTask):
    def __init__(self):
        logger.info("init jobnavi_task")

    def get_event_listener(self, event_name):
        if "runJob" == event_name:
            return RunJobEventListener()

    def run(self, task_event):
        logger.info("run test jobnavi task. event info is:")
        logger.info(task_event)


class RunJobEventListener(EventListener):
    def do_event(self, task_event):
        logger.info("run job")
