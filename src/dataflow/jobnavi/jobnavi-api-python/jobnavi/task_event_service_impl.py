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
from jobnavi.exception import JobNaviTaskException
from jobnavi.jobnavi_logging import get_logger
from jobnavi.task_event.TaskEventService import Iface
from jobnavi.task_event.ttypes import TaskEventResult, TaskStatus

logger = get_logger()


class TaskEventServiceImpl(Iface):
    def processEvent(self, event):
        logger.info("process event...")
        if event.eventName == "running":
            logger.info("running task...")
            return self.__process_running(event)
        else:
            return self.__process_task_event(event)

    def __process_running(self, event):
        main = event.context.taskInfo.type.main
        main_class = main.split(".")[-1]
        remove_index = (len(main_class) + 1) * -1
        package_path = main[:remove_index]
        if not package_path:
            event.changeStatus = TaskStatus.failed
            event.eventName = "failed"
            next_events = [event]
            result = TaskEventResult(False, "package path can not be null.", next_events)
            return result
        try:
            package_obj = __import__(package_path, fromlist=True)
            if not package_obj:
                raise Exception("can not find package " + package_obj + ".")
            task = getattr(package_obj, main_class)
            self.jobnavi_task = task()
            if not hasattr(self.jobnavi_task, "run"):
                raise Exception("can not find run method.")
            else:
                self.jobnavi_task.run(event)
        except Exception as e:
            logger.exception(e)
            event.changeStatus = TaskStatus.failed
            event.eventName = "failed"
            if isinstance(e, JobNaviTaskException):
                event.changeStatus = e.task_status()
                event.eventName = TaskStatus._VALUES_TO_NAMES[e.task_status()]
            event.context.eventInfo = e.message
            next_events = [event]
            result = TaskEventResult(False, e.message, next_events)
            return result
        event.changeStatus = TaskStatus.finished
        event.eventName = "finished"
        next_events = [event]
        result = TaskEventResult(True, "", next_events)
        logger.info(result)
        return result

    def __process_task_event(self, event):
        try:
            if not self.jobnavi_task:
                raise Exception("task may not start.")
            elif not hasattr(self.jobnavi_task, "get_event_listener"):
                raise Exception("can not find get_event_listener method.")
            else:
                listener = self.jobnavi_task.get_event_listener(event.eventName)
                if listener:
                    result = listener.do_event(event)
                    if not result or not isinstance(result, TaskEventResult):
                        result = TaskEventResult(False, "event listener return value must be TaskEventResult.", [])
                    return result
                else:
                    raise Exception("cannot find listener by event name: " + event.eventName)
        except Exception as e:
            next_events = []
            result = TaskEventResult(False, e.message, next_events)
            return result
