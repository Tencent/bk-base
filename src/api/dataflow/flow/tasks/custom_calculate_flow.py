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

import importlib
import json
import sys
from datetime import datetime

from common.bklanguage import BkLanguage
from common.local import get_local_param
from django.db import transaction
from django.utils import translation
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.exceptions import FlowCustomCalculateError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.models import FlowExecuteLog, FlowInfo
from dataflow.flow.utils.decorators import ignore_exception
from dataflow.flow.utils.language import Bilingual
from dataflow.flow.utils.local import activate_request
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.send_message import get_common_message_title, send_message

from .custom_calculate.batch import BatchCustomCalculateNodeTask

importlib.reload(sys)
# sys.setdefaultencoding("utf-8")


class CustomCalculateTerminatedError(FlowCustomCalculateError):
    pass


class FlowCustomCalculateHandler(object):
    M_NODE_TASK = {"batch": BatchCustomCalculateNodeTask}

    @classmethod
    def create(cls, flow_id, operator, action, status, context=None, version=None):
        kwargs = {
            "flow_id": flow_id,
            "created_by": operator,
            "action": action,
            "status": status,
        }
        if context is not None:
            kwargs["context"] = json.dumps(context)
        if version is not None:
            kwargs["version"] = version

        o_flow_task = FlowExecuteLog.objects.create(**kwargs)
        return cls(o_flow_task.id)

    def __init__(self, id):
        self.id = id
        self.flow_task = FlowExecuteLog.objects.get(id=id)

        # 缓存 FlowHandler 对象，FlowInstance 对象
        self._flow = None
        self._instance = None

        # 上一次启动的版本
        self.pre_version = None

    @transaction.atomic()
    def execute_before(self, operator):
        self.log(Bilingual(ugettext_noop("******************** 开始补算任务 ********************")))
        node_tasks = self.build_custom_calculate_tasks("start")
        operators = self.get_settings_context("operators")
        if not operators:
            operators = [operator]
        else:
            operators.append(operator)
        self.set_settings_context("operators", operators)
        for _nt in node_tasks:
            _nt.start()
        self.flow_task.set_status(FlowExecuteLog.STATUS.RUNNING)

    def execute(self):
        # 重新初始化线程变量
        activate_request(self.flow_task.created_by)
        try:
            node_tasks = self.build_custom_calculate_tasks("start")
            self.check_custom_calculate(node_tasks)
        except CustomCalculateTerminatedError:
            self.fail_callback(Bilingual(ugettext_noop("当前补算任务被中止")))
        except Exception as e:
            logger.exception(_("轮询过程出现非预期异常，errors={}").format(str(e).replace("\n", " ")))

    @ignore_exception
    def send_message(self, message):
        """
        发送消息通知，根据用户语言发送相关信息
        对于详细信息，若有报错，信息用英文(celery任务默认语言为英文)，不报错则根据用户语言发送详细信息
        """
        flow_task = self.flow_task
        operators = self.get_settings_context("operators")
        if not operators:
            logger.info("当前补算任务(%s)邮件为空" % flow_task.id)
            operators = [flow_task.created_by]
        # 若获取不到用户的语言配置(如内部版)，采用用户访问时的语言环境
        language = BkLanguage.get_user_language(operators[0], default=get_local_param("request_language_code"))
        translation.activate(language)
        receivers = set(operators)
        flow_name = FlowInfo.objects.get(flow_id=flow_task.flow_id).flow_name
        title = get_common_message_title()
        content = _(
            "通知类型: {message_type}<br>"
            "任务名称: {flow_name}<br>"
            "部署类型: {op_type}<br>"
            "部署状态: {result}<br>"
            "详细信息: <br>{message}"
        ).format(
            message_type=_("任务补算"),
            flow_name=flow_name,
            op_type=dict(FlowExecuteLog.ACTION_TYPES_CHOICES).get(flow_task.action, flow_task.action),
            result=_("执行完成"),
            message=message,
        )
        send_message(receivers, title, content)
        translation.activate("en")

    def ok_callback(self, message):
        self.flow_task.set_status(FlowExecuteLog.STATUS.FINISHED)
        self.send_message(message)

    def fail_callback(self, message):
        self.flow_task.set_status(FlowExecuteLog.STATUS.FAILURE)
        self.send_message(message)

    def build_custom_calculate_tasks(self, action):
        """
        组装补算任务列表
        """
        tasks = []

        for task_type in self.get_all_task_context():
            if task_type in list(self.M_NODE_TASK.keys()):
                _task = self.M_NODE_TASK[task_type](self, action)
                tasks.append(_task)
            else:
                raise Exception("Makeup task not found.")

        return tasks

    def lock(self):
        self.log(Bilingual(ugettext_noop("锁定任务")))
        self.flow.lock("补算流程中")

    def unlock(self):
        self.log(Bilingual(ugettext_noop("解锁任务")))
        self.flow.unlock()

    def stop(self):
        node_tasks = self.build_custom_calculate_tasks("stop")
        for _nt in node_tasks:
            _nt.stop()

    def validate_status(self, node_tasks):
        if self.check_terminated():
            raise CustomCalculateTerminatedError()
        for node_task in node_tasks:
            if not node_task.is_done:
                return True
        return False

    @ignore_exception
    def show_finished_jobs(self, node_tasks):
        for node_task in node_tasks:
            node_task.show_finished_jobs()

    def check_custom_calculate(self, node_tasks):
        if self.validate_status(node_tasks):
            # 检查各节点补算状态，更新补算日志
            for _nt in node_tasks:
                _nt.check()
            if not [_nt for _nt in node_tasks if not _nt.is_done]:
                self.progress = 100.0
                self.log(Bilingual(ugettext_noop("******************** 补算结束，结果如下 ********************")))
                self.show_finished_jobs(node_tasks)
                self.log(Bilingual(ugettext_noop("******************** 结束补算任务 ********************")))
                # 确保状态后再解锁，以防用户点停止补算后立刻操作加锁，而误错误解锁
                if self.flow.status == FlowInfo.STATUS.RUNNING:
                    self.unlock()
                if self.flow_task.status not in [FlowExecuteLog.STATUS.TERMINATED]:
                    self.ok_callback(Bilingual(ugettext_noop("无")))
        self.update_progress()

    def terminate(self):
        """
        中止补算任务
        """
        if self.flow_task.status not in [
            FlowExecuteLog.STATUS.PENDING,
            FlowExecuteLog.STATUS.RUNNING,
        ]:
            raise CustomCalculateTerminatedError(_("当前不存在正在补算的任务，不允许停止"))
        # 当前补算任务已然结束(可能是轮询任务将状态进行更新)，不需要进行额外的中止动作
        if self.flow_task.status in FlowExecuteLog.FINAL_STATUS:
            return
        try:
            self.stop()
            self.flow_task.set_status(FlowExecuteLog.STATUS.TERMINATED)
        finally:
            # 正确终止，解锁
            self.flow.unlock()

    def log(self, msg, level="INFO", time=None):
        if self.check_terminated():
            raise CustomCalculateTerminatedError()
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = [
            {
                "msg": msg,
                "level": level,
                "time": _time,
                "progress": "%.1f" % float(self.progress),
            }
        ]
        self.logs(data)

    def logs(self, data):
        self.flow_task.add_logs(data)

    def check_terminated(self):
        """
        检查当前补算任务是否被中止，实时从 DB 获取
        """
        return FlowExecuteLog.objects.get(id=self.id).status == FlowExecuteLog.STATUS.TERMINATED

    @property
    def flow(self):
        if self._flow is None:
            self._flow = FlowHandler(flow_id=self.flow_id)
        return self._flow

    @property
    def flow_id(self):
        return self.flow_task.flow_id

    @property
    def action_type(self):
        return self.flow_task.action

    @property
    def context(self):
        _context = self.flow_task.get_context()
        return {} if _context is None else _context

    def set_settings_context(self, key, value):
        _context = self.context
        if "settings" in _context:
            _context["settings"][key] = value
        else:
            _context["settings"] = {key: value}
        self.flow_task.save_context(_context)

    def get_settings_context(self, key):
        _context = self.context
        if "settings" in _context and key in _context["settings"]:
            return _context["settings"][key]
        else:
            return None

    def set_task_context(self, task_type, task_context):
        _context = self.context
        if "tasks" not in _context:
            _context["tasks"] = {}
        _context["tasks"][task_type] = task_context
        self.flow_task.save_context(_context)

    def get_task_context(self, task_type, key=None):
        task_context = self.get_all_task_context().get(task_type)
        if task_context:
            if key is None:
                return task_context
            elif key in task_context:
                return task_context[key]
        return None

    def get_all_task_context(self):
        return self.context.get("tasks", {})

    def update_progress(self):
        all_progress = []
        for task_type, task_context in list(self.get_all_task_context().items()):
            all_progress.append(task_context.get("progress", 0))
        # 整体补算进度最小为 10%
        self.progress = max(sum(all_progress) * 1.0 / len(all_progress) if all_progress else 0, 10)

    @property
    def progress(self):
        # 默认最小进度10
        return self.context.get("progress", 10)

    @progress.setter
    def progress(self, progress):
        _context = self.context
        _context["progress"] = progress
        self.flow_task.save_context(_context)
