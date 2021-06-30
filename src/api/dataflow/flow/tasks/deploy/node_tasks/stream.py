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

from time import sleep

from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.exceptions import FlowTaskError
from dataflow.flow.settings import SUBMIT_STREAM_JOB_POLLING_TIMES
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.stream.stream_helper import StreamHelper

from . import BaseNodeTaskHandler


class StreamNodeTask(BaseNodeTaskHandler):
    def get_task_type(self):
        return "实时"

    def build_start_context(self, flow_task_handler, o_nodes):
        """
        实时任务启动参数，由 DataFlow 整体配置给出
        """
        operator = flow_task_handler.operator
        task_context = flow_task_handler.context
        _flow_handler = flow_task_handler.flow
        # 申请 job_id
        consuming_mode = task_context["consuming_mode"] if "consuming_mode" in task_context else "continue"
        cluster_group = task_context["cluster_group"] if "cluster_group" in task_context else None
        geog_area_code = TagHelper.get_geog_area_code_by_project(_flow_handler.project_id)
        head_rts, tail_rts = _flow_handler.get_topo_head_tail_rts()
        job_id = _flow_handler.get_or_create_steam_job_id(
            _flow_handler.project_id,
            o_nodes,
            head_rts,
            tail_rts,
            geog_area_code,
            operator,
            consuming_mode,
            cluster_group,
        )

        return {
            "operator": operator,
            "job_id": job_id,
            "heads": ".".join(head_rts),
            "tails": ".".join(tail_rts),
            "geog_area_code": geog_area_code,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        operator = flow_task_handler.operator
        _flow_handler = flow_task_handler.flow
        geog_area_code = TagHelper.get_geog_area_code_by_project(_flow_handler.project_id)
        job_id = _flow_handler.get_stream_job_id(o_nodes)

        return {
            "job_id": job_id,
            "operator": operator,
            "geog_area_code": geog_area_code,
        }

    def start_inner(self):
        self.log(Bilingual(ugettext_noop("启动 {task_type} 任务进程")).format(task_type=self.get_task_type()))
        try:
            self.lock()
            self.get_code_version()
            # register和submit本质上可以在stream API统一实现
            # 这里通过flow API两步调用是为了减少超时风险
            self.register()
            self.submit()
            self.confirm_job_status_exist()
        except Exception:
            # 不做任何处理，重新抛出
            raise
        finally:
            self.unlock()

    def stop_inner(self):
        if "job_id" not in self.context or not self.context["job_id"]:
            # job_id 不存在，直接停止成功
            self.log(Bilingual(ugettext_noop("job_id 不存在，flow(%s)停止成功" % (self.flow_task_handler.flow_id))))
            return
        self.log(Bilingual(ugettext_noop("停止 {task_type} 任务进程")).format(task_type=self.get_task_type()))
        try:
            self.lock()
            self.stop_job()
            self.confirm_job_status_not_exist()
        except Exception:
            # 不做任何处理，重新抛出
            raise
        finally:
            self.unlock()

    def lock(self):
        job_id = self.context["job_id"]
        self.log(Bilingual(ugettext_noop("开始锁定作业，job_id={job_id}")).format(job_id=job_id))
        StreamHelper.lock_job(job_id)
        self.log(Bilingual(ugettext_noop("成功锁定作业")))

    def unlock(self):
        job_id = self.context["job_id"]
        StreamHelper.unlock_job(job_id)
        self.log(Bilingual(ugettext_noop("成功解锁作业")))

    def get_code_version(self):
        self.log(Bilingual(ugettext_noop("获取作业代码版本")))
        job_id = self.context["job_id"]
        data = StreamHelper.get_code_version(job_id)
        self.log(Bilingual(ugettext_noop("获取成功")))
        self.add_context({"jar_name": data["jar_name"]})

    def register(self):
        api_params = {
            "job_id": self.context["job_id"],
            "jar_name": self.context["jar_name"],
            "geog_area_code": self.context["geog_area_code"],
        }
        self.log(Bilingual(ugettext_noop("开始注册作业拓扑")))
        data = StreamHelper.register_job(**api_params)
        if data is None:
            raise FlowTaskError(_("注册作业失败，返回配置为空"))
        self.log(Bilingual(ugettext_noop("注册成功")))

        self.add_context({"conf": data})

    def submit(self):
        api_params = {"conf": self.context["conf"], "job_id": self.context["job_id"]}

        self.log(Bilingual(ugettext_noop("开始将作业提交至集群")))
        data = StreamHelper.submit_job(**api_params)
        if data is None:
            raise FlowTaskError(_("提交作业失败，返回配置为空"))
        self.log(Bilingual(ugettext_noop("提交成功")))
        self.add_context({"operate_conf": data})

    def stop_job(self):
        api_params = {"job_id": self.context["job_id"]}

        self.log(Bilingual(ugettext_noop("开始停止作业")))
        data = StreamHelper.cancel_job(**api_params)
        if data is None:
            raise FlowTaskError(_("停止作业失败，返回配置为空"))
        self.log(Bilingual(ugettext_noop("停止成功")))
        self.add_context({"operate_conf": data})

    def confirm_job_status_exist(self):
        self.log(Bilingual(ugettext_noop("确认 JOB 是否处于激活状态")))
        for i in range(SUBMIT_STREAM_JOB_POLLING_TIMES):
            job_status = self.get_job_status()
            if job_status == "ACTIVE":
                self.log(Bilingual(ugettext_noop("确认完毕")))
                return True

            sleep(5)
        # 同步任务失败，开始强制停止任务
        res_data = False
        count = 0
        delta = 2
        kill_count = 3
        while not res_data and count < kill_count:
            try:
                res_data = StreamHelper.force_kill(self.context["job_id"], 50)
            except Exception as e:
                raise FlowTaskError(_("多次轮询，未检测到 JOB 激活状态，强制停止作业失败，明细({})").format(str(e)))
            if not res_data:
                sleep(delta)
            count = count + 1
        if count >= kill_count:
            raise FlowTaskError(_("启动超时，强制停止作业失败，请联系管理员"))
        raise FlowTaskError(_("多次轮询，未检测到 JOB 激活状态"))

    def confirm_job_status_not_exist(self):
        self.log(Bilingual(ugettext_noop("确认 JOB 状态是否已清除")))

        for i in range(36):
            job_status = self.get_job_status()
            if job_status is None:
                self.log(Bilingual(ugettext_noop("确认完毕")))
                return True

            sleep(5)

        raise FlowTaskError(_("多次轮询，JOB 依旧处于激活状态"))

    def get_job_status(self):
        api_params = {"job_id": self.context["job_id"]}
        api_params.update(self.context["operate_conf"])
        data = StreamHelper.sync_status(**api_params)
        if data is None:
            raise FlowTaskError(_("同步 JOB 状态失败，返回状态信息为空"))
        job_status = data.get(self.context["job_id"])
        job_status_display = (
            Bilingual(ugettext_noop("未启动")) if job_status != "ACTIVE" else Bilingual(ugettext_noop("已启动"))
        )
        self.log(Bilingual(ugettext_noop("同步状态，当前 JOB 状态 {job_status}")).format(job_status=job_status_display))
        return job_status
