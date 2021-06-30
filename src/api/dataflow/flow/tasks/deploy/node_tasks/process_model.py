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

from common.exceptions import ApiRequestError
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.exceptions import FlowTaskError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.settings import SUBMIT_STREAM_JOB_POLLING_TIMES
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.model.process_model_helper import ProcessModelHelper

from . import BaseNodeTaskHandler


class ProcessModelNodeTask(BaseNodeTaskHandler):
    def build_start_context(self, flow_task_handler, o_nodes):
        # 1. update or create job
        task_context = flow_task_handler.context
        consuming_mode = task_context["consuming_mode"] if "consuming_mode" in task_context else "continue"
        cluster_group = task_context["cluster_group"] if "cluster_group" in task_context else None
        logger.info("ProcessModelNodeTask.build_start_context, task_context (%s)" % (task_context))
        job_id = task_context["job_id"] if "job_id" in task_context else None
        task_display = Bilingual.from_lazy(o_nodes[0].get_node_type_display())

        return {
            "processing_id": o_nodes[0].processing_id,
            "job_id": job_id,
            "offset": consuming_mode,
            "cluster_group": cluster_group,
            "task_display": task_display,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        # 1. update or create job
        task_display = Bilingual.from_lazy(o_nodes[0].get_node_type_display())
        return {"processing_id": o_nodes[0].processing_id, "task_display": task_display}

    def start_inner(self):
        # 1. submit
        task_display = self.context["task_display"]
        processing_id = self.context["processing_id"]
        offset = self.context["offset"]
        cluster_group = self.context["cluster_group"]
        self.log(Bilingual(ugettext_noop("启动 {task_display} 实例")).format(task_display=task_display))
        api_params = {
            "processing_id": processing_id,
            "offset": offset,
            "cluster_group": cluster_group,
        }
        start_result = ProcessModelHelper.start_job(api_params)
        if start_result and "job_id" in start_result and "module" in start_result:
            job_id = start_result["job_id"]
            job_type = start_result["module"]
            self.log(
                Bilingual(ugettext_noop("启动 {task_display} 实例完成, job_id {job_id}")).format(
                    task_display=task_display, job_id=job_id
                )
            )
            # update job_id info in dataflow_job
            # 将job_id保存到flow表，仅记录实时节点
            for _n in self.nodes:
                NodeUtils.update_job_id(_n.flow_id, _n.node_id, job_id, job_type + "_model")

        # 2. confirm_job_status_exist
        # self.confirm_job_status_exist()

    def stop_inner(self):
        processing_id = self.context["processing_id"]
        task_display = self.context["task_display"]
        self.log(Bilingual(ugettext_noop("停止 {task_display} 实例")).format(task_display=task_display))
        api_params = {"processing_id": processing_id}
        try:
            # 先获取配置，获取配置异常，说明已经被删除
            ProcessModelHelper.get_processing(processing_id)
        except ApiRequestError:
            logger.info("processing_id(%s)已经被删除，无需停止" % processing_id)
            return

        ProcessModelHelper.stop_job(**api_params)
        # self.confirm_job_status_not_exist()

    def confirm_job_status_exist(self):
        """
        @step 任务步骤，轮训确认 job 状态存在
        """
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
                res_data = ProcessModelHelper.force_kill(self.context["processing_id"], 50)
            except Exception as e:
                raise FlowTaskError(_("多次轮询，未检测到 JOB 激活状态，强制停止作业失败，明细({})").format(str(e)))
            if not res_data:
                sleep(delta)
            count = count + 1
        if count >= kill_count:
            raise FlowTaskError(_("启动超时，强制停止作业失败，请联系管理员"))
        raise FlowTaskError(_("多次轮询，未检测到 JOB 激活状态"))

    def confirm_job_status_not_exist(self):
        """
        @step 任务步骤，轮训确认 job 状态不存在
        """
        self.log(Bilingual(ugettext_noop("确认 JOB 状态是否已清除")))

        for i in range(36):
            job_status = self.get_job_status()
            if job_status is None:
                self.log(Bilingual(ugettext_noop("确认完毕")))
                return True

            sleep(5)

        raise FlowTaskError(_("多次轮询，JOB 依旧处于激活状态"))

    def get_job_status(self):
        api_params = {
            "processing_id": self.context["processing_id"],
        }
        # api_params.update(self.context['operate_conf'])
        data = ProcessModelHelper.sync_status(**api_params)
        if data is None:
            raise FlowTaskError(_("同步 JOB 状态失败，返回状态信息为空"))
        job_status = data.get(self.context["processing_id"])
        job_status_display = (
            Bilingual(ugettext_noop("未启动")) if job_status != "ACTIVE" else Bilingual(ugettext_noop("已启动"))
        )
        self.log(Bilingual(ugettext_noop("同步状态，当前 JOB 状态 {job_status}")).format(job_status=job_status_display))
        return job_status
