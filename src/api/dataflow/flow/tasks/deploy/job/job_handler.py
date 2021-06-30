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
import random
import time

from common.exceptions import ApiRequestError
from common.local import get_request_username
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.component.exceptions.comp_execptions import JobNotExistsException
from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import BatchJob, ModelAppJob, StreamJob
from dataflow.flow.utils.language import Bilingual
from dataflow.modeling.job.job_config_controller import ModelingJobConfigController
from dataflow.modeling.model.model_controller import ModelController
from dataflow.models import ProcessingJobInfo
from dataflow.pizza_settings import BATCH_MODULE_NAME, MODEL_APP_MODULE_NAME, STREAM_MODULE_NAME
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.shared.stream.stream_helper import StreamHelper
from dataflow.stream.handlers.job_handler import JobHandler as StreamJobHandler
from dataflow.udf.debug.debug_driver import generate_job_config as udf_generate_job_config


class JobHandler(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self._job_info = None

    @classmethod
    def build_job_params(
        cls,
        module,
        component_type,
        project_id,
        cluster_group,
        code_version,
        processings,
        job_config,
        deploy_config,
    ):
        geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
        jobserver_config = {"geog_area_code": geog_area_code, "cluster_id": JobNaviHelper.get_jobnavi_cluster(module)}
        params = {}
        if module == STREAM_MODULE_NAME:
            params = {
                "project_id": project_id,
                "code_version": code_version,
                "cluster_group": cluster_group,
                "job_config": job_config,
                "deploy_config": deploy_config,
                "processings": processings,
                "jobserver_config": jobserver_config,
                "component_type": component_type,
            }
        elif module == BATCH_MODULE_NAME:
            params = {
                "processing_id": processings[0],
                "code_version": code_version,
                "cluster_group": cluster_group,
                "cluster_name": "",
                "deploy_mode": "yarn",
                "deploy_config": json.dumps(deploy_config),
                "job_config": job_config,
                "project_id": project_id,
                "jobserver_config": jobserver_config,
            }
        elif module == MODEL_APP_MODULE_NAME:
            params = {
                "processing_id": processings[0],
                "code_version": code_version,
                "cluster_group": cluster_group,
                "cluster_name": "",
                "deploy_mode": "yarn",
                "deploy_config": json.dumps(deploy_config),
                "job_config": job_config,
                "project_id": project_id,
                "jobserver_config": jobserver_config,
            }
        return params

    @classmethod
    def create_job(
        cls,
        module,
        component_type,
        project_id,
        cluster_group,
        code_version,
        processings,
        job_config,
        deploy_config,
    ):
        params = JobHandler.build_job_params(
            module,
            component_type,
            project_id,
            cluster_group,
            code_version,
            processings,
            job_config,
            deploy_config,
        )
        if module == STREAM_MODULE_NAME:
            o_job = StreamJob.create(params, get_request_username())
        elif module == BATCH_MODULE_NAME:
            o_job = BatchJob.create(params, get_request_username())
        elif module == MODEL_APP_MODULE_NAME:
            o_job = ModelAppJob.create(params, get_request_username())
        return {"job_id": o_job.job_id}

    def update_job(
        self,
        module,
        component_type,
        project_id,
        cluster_group,
        code_version,
        processings,
        job_config,
        deploy_config,
    ):
        params = JobHandler.build_job_params(
            module,
            component_type,
            project_id,
            cluster_group,
            code_version,
            processings,
            job_config,
            deploy_config,
        )
        if module == STREAM_MODULE_NAME:
            o_job = StreamJob(job_id=self.job_id)
            o_job.update(params, get_request_username())
        elif module == BATCH_MODULE_NAME:
            o_job = BatchJob(job_id=self.job_id)
            o_job.update(params, get_request_username())
        elif module == MODEL_APP_MODULE_NAME:
            o_job = ModelAppJob(job_id=self.job_id)
            o_job.update(params, get_request_username())
        return {"job_id": self.job_id}

    def start(self, module, params):
        if module == STREAM_MODULE_NAME:
            # get code version
            self.log(Bilingual(ugettext_noop("获取作业代码版本")))
            jar_name = StreamHelper.get_code_version(self.job_id)["jar_name"]
            # register
            self.log(Bilingual(ugettext_noop("开始注册作业拓扑")))
            api_params = {
                "job_id": self.job_id,
                "jar_name": jar_name,
                "geog_area_code": params["tags"][0],
            }
            conf = StreamHelper.register_job(**api_params)
            # submit
            api_params = {"conf": conf, "job_id": self.job_id}
            self.log(Bilingual(ugettext_noop("开始将作业提交至集群")))
            return StreamHelper.submit_job(**api_params)
        elif module == BATCH_MODULE_NAME:
            index = 1
            max_times = 3
            while True:
                try:
                    params["job_id"] = self.job_id
                    BatchHelper.start_job(**params)
                    break
                except ApiRequestError as e:
                    self.log(
                        Bilingual(
                            ugettext_noop("第{n}次尝试 batch.start_job 失败，error={err}".format(n=index, err=e.message))
                        )
                    )
                    index += 1
                    if index > max_times:
                        raise Errors.FlowTaskError(
                            Bilingual(ugettext_noop("连续{n}次尝试 batch.start_job 均失败，流程中止".format(n=index)))
                        )
                    time.sleep(random.uniform(1, 2))
            return {}
        elif module == MODEL_APP_MODULE_NAME:
            index = 1
            max_times = 3
            while True:
                try:
                    params["job_id"] = self.job_id
                    ModelingHelper.start_job(**params)
                    break
                except ApiRequestError as e:
                    self.log(
                        Bilingual(
                            ugettext_noop("第{n}次尝试 model_app.start_job 失败，error={err}".format(n=index, err=e.message))
                        )
                    )
                    index += 1
                    if index > max_times:
                        raise Errors.FlowTaskError(
                            Bilingual(ugettext_noop("连续{n}次尝试 model_app.start_job 均失败，流程中止".format(n=index)))
                        )
                    time.sleep(random.uniform(1, 2))
            return {}

    def sync_status_inner(self, operate_info, operate):
        success_status = "ACTIVE" if operate != "stop" else None
        for i in range(36):
            api_params = {"job_id": self.job_id, "operate_info": operate_info}
            data = StreamHelper.sync_status(**api_params)
            if data is None:
                raise Errors.FlowTaskError(_("同步 JOB 状态失败，返回状态信息为空"))
            job_status = data.get(self.job_id)
            if job_status == success_status:
                self.log(Bilingual(ugettext_noop("确认完毕")))
                return True

            time.sleep(5)

    def sync_status(self, operate_info):
        _operate_info = json.loads(operate_info)
        operate = _operate_info["operate"]
        if operate == "stop":
            self.log(Bilingual(ugettext_noop("确认 JOB 状态是否已清除")))
            if not self.sync_status_inner(operate_info, operate):
                raise Errors.FlowTaskError(_("多次轮询，JOB 依旧处于激活状态"))
            status = "INACTIVE"
        else:
            self.log(Bilingual(ugettext_noop("确认 JOB 是否处于激活状态")))
            if not self.sync_status_inner(operate_info, operate):
                # 同步任务失败，开始强制停止任务
                res_data = False
                count = 0
                delta = 2
                kill_count = 3
                while not res_data and count < kill_count:
                    try:
                        res_data = StreamHelper.force_kill(self.context["job_id"], 50)
                    except Exception as e:
                        raise Errors.FlowTaskError(_("多次轮询，未检测到 JOB 激活状态，强制停止作业失败，明细({})").format(str(e)))
                    if not res_data:
                        time.sleep(delta)
                    count = count + 1
                if count >= kill_count:
                    raise Errors.FlowTaskError(_("启动超时，强制停止作业失败，请联系管理员"))
                raise Errors.FlowTaskError(_("多次轮询，未检测到 JOB 激活状态"))
            status = "ACTIVE"
        return {self.job_id: status}

    def stop(self, module):
        if module == STREAM_MODULE_NAME:
            api_params = {"job_id": self.job_id}
            self.log(Bilingual(ugettext_noop("开始停止作业")))
            data = StreamHelper.cancel_job(**api_params)
            if not data:
                raise Errors.FlowTaskError(_("停止作业失败，返回配置为空"))
            self.log(Bilingual(ugettext_noop("停止成功")))
            return data
        elif module == BATCH_MODULE_NAME:
            self.log(Bilingual(ugettext_noop("停止离线调度进程")))
            api_param = {"job_id": self.job_id}
            BatchHelper.stop_job(**api_param)
            return {}
        elif module == MODEL_APP_MODULE_NAME:
            self.log(Bilingual(ugettext_noop("停止离线调度进程")))
            api_param = {"job_id": self.job_id}
            ModelingHelper.stop_job(**api_param)
            return {}

    def force_kill(self, timeout=180):
        try:
            job_info = ProcessingJobInfo.objects.get(job_id=self.job_id)
        except ProcessingJobInfo.DoesNotExist:
            return None
        if job_info.component_type not in ["flink", "spark_structured_streaming"]:
            raise Errors.ValidError(_("当前任务非 flink 任务，不支持 force_kill"))
        return StreamHelper.force_kill(self.job_id, timeout)

    @property
    def job_info(self):
        if not self._job_info:
            self._job_info = ProcessingJobInfo.objects.get(job_id=self.job_id)
        return self._job_info

    def delete(self):
        if self.job_info.processing_type == STREAM_MODULE_NAME:
            StreamHelper.delete_job(self.job_id)
        elif self.job_info.processing_type == BATCH_MODULE_NAME:
            BatchHelper.delete_job(self.job_id)
        elif self.job_info.processing_type == MODEL_APP_MODULE_NAME:
            ModelingHelper.delete_job(self.job_id)
        else:
            raise Errors.FlowTaskError(_("删除作业失败，不支持的 module 类型: %s" % self.job_info.processing_type))

    def log(self, msg):
        # TODO: 和 node_task 中的方法合二为一
        logger.info(msg)

    def generate_job_config(self, run_mode, job_type):
        if job_type == "flink":
            if run_mode == "product":
                # flink-code, flink-sql
                return StreamJobHandler(self.job_id, False).generate_job_config()
            elif run_mode == "debug":
                # flink-code-debug, flink-sql-debug
                return StreamJobHandler(self.job_id, True).generate_job_config()
            elif run_mode == "udf_debug":
                # flink-sql-udf-debug
                return udf_generate_job_config(self.job_id, job_type)
        elif job_type == "spark_structured_streaming":
            return StreamJobHandler(self.job_id, False).generate_job_config()
        elif job_type == "spark_mllib":
            if run_mode == "product":
                return ModelingJobConfigController(self.job_id, job_type, run_mode, False).generate_job_config()
            elif run_mode == "debug":
                return ModelingJobConfigController(self.job_id, job_type, run_mode, True).generate_debug_config()
            elif run_mode == "release_debug":
                return ModelController().generate_release_debug_config(self.job_id)
        elif job_type == "tensorflow":
            if run_mode == "product":
                return ModelingJobConfigController(self.job_id, job_type, run_mode, False).generate_job_config()
            else:
                raise Exception("暂不支持tensorflow运行模式:{}".format(run_mode))
        elif job_type in ["spark_sql", "hive", "spark_streaming"]:
            raise Exception("暂不支持获取 %s 任务类型配置" % job_type)
        raise JobNotExistsException(
            "作业信息配置不存在，job_type: {job_type}, {run_mode}".format(job_type=job_type, run_mode=run_mode)
        )
