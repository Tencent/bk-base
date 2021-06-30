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
import time
import uuid

from common.exceptions import ApiRequestError
from common.local import get_request_username
from django.db import transaction
from django.utils.translation import ugettext as _

from dataflow.component.utils.time_util import get_datetime
from dataflow.pizza_settings import JOBNAVI_STREAM_CLUSTER_ID
from dataflow.shared.flow.flow_helper import FlowHelper
from dataflow.shared.handlers import dataflow_job, processing_udf_job
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.stream.utils.resource_util import get_resource_group_id
from dataflow.shared.utils.concurrency import concurrent_call_func
from dataflow.stream.exceptions.comp_execptions import (
    DeleteJobError,
    ProcessingParameterNotExistsError,
    SubmitFlinkJobError,
)
from dataflow.stream.handlers import processing_job_info, processing_stream_info, processing_stream_job
from dataflow.stream.job.code_job import CodeJob
from dataflow.stream.job.flink_job import FlinkJob
from dataflow.stream.job.monitor_job import query_unhealth_job
from dataflow.stream.processing.processing_for_code import Processing
from dataflow.stream.settings import API_ERR_RETRY_TIMES, RECOVER_JOB_MAX_COROUTINE_NUM, ImplementType


class JobHandler(object):
    """
    job 静态表 Handler
    """

    _job_info = None
    _job = None
    _job_id = None
    is_debug = False

    def __init__(self, id, is_debug=False):
        # 初始化普通变量
        self._job_id = id
        self.is_debug = is_debug
        # 初始化 job 实例
        self._job = self._build_inner_job()

    @property
    def real_job_id(self):
        if self.is_debug:
            return self._job_id.split("__")[1]
        else:
            return self._job_id

    def _build_inner_job(self):
        """
        初始化 handler 时调用，当前共有三种 job
        @return:
        """
        if ImplementType.CODE.value == self.implement_type:
            if self.component_type_without_version in [
                "spark_structured_streaming",
                "flink",
            ]:
                return CodeJob(self._job_id, self.is_debug)
        elif self.component_type in ["flink"]:
            return FlinkJob(self._job_id, self.is_debug)
        elif self.component_type in ["storm"]:
            from dataflow.stream.extend.job.storm_job import StormJob

            return StormJob(self._job_id, self.is_debug)
        else:
            raise Exception("component_type(%s) is not defined." % self.component_type)

    @property
    def component_type_without_version(self):
        return self.component_type.split("-")[0]

    def get_job_info(self, related):
        return self._job.get_job_info(related)

    def lock(self):
        self._job.lock()

    def get_code_version(self):
        return self._job.get_code_version()

    def register(self, jar_name, geog_area_code):
        return self._job.register(jar_name, geog_area_code)

    def submit(self, conf):
        return self._job.submit(conf)

    def sync_status(self, operate_info):
        return self._job.sync_status(operate_info)

    @transaction.atomic()
    def unlock(self, entire=False):
        """
        对实时作业相关信息进行解锁
        @param entire: 是否对所有信息进行解锁
        @return:
        """
        self._job.unlock(entire)

    def cancel(self):
        return self._job.cancel()

    def force_kill(self, timeout=180):
        return self._job.force_kill(timeout)

    @property
    def component_type(self):
        return self.job_info.component_type

    @property
    def implement_type(self):
        return self.job_info.implement_type

    @property
    def job_info(self):
        if not self._job_info:
            self._job_info = processing_job_info.get(self.real_job_id)
        return self._job_info

    @property
    def deploy_mode(self):
        return self.job_info.deploy_mode

    @property
    def deploy_config(self):
        return self.job_info.deploy_config

    @property
    def code_version(self):
        return self.job_info.code_version

    @property
    def job_config(self):
        return json.loads(self.job_info.job_config)

    @transaction.atomic()
    def update_job(self, args):
        self._job.update(args)
        return self._job_id

    @transaction.atomic()
    def update_job_conf(self, args):
        self._job.update_job_conf(args)
        return self._job_id

    @transaction.atomic()
    def delete(self, is_force=False):
        """
        删除作业静态配置和动态配置
        @param is_force: 是否需先确保内部的关联关系已删除
        @return:
        """
        if not is_force:
            if processing_udf_job.exists(job_id=self.real_job_id):
                raise DeleteJobError(_("JOB 存在关联信息，不允许删除"))
            stream_job = processing_stream_job.get(self.real_job_id)
            if stream_job and stream_job.status == "running":
                raise DeleteJobError(_("JOB 正在运行，不允许删除"))
        processing_stream_job.delete(self.real_job_id)
        processing_job_info.delete(self.real_job_id)

    @classmethod
    @transaction.atomic()
    def create_job(cls, args):
        if len(args["processings"]) == 0:
            raise ProcessingParameterNotExistsError()

        # 获取1个processing来确定job的 component_type 和 implement_type
        one_processing = Processing(args["processings"][0])
        deploy_mode = one_processing.deploy_mode
        implement_type = one_processing.implement_type
        component_type = one_processing.component_type
        programming_language = one_processing.programming_language
        args["jobserver_config"] = json.dumps(args["jobserver_config"])

        job_id = "{}_{}".format(args["project_id"], str(uuid.uuid4()).replace("-", ""))

        if implement_type == "code" and component_type == "spark_structured_streaming":
            processor_logic = json.loads(one_processing.processing_info.processor_logic)
            args["job_config"]["processor_type"] = "spark_python_code"
            args["job_config"]["processor_logic"] = {}
            args["job_config"]["processor_logic"]["programming_language"] = programming_language
            args["job_config"]["processor_logic"]["user_args"] = processor_logic["user_args"]
            args["job_config"]["processor_logic"]["advanced"] = processor_logic["advanced"]
        # save processing_stream_info
        for processing in args["processings"]:
            processing_stream_info.update(processing, stream_id=job_id)
        # job_config 存储 processing 信息
        args["job_config"]["processings"] = args["processings"]

        # flow传递过来的cluster_group通过查询资源系统立即转为确切的resource_group_id并存储供后续流程使用
        jobserver_config = json.loads(args["jobserver_config"])
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_group = args["cluster_group"]
        # flink code当前component_type为flink-1.10.1这种结构
        component_type_without_version = component_type.split('-')[0]
        resource_group_id = get_resource_group_id(geog_area_code, cluster_group, component_type_without_version)
        # save processing_stream_job
        processing_job_info.save(
            job_id=job_id,
            processing_type="stream",
            deploy_mode=deploy_mode,
            implement_type=implement_type,
            component_type=component_type,
            programming_language=programming_language,
            code_version=args["code_version"],
            cluster_group=resource_group_id,
            jobserver_config=args["jobserver_config"],
            job_config=json.dumps(args["job_config"]),
            deploy_config=json.dumps(args.get("deploy_config") or {}),
            created_at=get_datetime(),
            created_by=get_request_username(),
        )

        return job_id

    def generate_job_config(self):
        """
        提供给flow调用,用于uc启动时获取该job配置信息
        """
        snapshot_config = processing_stream_job.get(self.real_job_id).snapshot_config
        # 一般情况下获取的都是存储在快照中的submit时生成的uc_config配置,而不是重新生成
        if snapshot_config and not self.is_debug:
            snapshot_config = json.loads(snapshot_config)
            uc_config = snapshot_config["uc_config"]
        else:
            uc_config = self._job.generate_job_config()
        return uc_config

    def get_checkpoint(self, related, extra):
        """
        获取该job在redis中的所有checkpoint记录的信息
        """
        return self._job.get_checkpoint(related, extra)

    def reset_checkpoint_on_migrate_kafka(self, head_rt):
        """
        迁移RT所在kafka时重置redis中RT对应的相关checkpoint
        """
        return self._job.reset_checkpoint_on_migrate_kafka(head_rt)

    def transform_checkpoint(self):
        """
        将该job在redis中的所有checkpoint记录的信息从storm格式转换为flink格式
        """
        return self._job.transform_checkpoint()

    def transform_config(self):
        """
        将该job配置从storm转换为flink配置
        """
        return self._job.transform_config()

    def revert_storm(self):
        """
        转换配置失败时候回滚为storm类型
        """
        self._job.revert_storm()

    def migrate(self, step_id):
        """
        转换配置失败时候回滚为storm类型
        """
        result = {}
        flow_id = list(dataflow_job.filter(job_id=self._job_id).only("flow_id").values()).first()["flow_id"]
        if step_id == 1:
            # 步骤一：停止stream任务
            result["step_1_stop"] = FlowHelper.op_flow_task(flow_id, "stop")
        elif step_id == 2:
            # 步骤二：转换checkpoint
            result["step_2_checkpoint"] = self.transform_checkpoint()
        elif step_id == 3:
            # 步骤三：转换配置
            result["step_3_config"] = self.transform_config()
        elif step_id == 4:
            # 步骤四：启动任务
            result["step_4_start"] = FlowHelper.op_flow_task(flow_id, "start")
        elif step_id == 5:
            # 全部步骤都执行
            result["step_1_stop"] = FlowHelper.op_flow_task(flow_id, "stop")
            result["step_2_checkpoint"] = self.transform_checkpoint()
            result["step_3_config"] = self.transform_config()
            result["step_4_start"] = FlowHelper.op_flow_task(flow_id, "start")
        return result

    @transaction.atomic()
    def update_stream_conf(self, params):
        self._job.update_stream_conf(params)
        return self._job_id

    @classmethod
    @transaction.atomic()
    def create_single_processing_job(cls, args):
        processor_args, job_info_args = cls.build_single_processing_param(args)
        # 1. create processing
        processing_stream_info.save(
            processing_id=args["processings"][0],
            processor_type="code",
            processor_logic=json.dumps(processor_args),
            component_type=args["component_type"],
            created_at=get_datetime(),
            created_by=get_request_username(),
            description="",
        )
        # 2. create job_info
        job_id = JobHandler.create_job(job_info_args)
        return job_id

    def update_single_processing_job(self, args):
        processor_args, job_info_args = self.build_single_processing_param(args)
        # 1. create processing
        processing_stream_info.update(
            processing_id=args["processings"][0],
            processor_type="code",
            processor_logic=json.dumps(processor_args),
            component_type=args["component_type"],
            created_at=get_datetime(),
            created_by=get_request_username(),
            description="",
        )
        # 2. create job_info
        job_id = self.update_job(job_info_args)
        return job_id

    @classmethod
    def build_single_processing_param(cls, args):
        heads = args["job_config"]["heads"].split(",")
        tails = args["job_config"]["tails"].split(",")
        processor_args = {
            "static_data": [],
            "input_result_tables": heads,
            "output_result_tables": tails,
            "code": "",
        }
        processor_args.update(args["job_config"]["processor_logic"])
        job_info_args = {
            "project_id": args["project_id"],
            "code_version": "",
            "deploy_config": "",
            "jobserver_config": {
                "geog_area_code": args["tags"][0],
                "cluster_id": JOBNAVI_STREAM_CLUSTER_ID,
            },
            "cluster_group": args["cluster_group"],
            "processings": args["processings"],
            "job_config": {
                "tails": args["job_config"]["tails"],
                "heads": args["job_config"]["heads"],
                "concurrency": args["deploy_config"]["resource"]["concurrency"],
                "offset": args["job_config"]["offset"],
            },
        }
        return processor_args, job_info_args

    def start_stream(self):
        """
        支持单个spark structured streaming processing任务提交
        兼容从flow迁移过来的AIops任务提交接口
        """
        jar_name = self.get_code_version()
        jobserver_config = json.loads(self.job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        # 1. 注册stream_job
        submit_params = self._job.register(jar_name, geog_area_code)
        # 2. 提交任务
        try:
            operate_info = self._job.submit(submit_params)
        except Exception as e:
            logger.exception(e)
            raise SubmitFlinkJobError("submit job (%s) failed." % self._job_id)
        operate_info["job_id"] = self._job_id
        return operate_info

    def recover(self, timeout=API_ERR_RETRY_TIMES):
        """
        快速恢复任务运行
        提交任务使用snapshot_config中cache的配置(submit_config)
        获取配置使用snapshot_config中cache的配置(uc_config)
        """
        # step-1: kill任务并删除schedule
        self.force_kill(timeout)
        # step-2: 重新提交任务
        snapshot_config = processing_stream_job.get(self._job_id).snapshot_config
        snapshot_config = json.loads(snapshot_config)
        submit_config = snapshot_config["submit_config"]
        # sometimes the jobnavi's gunicorn may appear 502 response, just retry after sleep
        for i in range(timeout):
            result = None
            try:
                result = self._job.submit(submit_config, is_recover=True)
            except ApiRequestError as e1:
                logger.error(
                    "[recover session] force kill flink job %s call ApiRequestError exception, details is : %s"
                    % (self.session_config["cluster_name"], e1.message)
                )
            if result:
                return self._job_id
            time.sleep(1)
        return None

    def fill_snapshot(self):
        """
        生成并填充快照配置
        """
        jar_name = self.get_code_version()
        jobserver_config = json.loads(self.job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        submit_params = self._job.register(jar_name, geog_area_code, is_fill_snapshot=True)
        snapshot_config = {
            "submit_config": submit_params,
            "uc_config": self.generate_job_config(),
        }
        processing_stream_job.update(self._job_id, snapshot_config=json.dumps(snapshot_config))
        return snapshot_config

    @classmethod
    def recover_jobs(cls, job_id_list, concurrency):
        if not concurrency:
            concurrency = RECOVER_JOB_MAX_COROUTINE_NUM
        if not job_id_list:
            # cluster_job + session_job 一起恢复
            unhealth_jobs = query_unhealth_job()
            job_id_list = []
            job_id_list = job_id_list + unhealth_jobs["yarn_cluster_job"]
            job_id_list = job_id_list + unhealth_jobs["yarn_session_job"]
            job_id_list = job_id_list + unhealth_jobs["flink_streaming_code_job"]
            job_id_list = job_id_list + unhealth_jobs["spark_structured_streaming_code_job"]
        res_list = []
        func_info = []
        for job_id in job_id_list:
            func_info.append([JobHandler(job_id).recover, {}])

        for segment in [func_info[i : i + concurrency] for i in range(0, len(func_info), concurrency)]:
            threads_res = concurrent_call_func(segment)
            for segment_index, res_info in enumerate(threads_res):
                if res_info:
                    res_list.append(res_info)
        return res_list
