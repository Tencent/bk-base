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
import math

from common.local import get_request_username
from django.utils.translation import ugettext as _

from dataflow import pizza_settings
from dataflow.pizza_settings import (
    FLINK_ADVANCED_TASK_MANAGER_MEMORY,
    FLINK_CODE_VERSION,
    FLINK_DEFAULT_YARN_CLUSTER_SLOTS,
    FLINK_STANDARD_TASK_MANAGER_MEMORY,
    FLINK_UDF_YARN_CLUSTER_SLOTS,
    FLINK_VERSION,
    STREAM_YARN_CLUSTER_DEFAULT_RESOURCE,
)
from dataflow.shared.component.component_helper import ComponentHelper
from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.stream.utils.resource_util import (
    get_flink_yarn_cluster_queue_name,
    get_flink_yarn_session_name,
    get_spark_yarn_cluster_queue_name,
)
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.api.api_helper import DatabusApiHelper
from dataflow.stream.exceptions.comp_execptions import CancelFlinkJobError, JobNaviError, SubmitFlinkJobError
from dataflow.stream.handlers import processing_stream_job
from dataflow.stream.settings import (
    DEBUG_CLUSTER_VERSIONS,
    RESOURCES_ALLOCATE_MEMORY_DIVIDE_SLOT_NUM,
    SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER,
    SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG,
    DeployMode,
    ImplementType,
)


class Adaptor(object):
    def __init__(self, job):
        self.job = job
        # 任务动态配置，基于静态表和其它策略生成
        # 数据源总 partition 数
        self._source_partitions = 0
        self._deploy_mode = None

    @property
    def geog_area_code(self):
        return self.job.geog_area_code

    @property
    def cluster_id(self):
        return self.job.cluster_id

    def get_submit_yarn_cluster_name(self):
        raise NotImplementedError

    @property
    def source_partitions(self):
        # 将source中partition大那个并行度，解决大多数情况下partition堆积的问题
        _partitions = 0
        if not self._source_partitions:
            for head in self.job.heads:
                _partitions = max(
                    DatabusApiHelper.get_partition_for_result_table(head), _partitions
                )  # 总线获取partition的接口
            self._source_partitions = _partitions
        return self._source_partitions

    @property
    def parallelism(self):
        # 显式指定了 concurrency 但实际不合理，小于topic partition，依然使用topic parition
        # 未显式指定 concurrency则使用topic partition
        if self.job.concurrency:
            return max(self.job.concurrency, int(self.source_partitions))
        else:
            return int(self.source_partitions)

    @property
    def deploy_mode(self):
        raise NotImplementedError

    @property
    def cluster_name(self):
        raise NotImplementedError

    @property
    def type_id(self):
        raise NotImplementedError

    @property
    def job_type(self):
        raise NotImplementedError

    @property
    def node_label(self):
        """
        运行在jobnavi 节点的标签信息

        :return: 标签信息
        """
        raise NotImplementedError

    def build_common_extra_info(self):
        extra_info = {}
        if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
            extra_info["license"] = str(
                json.dumps(
                    {
                        "license_server_url": pizza_settings.LICENSE_SERVER_URL,
                        "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
                    }
                )
            )
        return extra_info

    def build_yarn_cluster_extra_info(self, conf):
        """
        yarn-cluster 模式，构造适配层的参数
        @param conf: 调用 job register 后返回的参数
        @return:
        """
        return self.build_common_extra_info()

    def build_yarn_cluster_params(self, conf):
        """
        构造创建 yarn-cluster 任务参数
        @param conf: 调用 job register 后返回的参数
        @return:
        """
        extra_info = self.build_yarn_cluster_extra_info(conf)
        register_params = {
            "schedule_id": self.job.job_id,
            "type_id": self.type_id,
            "description": self.job_type,
            "extra_info": str(json.dumps(extra_info)),
            "exec_oncreate": True,
            "recovery": {"enable": True},
            "max_running_task": 1,
        }
        # 增加jobnavi runner标签信息
        if self.node_label:
            register_params["node_label"] = self.node_label
        return register_params

    def submit_yarn_cluster_job(self, register_params):
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.job.cluster_id)
        try:
            # 假如schedule存在，则判定任务是running的
            operate_info = {"operate_info": json.dumps({"operate": "start"})}
            schedule_exists = jobnavi_stream_helper.get_schedule(self.job.job_id)
            if schedule_exists:
                logger.info("[submit_flink_job] the schedule id exists, return job is running.")
                execute_id = jobnavi_stream_helper.get_execute_id(self.job.job_id)
                operate_info["operate_info"] = json.dumps({"execute_id": execute_id, "operate": "start"})
                return operate_info
            # 注册 schedule
            execute_id = jobnavi_stream_helper.add_yarn_schedule(register_params)
            # 查看 jobnavi 对应的 execute_id 是否执行 即任务是否执行成功
            jobnavi_stream_helper.get_execute_status(execute_id)
            # 创建 schedule 为异步的过程，可能由于 JAR 包逻辑错误或资源不足，在提交片刻后变为 failed
            # 需要在同步阶段通过 execute_id 进行再次确认
            # 否则同步阶段无法事先区分任务是处于已经失败还是正在启动的过程，而陷入等待同步的过程
            operate_info["operate_info"] = json.dumps({"execute_id": execute_id, "operate": "start"})
            return operate_info
        except Exception as e:
            # 启动异常时，删除schedule id
            jobnavi_stream_helper.del_schedule(self.job.job_id)
            raise e

    def sync_job(self, execute_id):
        """
        基于 sync_job 事件调用 jobnavi 适配层同步 job 状态
        @param execute_id:
        @return:
        """
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        # 调用获取job状态的接口
        event_id = jobnavi_stream_helper.request_jobs_status(execute_id)
        # 根据event id获取任务状态
        jobs_status_list = jobnavi_stream_helper.list_jobs_status(event_id)
        if self.job.job_id in jobs_status_list:
            # todo [new]  jobs_status_list job status contain running  restarting...
            job_status = {self.job.job_id: "ACTIVE"}
        else:
            job_status = {self.job.job_id: None}
        logger.info("[sync job status] the job status is %s" % str(job_status))
        return job_status

    def sync_yarn_cluster_status(self, operate_info):
        """
        同步 yarn-cluster 任务状态
        @param operate_info:
        @return:
        """
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        if operate_info["operate"] == "start":
            # 启动时同步状态包括：
            #   1. 确保 jobnavi execute 为 running
            #   2. 通过 application_status 事件拉取状态信息
            #   3. 若 2 有返回信息，通过 sync_job 事件拉取 execute 任务状态信息
            #   4. 启动失败删除 schedule 信息
            try:
                execute_id = operate_info["execute_id"]
                # 轮询，确保提交的 yarn-cluster 任务 execute 状态为 running 或 skipped
                jobnavi_stream_helper.get_execute_status(execute_id)
                # 查看yarn cluster application是否部署成功
                result = jobnavi_stream_helper.get_cluster_app_deployed_status(execute_id)
                if not result:
                    return {self.job.job_id: None}
                # get status by sync_job event
                job_status = self.sync_job(execute_id)
                if "ACTIVE" == job_status[self.job.job_id]:
                    processing_stream_job.update(
                        self.job.job_id,
                        status="running",
                        updated_by=get_request_username(),
                    )
                return job_status
            except Exception as e:
                # 启动异常删除schedule id
                logger.exception(e)
                jobnavi_stream_helper.del_schedule(self.job.job_id)
                raise e
        elif operate_info["operate"] == "stop":
            # 停止时同步状态包括：
            #   1. 确保 jobnavi schedule 存在，否则认为停止成功
            #   2. 通过 cancel 时 event_id 拉取状态信息，若处理了并且处理成功，表示任务停止成功
            #   3. 停止成功删除 schedule 信息
            # 当 schedule_id 不存在时 则任务不存在
            cluster_is_exist = jobnavi_stream_helper.get_schedule(self.job.job_id)
            if not cluster_is_exist:
                processing_stream_job.update(self.job.job_id, status="stopped", updated_by=get_request_username())
                return {self.job.job_id: None}
            result = jobnavi_stream_helper.get_cancel_job_result_for_cluster(operate_info["event_id"])
            # 停止任务结果没有返回之前，任务是启动状态
            if not result:
                return {self.job.job_id: "ACTIVE"}
            # delete schedule id
            jobnavi_stream_helper.del_schedule(self.job.job_id)
            logger.info("[cancel_sync_status] delete schedule id %s" % self.job.job_id)
            # 更改动态作业表中的运行状态
            processing_stream_job.update(self.job.job_id, status="stopped", updated_by=get_request_username())
            return {self.job.job_id: None}

    def cancel_yarn_cluster_job(self):
        operate_info = {"operate_info": json.dumps({})}
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        # check if the schedule id exists, and if it is not, return true.
        cluster_is_exist = jobnavi_stream_helper.get_schedule(self.job.job_id)
        # 当schedule id不存在时 则任务不存在
        if not cluster_is_exist:
            operate_info["operate_info"] = json.dumps({"operate": "stop"})
            return operate_info
        # 获取execute id，如果获取不到，则视为异常情况
        execute_id = jobnavi_stream_helper.get_execute_id(self.job.job_id)
        if not execute_id:
            raise CancelFlinkJobError()
        # 正常停止流程，不使用flink的savepoint
        event_id = jobnavi_stream_helper.send_event_for_kill_yarn_app(execute_id)
        operate_info["operate_info"] = json.dumps({"event_id": event_id, "operate": "stop"})
        return operate_info

    def sync_status(self, operate_info):
        if not self.job.is_debug:
            # 非 debug 状态下，若无作业动态配置直接返回
            stream_job = processing_stream_job.get(self.job.job_id, raise_exception=False)
            if not stream_job:
                return {self.job.job_id: None}
            cluster_name = stream_job.cluster_name
            deploy_mode = stream_job.deploy_mode
        else:
            deploy_mode = DeployMode.YARN_SESSION.value
            cluster_name = DEBUG_CLUSTER_VERSIONS.get(self.job.implement_type)
        return self.sync_inner_status(operate_info, deploy_mode, cluster_name)

    def cancel(self):
        if not self.job.is_debug:
            # 非 debug 状态下，若无作业动态配置直接返回
            operate_info = {"operate_info": json.dumps({"operate": "stop"})}
            stream_job = processing_stream_job.get(self.job.job_id, raise_exception=False)
            if not stream_job:
                return operate_info
            deploy_mode = stream_job.deploy_mode
            if not deploy_mode:
                # 首次启动并且注册任务后失败，该值为None
                return operate_info
            cluster_name = stream_job.cluster_name
        else:
            deploy_mode = DeployMode.YARN_SESSION.value
            cluster_name = DEBUG_CLUSTER_VERSIONS.get(self.job.implement_type)
        return self.cancel_inner_job(deploy_mode, cluster_name)

    def submit(self, conf):
        raise NotImplementedError

    def sync_inner_status(self, operate_info, deploy_mode, cluster_name):
        raise NotImplementedError

    def cancel_inner_job(self, deploy_mode, cluster_name):
        raise NotImplementedError

    def check_job_before_submit(self, conf):
        pass

    @property
    def task_manager_memory(self):
        return STREAM_YARN_CLUSTER_DEFAULT_RESOURCE

    def force_kill(self, timeout):
        stream_job = processing_stream_job.get(self.job.job_id, raise_exception=False)
        if not stream_job or stream_job.deploy_mode != DeployMode.YARN_CLUSTER.value:
            return True
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        execute_id = None
        result = False
        try:
            execute_id = jobnavi_stream_helper.get_execute_id(self.job.job_id, timeout)
        except Exception:
            # 当 execute id 不存在，不抛出异常
            result = True
        # delete schedule id in jobnavi
        # kill yarn application
        if execute_id:
            result = jobnavi_stream_helper.kill_yarn_application(execute_id, timeout)
        # 正常强制删除任务后 删除schedule id
        # 删除 schedule 有两种情况
        #   1. schedule 对应的 execute_id 不存在
        #   2. execute_id 存在并且 kill_application 成功
        if result:
            jobnavi_stream_helper.del_schedule(self.job.job_id)
        return result


class SparkStructuredStreamingAdaptor(Adaptor):
    @property
    def type_id(self):
        return "sparkstreaming"

    @property
    def deploy_mode(self):
        return DeployMode.YARN_CLUSTER.value

    def get_submit_yarn_cluster_name(self):
        """
        获取SparkStructuredStreaming任务即将提交的集群
        @return: 队列
        """
        ###
        geog_area_code = self.job.geog_area_code
        resource_group_id = self.job.cluster_group
        return get_spark_yarn_cluster_queue_name(geog_area_code, resource_group_id)

    @property
    def cluster_name(self):
        return self.get_submit_yarn_cluster_name()

    @property
    def job_type(self):
        return "spark_structured_streaming"

    @property
    def node_label(self):
        return None

    def build_yarn_cluster_extra_info(self, conf):
        extra_info = super(SparkStructuredStreamingAdaptor, self).build_yarn_cluster_extra_info(conf)
        extra_info.update(
            {
                "debug": self.job.is_debug,
                "name": conf["submit_args"]["job_id"],
                "queue": conf["submit_args"]["cluster_name"],
                "resource_group_id": self.job.cluster_group,
                "geog_area_code": self.job.geog_area_code,
                "service_type": "stream",
                "component_type": "spark",
                "pyFiles": conf["submit_args"]["jar_files"],
                "primaryResource": "spark_streaming_main.py",
                "type": "pyspark",
                "numExecutors": conf["submit_args"]["parallelism"],
                "executorMemory": conf["submit_args"]["task_manager_memory"],
                "mainArgs": str(json.dumps(conf["job_args"])),
            }
        )
        return extra_info

    def submit(self, conf, is_recover=False):
        register_params = self.build_yarn_cluster_params(conf)
        return self.submit_yarn_cluster_job(register_params)

    def check_job_before_submit(self, conf):
        # structured streaming 时，选择从最新/最早消费数据或者不启动 savepoint 时，就需要清理savepoint
        if self.job.offset != 1 or not conf["submit_args"]["use_savepoint"]:
            # spark structure streaming checkpoint 使用低负载的hdfs集群
            checkpoint_custer_tag = SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG
            checkpoint_ftp_server = self.job.ftp_server(tag=checkpoint_custer_tag)
            checkpoint_cluster_group = checkpoint_ftp_server.cluster_group
            savepoint_hdfs_path = "/app/{}/checkpoints/{}".format(
                self.job_type,
                self.job.job_id,
            )
            logger.info(
                "Clean structured streaming savepoint, hdfs [%s] path is %s"
                % (checkpoint_cluster_group, savepoint_hdfs_path)
            )
            hdfs_proxy_user = SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER
            ComponentHelper.hdfs_clean(checkpoint_cluster_group, [savepoint_hdfs_path], True, hdfs_proxy_user)

    def sync_inner_status(self, operate_info, deploy_mode, cluster_name):
        return self.sync_yarn_cluster_status(operate_info)

    def cancel_inner_job(self, deploy_mode, cluster_name):
        return self.cancel_yarn_cluster_job()

    @property
    def task_manager_memory(self):
        deploy_config = self.job.deploy_config or {}
        if deploy_config.get("resource") and deploy_config.get("resource").get("task_manager_memory"):
            task_manager_memory = "%sM" % deploy_config["resource"]["task_manager_memory"]
        else:
            task_manager_memory = "%sM" % STREAM_YARN_CLUSTER_DEFAULT_RESOURCE
        return task_manager_memory


class FlinkAdaptor(Adaptor):
    @property
    def type_id(self):
        if self.job.implement_type == ImplementType.CODE.value:
            return "stream-code-%s" % self.job.component_type
        else:
            return "stream"

    @property
    def version(self):
        if self.job.implement_type == ImplementType.CODE.value:
            return FLINK_CODE_VERSION
        else:
            return FLINK_VERSION

    @property
    def deploy_mode(self):
        """
        若 DB 未配置 deploy_mode, 大于5个(原先为20个) partition 时运行为 yarn-cluster 模式，否则为 yarn-session 模式
        @return:
        """
        deploy_mode = self.job.deploy_mode
        if deploy_mode not in [item.value for item in DeployMode]:
            if self.parallelism > 5:
                deploy_mode = DeployMode.YARN_CLUSTER.value
            else:
                deploy_mode = DeployMode.YARN_SESSION.value
        return deploy_mode

    def get_submit_yarn_cluster_name(self):
        """
        获取Flink任务即将提交的集群
        @return: 队列
        """
        ###
        geog_area_code = self.job.geog_area_code
        resource_group_id = self.job.cluster_group
        return get_flink_yarn_cluster_queue_name(geog_area_code, resource_group_id)

    @property
    def cluster_name(self):
        """
        flink任务
        对于 yarn-cluster，不再拼接队列，而是从资源系统获得队列名称作为 cluster_name
        对于 yarn-session，根据数据源的 partition 分配任务的运行方式
        1.小于等于5个partition 运行在yarn session上
        取消原先 20 > partition > 5 的advanced分配策略，yarn session只保留standard一种类型集群
        @return:
        """
        # yarn-cluster 模式不可指定 cluster_name
        if self.deploy_mode == DeployMode.YARN_CLUSTER.value:
            cluster_name = self.get_submit_yarn_cluster_name()
        else:
            # yarn session
            # 除非手动对任务在job_info指定cluster_name细分的集群，否则这里一般都为NULL需要动态从资源系统拿优先级高的session集群
            cluster_name = self.job.cluster_name
            if not cluster_name or cluster_name == "":
                geog_area_code = self.job.geog_area_code
                resource_group_id = self.job.cluster_group
                cluster_name = get_flink_yarn_session_name(geog_area_code, resource_group_id)
        return cluster_name

    @property
    def job_type(self):
        return "flink"

    @property
    def node_label(self):
        return getattr(pizza_settings, "STREAM_JOB_NODE_LABEL", None)

    def build_yarn_cluster_extra_info(self, conf):
        extra_info = super(FlinkAdaptor, self).build_yarn_cluster_extra_info(conf)
        parallelism = conf["submit_args"]["parallelism"]
        slot_num_per_task = self.get_yarn_cluster_slot_num_per_task(parallelism)
        if self.job.implement_type in [ImplementType.CODE.value]:
            extra_info.update(
                {
                    "yarn_queue": conf["submit_args"]["cluster_name"],
                    "resource_group_id": self.job.cluster_group,
                    "geog_area_code": self.job.geog_area_code,
                    "service_type": "stream",
                    "component_type": "flink",
                    "job_manager_memory": 1024,
                    "task_manager_memory": conf["submit_args"]["task_manager_memory"],
                    "slot": slot_num_per_task,
                    "parallelism": parallelism,
                    "mode": self.deploy_mode,
                    "jar_file_name": conf["submit_args"]["jar_files"],
                    "use_savepoint": conf["submit_args"]["use_savepoint"],
                    "argument": str(json.dumps(conf["job_args"])),
                    "code": conf["submit_args"]["code"],
                    "user_main_class": conf["submit_args"]["user_main_class"],
                }
            )
        else:
            extra_info.update(
                {
                    "yarn_queue": conf["submit_args"]["cluster_name"],
                    "resource_group_id": self.job.cluster_group,
                    "geog_area_code": self.job.geog_area_code,
                    "service_type": "stream",
                    "component_type": "flink",
                    "container": 1,
                    "job_manager_memory": 1024,
                    "task_manager_memory": conf["submit_args"]["task_manager_memory"],
                    "slot": slot_num_per_task,
                    "parallelism": parallelism,
                    "mode": DeployMode.YARN_CLUSTER.value,
                    "jar_file_name": conf["submit_args"]["jar_files"],
                    "use_savepoint": False,  # 正常启动任务，不使用flink的savepoint
                    "argument": str(json.dumps(conf["job_args"])),
                }
            )
        return extra_info

    def build_yarn_session_extra_info(self, conf):
        extra_info = self.build_common_extra_info()
        cluster_name = conf["submit_args"]["cluster_name"]
        # 只保留standard集群，debug任务内存适当多些
        if cluster_name in list(DEBUG_CLUSTER_VERSIONS.values()):
            task_manager_memory = FLINK_ADVANCED_TASK_MANAGER_MEMORY
        else:
            task_manager_memory = FLINK_STANDARD_TASK_MANAGER_MEMORY
        extra_info.update(
            {
                "job_name": self.job.job_id,
                # 目前该参数用于 jobnavi 校验 yarn-session 任务所用资源
                "task_manager_memory": task_manager_memory,
                "parallelism": conf["submit_args"]["parallelism"],
                "jar_file_name": conf["submit_args"]["jar_files"],
                "argument": str(json.dumps(conf["job_args"])),
            }
        )
        if self.job.implement_type in [ImplementType.CODE.value]:
            extra_info.update(
                {
                    "code": conf["submit_args"]["code"],
                    "user_main_class": conf["submit_args"]["user_main_class"],
                }
            )
        return extra_info

    def sync_yarn_session_job_status(self, operate_info, schedule_id, is_recover=False):
        """
        同步状态包括:
            1. 确保 jobnavi schedule 存在
            2. 获取 schedule running 状态的 execute
            3. 通过 sync_job 事件拉取 execute 任务状态信息
        @param operate_info:
        @param schedule_id:
        @param is_recover: 恢复任务调用不更新DB状态,默认False
        @return:
        """
        # yarn-session 模式 cluster_name 即 schedule_id
        cluster_name = schedule_id
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        # 获取cluster_name对应的execute id(对应yarn session)；cluster_name对应jobnavi中的schedule id
        cluster_is_exist = jobnavi_stream_helper.get_schedule(cluster_name)
        # 当schedule id不存在时 则任务不存在
        if not cluster_is_exist:
            raise CancelFlinkJobError() if operate_info.get("operate") == "stop" else SubmitFlinkJobError()
        # 获取execute id，如果获取不到，则视为异常情况
        execute_id = jobnavi_stream_helper.get_execute_id(cluster_name)
        logger.info("[sync flink yarn-session job]: cluster_name({}), execute_id({})".format(cluster_name, execute_id))
        if not execute_id:
            raise CancelFlinkJobError() if operate_info.get("operate") == "stop" else SubmitFlinkJobError()
        # get status by sync_job event
        job_status = self.sync_job(execute_id)
        # 更新运行态作业记录
        # recover不需要更改任务状态
        if not self.job.is_debug and not is_recover:
            if "ACTIVE" == job_status[self.job.job_id]:
                processing_stream_job.update(self.job.job_id, status="running", updated_by=get_request_username())
            else:
                processing_stream_job.update(self.job.job_id, status="stopped", updated_by=get_request_username())
        return execute_id, job_status

    def cancel_yarn_session_job(self, schedule_id):
        operate_info = {"operate": "stop"}
        # 同步状态，如果任务已经在停止，则返回true
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.cluster_id)
        execute_id, job_status = self.sync_yarn_session_job_status(operate_info, schedule_id)
        if "ACTIVE" != job_status.get(self.job.job_id):
            # 如果是 yarn-cluster 模式 则 kill execute_id
            execute_ids = jobnavi_stream_helper.get_one_yarn_cluster_all_execute_id(self.job.job_id)
            for execute_id in execute_ids:
                jobnavi_stream_helper.kill_execute_id(execute_id)

            return {"operate_info": json.dumps(operate_info)}
        # savepoint 是为下一次启动考虑的，停止必须指定 savepoint
        # 对于更高版本的Flink(1.9.1)，需要调用 stop_job
        event_name = "cancel_job"
        use_savepoint = False
        if self.type_id != "stream":
            event_name = "stop_job"
            use_savepoint = True
        event_id = jobnavi_stream_helper.cancel_flink_job(
            execute_id,
            self.job.job_id,
            use_savepoint=use_savepoint,
            event_name=event_name,
        )
        # 根据 event_id 获取任务是否停止成功
        result = jobnavi_stream_helper.get_run_cancel_job_result(event_id)
        if result:
            return {"operate_info": json.dumps(operate_info)}
        else:
            raise JobNaviError(_("停止作业失败"))

    def build_yarn_session_params(self, execute_id, conf):
        """
        构造创建 yarn-session 任务参数
        @param execute_id: yarn-session 集群的 execute_id
        @return:
        """
        event_info = self.build_yarn_session_extra_info(conf)
        register_params = {
            "event_name": "run_job",
            "execute_id": execute_id,
            "event_info": str(json.dumps(event_info)),
        }
        return register_params

    def submit_yarn_session_job(self, register_params):
        operate_info = {"operate_info": json.dumps({"operate": "start"})}
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(self.geog_area_code, self.job.cluster_id)
        try:
            execute_id = register_params["execute_id"]
            event_name = register_params["event_name"]
            event_info = register_params.get("event_info", None)
            event_id = jobnavi_stream_helper.send_event(execute_id, event_name, event_info, timeout_auto_retry=True)
            logger.info(
                "[run_flink_job] execute id {} and result {}".format(register_params.get("execute_id"), event_id)
            )
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("提交作业失败"))
        # 根据 event_id 获取任务是否启动成功
        submit_result = jobnavi_stream_helper.get_run_cancel_job_result(event_id)
        if submit_result:
            return operate_info
        else:
            raise JobNaviError(_("任务提交失败，请查看日志"))

    def submit(self, conf, is_recover=False):
        deploy_mode = conf["submit_args"]["deploy_mode"]
        if not self.job.is_debug and deploy_mode == DeployMode.YARN_CLUSTER.value:
            register_params = self.build_yarn_cluster_params(conf)
            return self.submit_yarn_cluster_job(register_params)
        else:
            # 若任务已运行，直接返回成功
            operate_info = {"operate": "start"}
            execute_id, job_status = self.sync_yarn_session_job_status(
                operate_info, conf["submit_args"]["cluster_name"], is_recover
            )
            if "ACTIVE" == job_status.get(self.job.job_id):
                return {"operate_info": json.dumps(operate_info)}
            # 否则提交任务
            register_params = self.build_yarn_session_params(execute_id, conf)
            return self.submit_yarn_session_job(register_params)

    def sync_inner_status(self, operate_info, deploy_mode, cluster_name):
        if deploy_mode == DeployMode.YARN_CLUSTER.value:
            return self.sync_yarn_cluster_status(operate_info)
        else:
            # 同步 yarn-session 任务需要通过作业动态表获取 deploy_mode, cluster_name
            execute_id, status = self.sync_yarn_session_job_status(operate_info, cluster_name)
            return status

    def cancel_inner_job(self, deploy_mode, cluster_name):
        if deploy_mode == DeployMode.YARN_CLUSTER.value:
            return self.cancel_yarn_cluster_job()
        else:
            return self.cancel_yarn_session_job(cluster_name)

    @property
    def task_manager_memory(self):
        return self.get_tm_memory_by_partition()

    def get_tm_memory_by_partition(self, partition=None):
        """
        获取 flink taskmanager memory
        1. 当 job 表字段 deploy_config 中有 task_manager_memory 配置时，以这个配置为准
        2. 当 job 没有配置 task_manager_memory 时，采用默认策略
            a. 当每个tm包含slot数大于等于5时，tm的memory分配5G
            b. 当每个tm包含slot数不大于5时， tm的memory为 1G × slot数
        """
        deploy_config = self.job.deploy_config or {}
        if deploy_config.get("resource") and deploy_config.get("resource").get("task_manager_memory"):
            task_manager_memory = deploy_config["resource"]["task_manager_memory"]
        else:
            slot_memory = STREAM_YARN_CLUSTER_DEFAULT_RESOURCE
            slot_num = self.get_yarn_cluster_slot_num_per_task(parallelism=partition)
            if slot_num >= RESOURCES_ALLOCATE_MEMORY_DIVIDE_SLOT_NUM:
                task_manager_memory = slot_memory * RESOURCES_ALLOCATE_MEMORY_DIVIDE_SLOT_NUM
            else:
                task_manager_memory = slot_num * slot_memory
        return task_manager_memory

    def get_yarn_session_total_tm_mem(self, partition=None):
        """
        获取 yarn-session 模式下任务的task_manager内存之和
        @param partition: 可选参数，基于kafka topic扩分区后新的分区数
        @return: 任务task_manager内存之和
        """
        if not partition:
            partition = self.parallelism
        if partition <= 5:
            total_task_manager_memory = FLINK_STANDARD_TASK_MANAGER_MEMORY * partition
        else:
            total_task_manager_memory = FLINK_ADVANCED_TASK_MANAGER_MEMORY * partition
        return total_task_manager_memory

    def get_yarn_cluster_total_tm_mem(self, partition=None):
        """
        获取 yarn-cluster 模式下任务的task_manager内存之和
        @param partition: 可选参数，基于kafka topic扩分区后新的分区数
        @return: 任务task_manager内存之和
        """
        single_task_manager_memory = self.get_tm_memory_by_partition(partition=partition)
        single_task_manager_slot_num = self.get_yarn_cluster_slot_num_per_task(parallelism=partition)
        if not partition:
            partition = self.parallelism
        total_task_manager_num = int(math.ceil(partition / float(single_task_manager_slot_num)))
        total_task_manager_memory = single_task_manager_memory * total_task_manager_num
        return total_task_manager_memory

    def get_yarn_cluster_slot_num_per_task(self, parallelism=None):
        """
        获取 yarn-cluster 模式下一个 task_manager 的 slot 数目
        @param parallelism: 可选参数，基于指定并行度读获取一个 task_manager 的 slot 数目
        @return:
        """
        # 获取并行度可能需要获取 kafka 的分区数
        if not parallelism:
            parallelism = self.parallelism
        # 优先使用配置表中的配置。如没有配置并且 flink 任务中包含 udf，为避免GIL对任务的性能影响, 则一个taskmanager分配1个slot
        if self.job.slots:
            max_slot_per_taskmanager = self.job.slots
        elif self.job.implement_type != ImplementType.CODE.value and processing_udf_info.exists(
            processing_id__in=self.job.real_processing_ids
        ):
            max_slot_per_taskmanager = FLINK_UDF_YARN_CLUSTER_SLOTS
        else:
            max_slot_per_taskmanager = FLINK_DEFAULT_YARN_CLUSTER_SLOTS
        taskmanager_num = math.ceil(parallelism / float(max_slot_per_taskmanager))
        slot_num = int(math.ceil(parallelism / float(taskmanager_num)))
        return slot_num
