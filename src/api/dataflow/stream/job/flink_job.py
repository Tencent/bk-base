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

from conf.dataapi_settings import API_URL, SYSTEM_ADMINISTRATORS
from django.db import transaction
from django.db.models import Max
from django.utils.translation import ugettext as _

from dataflow.pizza_settings import FLINK_STATE_CHECKPOINT_DIR
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.handlers import processing_udf_info, processing_udf_job, processing_version_config
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.send_message import send_message
from dataflow.stream.exceptions.comp_execptions import CheckpointInfoError, CodeVersionNotFoundError
from dataflow.stream.handlers import processing_stream_info, processing_stream_job
from dataflow.stream.job.adaptors import FlinkAdaptor
from dataflow.stream.job.entity.stream_conf import StreamConf
from dataflow.stream.job.job import Job
from dataflow.stream.result_table.result_table_for_flink import load_flink_chain
from dataflow.stream.settings import (
    METRIC_KAFKA_TOPIC,
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    UC_TIME_ZONE,
)
from dataflow.stream.utils.checkpoint_manager import CheckpointManager


class FlinkJob(Job):
    def get_code_version(self):
        code_version = super(FlinkJob, self).get_code_version()

        if not code_version or code_version == "default":
            code_version = "master"
        else:
            rows = list(
                processing_version_config.where(component_type=self.component_type)
                .exclude(branch__icontains="code")
                .values()
            )
            for row in rows:
                tag = row["branch"] + "-" + row["version"]
                if tag == code_version:
                    return code_version + ".jar"
        version = processing_version_config.where(component_type=self.component_type, branch=code_version).aggregate(
            Max("version")
        )["version__max"]
        if not version:
            logger.exception("获取版本任务失败: {}-{}".format(self.component_type, code_version))
            raise CodeVersionNotFoundError()
        return code_version + "-" + version + ".jar"

    def _check_alter_checkpoint_info(self):
        """
        检查是否修改了 redis 配置，若修改了，发送告警信息
        @return:
        """
        last_job_info = processing_stream_job.get(self.job_id, raise_exception=False)
        if last_job_info:
            # 兼容 None 值的情况
            if last_job_info.deploy_config:
                deploy_config = json.loads(last_job_info.deploy_config)
                checkpoint_host = deploy_config.get("checkpoint", {}).get("checkpoint_host")
                checkpoint_port = deploy_config.get("checkpoint", {}).get("checkpoint_port")
                # 旧任务/旧版本没有这个配置
                if checkpoint_host and checkpoint_port:
                    checkpoint_manager = CheckpointManager(self.geog_area_code)
                    if (
                        checkpoint_manager.checkpoint_host != checkpoint_host
                        or checkpoint_manager.checkpoint_port != checkpoint_port
                    ):
                        content = "<br>".join(
                            [
                                "任务ID：%s" % self.job_id,
                                "旧配置：{}:{}".format(checkpoint_host, checkpoint_port),
                                "新配置：%s:%s"
                                % (
                                    checkpoint_manager.checkpoint_host,
                                    checkpoint_manager.checkpoint_port,
                                ),
                            ]
                        )
                        logger.exception("checkpoint 配置发生变更: %s" % content)
                        send_message(
                            SYSTEM_ADMINISTRATORS,
                            "《Checkpoint 配置信息发生变更》",
                            content,
                            raise_exception=False,
                        )
                        raise CheckpointInfoError(_("实时计算配置信息异常，请联系管理员"))

    @transaction.atomic()
    def _generate_udf_info(self, processing_ids, with_update):
        function_names = []
        udf_infos = []
        if with_update:
            processing_udf_job.delete(job_id=self.job_id)
        for processing_id in processing_ids:
            function_infos = processing_udf_info.where(processing_id=processing_id)
            for function in function_infos:
                if function.udf_name not in function_names:
                    udf_info = json.loads(function.udf_info)
                    udf_infos.append(udf_info)
                if with_update:
                    processing_udf_job.save(
                        job_id=self.job_id,
                        processing_id=processing_id,
                        processing_type="stream",
                        udf_name=function.udf_name,
                        udf_info=function.udf_info,
                    )
                function_names.append(function.udf_name)
        return {"source_data": [], "config": udf_infos}

    def _update_flink_stream_job(self, running_version, processing_ids, checkpoint_manager, cluster_name):
        # 更新 job 信息前，检查 checkpoint 信息是否发生了修改
        if self.implement_type == "sql":
            self._check_alter_checkpoint_info()
        deploy_config = self.deploy_config
        checkpoint_info = deploy_config.setdefault("checkpoint", {})
        checkpoint_info.update(
            {
                "checkpoint_host": checkpoint_manager.checkpoint_host,
                "checkpoint_port": checkpoint_manager.checkpoint_port,
            }
        )
        processing_stream_job.save(
            stream_id=self.job_id,
            running_version=running_version,
            component_type="flink",
            cluster_group=self.cluster_group,
            cluster_name=cluster_name,
            heads=",".join(self.heads),
            tails=",".join(self.tails),
            status="preparing",
            concurrency=self.adaptor.parallelism,
            deploy_mode=self.adaptor.deploy_mode,
            deploy_config=json.dumps(deploy_config),
            offset=self.offset,
            implement_type=self.implement_type,
            programming_language=self.programming_language,
            created_by=self.job_info.created_by,
            updated_by=self.job_info.updated_by,
        )
        udf_infos = self._generate_udf_info(processing_ids, True)
        jobnavi_info = {
            "udf_path": list(
                map(
                    lambda udf_info_config: udf_info_config["hdfs_path"],
                    udf_infos["config"],
                )
            )
        }
        return jobnavi_info

    def _get_stream_conf(self, params=None):
        # 更新配置时先通过参数更新，后通过job_info填充
        if params:
            self._stream_conf = (
                StreamConf()
                .update_by_params(params)
                .update_by_deploy_config(self.job_info.deploy_mode, self.job_info.deploy_config)
            )
        # 正常使用时从DB获取
        elif not self._stream_conf:
            self._stream_conf = StreamConf().update_by_deploy_config(
                self.job_info.deploy_mode, self.job_info.deploy_config
            )
        return self._stream_conf

    def _get_state_backend(self):
        """
        获取 state_backend, 若除非设置为 filesystem，否则都为 rocksdb
        @return:
        """
        state_backend = self._get_stream_conf().state_backend
        if state_backend != "filesystem":
            state_backend = "rocksdb"
        return state_backend

    def _get_checkpoint_interval(self):
        default_checkpoint_interval = 600000
        checkpoint_interval = self._get_stream_conf().checkpoint_interval
        if not checkpoint_interval:
            checkpoint_interval = default_checkpoint_interval
        return int(checkpoint_interval)

    def _get_concurrency(self):
        concurrency = self._get_stream_conf().concurrency
        if not concurrency:
            concurrency = self.adaptor.parallelism
        return concurrency

    def _generate_nodes(self):
        tails_str = ",".join(self.tails)
        heads_str = ",".join(self.heads)
        source = {}
        transform = {}
        sink = {}
        result_table_info = load_flink_chain(heads_str, tails_str, self.real_job_id)
        for result_table_id, result_table in list(result_table_info.items()):
            if result_table_id in self.heads:
                # source
                source[result_table_id] = result_table
            else:
                # transform
                transform_result_table = result_table.copy()
                del transform_result_table["output"], transform_result_table["storages"]
                transform[result_table_id] = transform_result_table
                self._put_static_join_transform_conf(transform_result_table)
                # sink
                if result_table["output"] and not self.is_debug:
                    sink_result_table = result_table.copy()
                    del (
                        sink_result_table["storages"],
                        sink_result_table["parents"],
                        sink_result_table["window"],
                        sink_result_table["processor"],
                    )
                    sink[result_table_id] = sink_result_table
        nodes = {"source": source, "transform": transform, "sink": sink}
        return nodes

    def _put_static_join_transform_conf(self, transform_table):
        """
        对于静态关联节点添加并发配置
        @return:
        """
        if transform_table["processor"]["processor_type"] == "static_join_transform" and self.async_io_concurrency:
            processor_args_json = json.loads(transform_table["processor"]["processor_args"])
            processor_args_json["async_io_concurrency"] = self.async_io_concurrency
            transform_table["processor"]["processor_args"] = json.dumps(processor_args_json)

    @property
    def real_processing_ids(self):
        """
        获取作业对应的非虚拟 DP 列表
        PS: 所有的函数信息仅与真实 DP 进行关联
        @return:
        """
        return processing_stream_info.filter(stream_id=self.real_job_id).values_list("processing_id", flat=True)

    def register(self, jar_name, geog_area_code, is_fill_snapshot=False):
        """
        注册作业，调试任务不走该流程
        @param jar_name:
        @param geog_area_code:
        @param is_fill_snapshot: True register的目的是对当前运行中的任务获取并填充提交信息，无副作用，不更新db
        @return:
        """
        checkpoint_manager = CheckpointManager(geog_area_code)
        # params
        deploy_mode = self.adaptor.deploy_mode
        # adaptor的cluster_name动态从资源系统选择合适的cluster or session集群
        cluster_name = self.adaptor.cluster_name
        if is_fill_snapshot:
            udf_infos = self._generate_udf_info(self.real_processing_ids, False)
            jobnavi_info = {
                "udf_path": list(
                    map(
                        lambda udf_info_config: udf_info_config["hdfs_path"],
                        udf_infos["config"],
                    )
                )
            }
        else:
            # 动态 job 表生成
            jobnavi_info = self._update_flink_stream_job(
                jar_name, self.real_processing_ids, checkpoint_manager, cluster_name
            )
        job_args = {
            "job_id": self.job_id,
            "job_type": "flink",
            "run_mode": "product",
            "api_url": {"base_dataflow_url": "http://%s/v3/dataflow/" % API_URL},
            "jobnavi": jobnavi_info,
        }
        submit_args = {
            "cluster_name": cluster_name,
            "parallelism": self.adaptor.parallelism,
            "jar_files": jar_name,
            "task_manager_memory": self.adaptor.task_manager_memory,
            "deploy_mode": deploy_mode,
        }
        submit_config = {"job_args": job_args, "submit_args": submit_args}
        return submit_config

    @property
    def adaptor(self):
        if not self._adaptor:
            self._adaptor = FlinkAdaptor(self)
        return self._adaptor

    def generate_job_config(self):
        geog_area_code = self.geog_area_code
        metric_kafka_server = DatamanageHelper.get_metric_kafka_server(geog_area_code)
        if not self.is_debug:
            checkpoint_manager = CheckpointManager(geog_area_code)
            if checkpoint_manager.enable_sentinel:
                checkpoint_redis_host = None
                checkpoint_redis_port = 0
                checkpoint_redis_password = None
                checkpoint_redis_sentinel_host = checkpoint_manager.host_sentinel
                checkpoint_redis_sentinel_port = checkpoint_manager.port_sentinel
                checkpoint_redis_sentinel_name = checkpoint_manager.name_sentinel
            else:
                checkpoint_redis_host = checkpoint_manager.host
                checkpoint_redis_port = checkpoint_manager.port
                checkpoint_redis_password = checkpoint_manager.password
                checkpoint_redis_sentinel_host = None
                checkpoint_redis_sentinel_port = 0
                checkpoint_redis_sentinel_name = None
            run_mode = "product"
            manager = "redis"
            debug_id = None
            start_position = "from_tail" if self.offset == 0 else "continue"
        else:
            checkpoint_redis_host = None
            checkpoint_redis_port = 0
            checkpoint_redis_password = None
            checkpoint_redis_sentinel_host = None
            checkpoint_redis_sentinel_port = 0
            checkpoint_redis_sentinel_name = None
            run_mode = "debug"
            manager = "dummy"
            debug_id = self.job_id.split("__")[0]
            start_position = "from_head"
        nodes = self._generate_nodes()
        job_config = {
            "job_id": self.job_id,
            "job_name": self.job_id,
            "job_type": "flink",
            "run_mode": run_mode,
            "time_zone": UC_TIME_ZONE,
            "chain": self.chain,
            "checkpoint": {
                "start_position": start_position,
                "manager": manager,
                "checkpoint_redis_host": checkpoint_redis_host,
                "checkpoint_redis_port": checkpoint_redis_port,
                "checkpoint_redis_password": checkpoint_redis_password,
                "checkpoint_redis_sentinel_host": checkpoint_redis_sentinel_host,
                "checkpoint_redis_sentinel_port": checkpoint_redis_sentinel_port,
                "checkpoint_redis_sentinel_name": checkpoint_redis_sentinel_name,
                "state_backend": self._get_state_backend(),
                "checkpoint_interval": self._get_checkpoint_interval(),
                "state_checkpoints_dir": FLINK_STATE_CHECKPOINT_DIR,
            },
            "metric": {
                "metric_kafka_server": metric_kafka_server,
                "metric_kafka_topic": METRIC_KAFKA_TOPIC,
            },
            "debug": {
                "debug_id": debug_id,
                "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
                "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
                "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
            },
            "nodes": nodes,
            "udf": self._generate_udf_info(self.real_processing_ids, False),
        }
        return job_config

    def transform_checkpoint(self):
        raise NotImplementedError

    def transform_config(self):
        return r"当前任务已经是flink任务！"

    def update_stream_conf(self, params):
        """
        更新Flink相关运行配置信息
        :return:
        """
        stream_conf = self._get_stream_conf(params)
        self._update_deploy_conf(
            deploy_mode=stream_conf.deploy_mode,
            deploy_config=json.dumps(stream_conf.get_deploy_config()),
        )

    def retrieve_stream_conf(self):
        """
        获取 stream_job的Flink相关运行配置信息
        :return:
        """

        stream_conf = self._get_stream_conf()
        if not stream_conf.state_backend:
            stream_conf.state_backend = self._get_state_backend()

        if not stream_conf.checkpoint_interval:
            stream_conf.checkpoint_interval = self._get_checkpoint_interval()

        if not stream_conf.concurrency:
            stream_conf.concurrency = self._get_concurrency()

        return stream_conf.get_deploy_config()
