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

from conf.dataapi_settings import (
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    UC_TIME_ZONE,
)
from django.db.models import Max

from dataflow.component.utils.time_util import get_datetime
from dataflow.pizza_settings import API_URL
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.handlers import processing_version_config
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import CodeVersionNotFoundError, SourceIllegal
from dataflow.stream.handlers import processing_stream_info, processing_stream_job
from dataflow.stream.job.adaptors import FlinkAdaptor, SparkStructuredStreamingAdaptor
from dataflow.stream.job.job import Job
from dataflow.stream.result_table.result_table_for_common import fix_filed_type
from dataflow.stream.settings import (
    METRIC_KAFKA_TOPIC,
    ONE_CODE_USER_MAIN_CLASS,
    SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG,
)


class CodeJob(Job):
    def get_code_version(self):
        code_version = super(CodeJob, self).get_code_version()

        if self.job_info.programming_language == "java":
            package_type = ".jar"
        else:
            package_type = ".zip"
        # 若 DB 未配置 code_version，取默认
        branch_tag = "code"
        if not code_version:
            code_version = "master_{}_{}".format(
                branch_tag,
                self.job_info.programming_language,
            )
        else:
            rows = list(
                processing_version_config.where(
                    component_type=self.component_type, branch__icontains=branch_tag
                ).values()
            )
            for row in rows:
                tag = row["branch"] + "-" + row["version"]
                # 允许 code_version 带版本号
                if code_version == tag:
                    return tag + package_type
        # 若 code_version 不是全路径或未指定 code_version
        version = processing_version_config.where(component_type=self.component_type, branch=code_version).aggregate(
            Max("version")
        )["version__max"]
        logger.info(
            "the job %s code version is %s, component_type is %s, code_version is %s"
            % (self.job_id, version, self.component_type, code_version)
        )
        if not version:
            raise CodeVersionNotFoundError()
        return code_version + "-" + version + package_type

    def _get_start_position(self, use_savepoint):
        """
        根据 offset、use_savepoint 值判断从哪里启动任务
        offset: 0-> 最新 1->继续 -1->最早
        @param use_savepoint:
        @return:
        """
        if self.offset == 0:
            return "from_tail"
        elif self.offset == -1:
            return "from_head"
        else:
            if not use_savepoint:
                logger.info("当前(%s)选择了继续处理，但节点未启用 Savepoint，采用从头部进行处理." % self.job_id)
                return "from_head"
            return "continue"

    def _get_processor_logic(self):
        processings = processing_stream_info.filter(stream_id=self.real_job_id)
        # code节点 processing 只有一个
        if len(processings) != 1:
            raise Exception("The data processing corresponding to the job %s is not only one." % self.real_job_id)
        processor_logic = json.loads(processings[0].processor_logic)
        return processor_logic

    def _get_code(self):
        processings = processing_stream_info.filter(stream_id=self.real_job_id)
        if len(processings) == 1:
            processor_logic = json.loads(processings[0].processor_logic)
            return processor_logic["code"]
        else:
            raise Exception("The spark_structured_streaming processing is not only one[%s]." % len(processings))

    def register(self, jar_name, geog_area_code, is_fill_snapshot=False):

        task_manager_memory = self.adaptor.task_manager_memory
        parallelism = self.adaptor.parallelism
        logger.info("The job id parallelism is {} and unit memory is {}.".format(parallelism, task_manager_memory))

        # 拼接提交信息参数和作业参数
        processor_logic = self._get_processor_logic()

        custom_jars = []
        jar_files = jar_name
        ftp_server_url = self.ftp_server().get_url()
        job_args = {
            "job_id": self.job_id,
            "job_type": self.adaptor.job_type,
            "run_mode": "product",
            "api_url": {"base_dataflow_url": "http://%s/v3/dataflow/" % API_URL},
        }

        submit_args = {
            "job_id": self.job_id,
            "cluster_name": self.adaptor.cluster_name,
            "deploy_mode": self.adaptor.deploy_mode,
            "jar_files": jar_files,
            "custom_jars": custom_jars,
            "parallelism": self.adaptor.parallelism,
            "deploy_config": self.deploy_config,
            "task_manager_memory": task_manager_memory,
            "programming_language": processor_logic["programming_language"],
            "use_savepoint": processor_logic["advanced"]["use_savepoint"],
        }

        if self.component_type == "spark_structured_streaming":
            code_package_path = "{}/app/spark_structured_streaming/system/var/{}".format(
                self.ftp_server().get_url(),
                jar_name,
            )
            user_package = processor_logic["package"]["path"]
            if user_package != "":
                if not user_package.startswith("hdfs"):
                    user_package = ftp_server_url + user_package
                submit_args["jar_files"] = "{},{}".format(user_package, code_package_path)
            else:
                submit_args["jar_files"] = code_package_path
        elif self.component_type_without_version == "flink":
            submit_args["code"] = processor_logic["code"]
            submit_args["user_main_class"] = ONE_CODE_USER_MAIN_CLASS

        # 更新任务动态执行表
        if not is_fill_snapshot:
            processing_stream_job.save(
                stream_id=self.job_id,
                running_version=jar_name,
                component_type=self.component_type,
                cluster_group=self.cluster_group,
                cluster_name=self.adaptor.cluster_name,
                heads=",".join(self.heads),
                tails=",".join(self.tails),
                status="preparing",
                concurrency=self.adaptor.parallelism,
                deploy_mode=self.adaptor.deploy_mode,
                deploy_config=json.dumps(self.deploy_config),
                offset=self.offset,
                implement_type=self.implement_type,
                programming_language=self.programming_language,
                created_at=get_datetime(),
                created_by=self.job_info.created_by,
                updated_by=self.job_info.updated_by,
            )

        return {"job_args": job_args, "submit_args": submit_args}

    @property
    def adaptor(self):
        if not self._adaptor:
            if self.component_type == "spark_structured_streaming":
                self._adaptor = SparkStructuredStreamingAdaptor(self)
            else:
                self._adaptor = FlinkAdaptor(self)
        return self._adaptor

    def _generate_fields(self, channel_direction, meta_fields):
        """
        产生 input | output 字段列表
        @param channel_direction:
        @param meta_fields:
        @return:
        """
        fixed_fields = []
        common_fields = []
        for field in meta_fields:
            if "timestamp" != field["field_name"]:
                common_fields.append(
                    {
                        "field": field["field_name"],
                        "type": fix_filed_type(field["field_type"]),
                        "origin": field["origins"] or "",
                        "description": field["description"],
                        "event_time": field["roles"].get("event_time"),
                    }
                )
        if channel_direction == "input":
            # code 输入允许用户使用以下系统字段，输出时 UC 会将这些字段信息补充
            fixed_fields.append(
                {
                    "field": "dtEventTimeStamp",
                    "type": "long",
                    "origin": "",
                    "description": "event timestamp",
                    "event_time": False,
                }
            )
            fixed_fields.append(
                {
                    "field": "localTime",
                    "type": "string",
                    "origin": "",
                    "description": "localTime",
                    "event_time": False,
                }
            )
            fixed_fields.append(
                {
                    "field": "dtEventTime",
                    "type": "string",
                    "origin": "",
                    "description": "event time",
                    "event_time": False,  # event_time 为 True 表示用户指定该字段为时间字段，其它为 False
                }
            )
        return fixed_fields + common_fields

    def _generate_nodes(self, input_result_tables, output_result_tables):
        """
        code 节点产生 source 和 sink
        @param input_result_tables: 输入结果表列表
        @param output_result_tables: 输出结果表列表
        @return:
        """
        nodes = {"source": {}, "sink": {}}
        generate_node_params = {
            "source": input_result_tables,
            "sink": output_result_tables,
        }
        for result_table_stage, result_table_ids in list(generate_node_params.items()):
            for result_table_id in result_table_ids:
                table_info = MetaApiHelper.get_result_table(result_table_id, related=["fields", "storages"])
                # 输入和输出必须有 kafka 存储
                if "kafka" not in table_info["storages"] and "source" == result_table_stage:
                    raise SourceIllegal()

                if "kafka" not in table_info["storages"] and "sink" == result_table_stage:
                    continue
                kafka_info = table_info["storages"]["kafka"]["storage_channel"]
                channel_direction = "input" if result_table_stage == "source" else "output"
                result_table = {
                    "id": table_info["result_table_id"],
                    "name": "{}_{}".format(table_info["result_table_name"], table_info["bk_biz_id"]),
                    "description": table_info["description"],
                    channel_direction: {
                        "type": "kafka",
                        "cluster_domain": kafka_info["cluster_domain"],
                        "cluster_port": str(kafka_info["cluster_port"]),
                    },
                    "fields": self._generate_fields(channel_direction, table_info["fields"]),
                }
                nodes[result_table_stage][result_table_id] = result_table
        return nodes

    def generate_job_config(self):
        processor_logic = self._get_processor_logic()
        hdfs_path_fs = self.ftp_server().get_url()
        # spark structure streaming checkpoint 使用低负载的hdfs集群
        if "spark_structured_streaming" == self.adaptor.job_type:
            checkpoint_custer_tag = SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG
            checkpoint_ftp_server = self.ftp_server(tag=checkpoint_custer_tag)
            hdfs_path_fs = checkpoint_ftp_server.get_url()
        hdfs_path = "{}/app/{}/checkpoints/{}/".format(
            hdfs_path_fs,
            self.adaptor.job_type,
            self.job_id,
        )
        metric_kafka_server = DatamanageHelper.get_metric_kafka_server(self.geog_area_code)
        if not self.is_debug:
            run_mode = "product"
            debug_id = None
            start_position = self._get_start_position(processor_logic["advanced"]["use_savepoint"])
        else:
            run_mode = "debug"
            debug_id = self.job_id.split("__")[0]
            start_position = "from_head"
        job_config = {
            "job_id": self.job_id,
            "job_name": self.job_id,
            "job_type": self.adaptor.job_type,
            "run_mode": run_mode,
            "time_zone": UC_TIME_ZONE,
            "savepoint": {
                "opened": processor_logic["advanced"]["use_savepoint"],
                "hdfs_path": hdfs_path,
                "start_position": start_position,
            },
            "debug": {
                "debug_id": debug_id,
                "debug_result_data_rest_api_url": STREAM_DEBUG_RESULT_DATA_REST_API_URL,
                "debug_node_metric_rest_api_url": STREAM_DEBUG_NODE_METRIC_REST_API_URL,
                "debug_error_data_rest_api_url": STREAM_DEBUG_ERROR_DATA_REST_API_URL,
            },
            "metric": {
                "metric_kafka_server": metric_kafka_server,
                "metric_kafka_topic": METRIC_KAFKA_TOPIC,
            },
            "nodes": self._generate_nodes(
                processor_logic["input_result_tables"],
                processor_logic["output_result_tables"],
            ),
            "user_main_class": processor_logic["user_main_class"]
            if self.adaptor.job_type == "spark_structured_streaming"
            else ONE_CODE_USER_MAIN_CLASS,
            "user_args": processor_logic["user_args"].split(" "),
            "engine_conf": processor_logic["advanced"]["engine_conf"]
            if "engine_conf" in processor_logic["advanced"] and processor_logic["advanced"]["engine_conf"]
            else {},
        }
        if self.adaptor.job_type == "spark_structured_streaming":
            job_config["code"] = self._get_code()
        return job_config

    def transform_checkpoint(self):
        raise NotImplementedError

    def transform_config(self):
        raise NotImplementedError

    def update_stream_conf(self, params):
        raise NotImplementedError

    def retrieve_stream_conf(self):
        raise NotImplementedError
