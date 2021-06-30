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

from django.utils.translation import ugettext as _

from dataflow.flow.api_models import BatchJob, ModelAppJob, ResultTable
from dataflow.flow.exceptions import NodeError, ValidError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_rt_node import RTNode
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.serializer.serializers import BatchStatusListSerializer
from dataflow.flow.utils.count_freq import CountFreq
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class BaseBatchNode(RTNode):
    """
    该类作为离线节点的通用类，包括普通的离线节点和 TDW 离线节点等
    """

    def check_downstream_expires(self, form_data):
        """
        在更新 processing 之前，校验下游的 HDFS 存储节点，应小于 HDFS 的过期时间
        @param form_data:
        @return:
        """
        for to_node in self.get_to_nodes_handler():
            if to_node.node_type == NodeTypes.HDFS_STORAGE:
                expires = to_node.get_config(False)["expires"]
                if expires != -1:
                    max_window_size_by_day = NodeUtils.get_max_window_size_by_day(form_data)
                    if max_window_size_by_day > expires:
                        logger.warning(
                            "max_window_size_by_day: {}, expires: {}".format(max_window_size_by_day, expires)
                        )
                        raise ValidError(_("当前节点窗口长度应小于或等于下游 HDFS 存储的过期时间(%s天)") % expires)

    def check_upstream_default_expires(self, form_data, upstream_nodes_info):
        """
        在创建/更新 processing 之前，校验上游 user 类型的 HDFS 存储的过期时间，保证当前欲保存/创建节点的数据窗口长度小于数据的过期时间
        @param form_data:
        @param upstream_nodes_info:
        @return: generate_type 为 system 的存储对应的 result_table_id 列表
        """
        upstream_system_storage_rts = []
        max_window_size_by_day = NodeUtils.get_max_window_size_by_day(form_data)
        for upstream_node_info in upstream_nodes_info:
            for result_table_id in upstream_node_info["result_table_id"]:
                rt = ResultTable(result_table_id)
                origin_expires_value = rt.get_storage_expires_value(self.default_storage_type, "d")
                if rt.has_storage_by_generate_type(self.default_storage_type, "user"):
                    if origin_expires_value != -1 and origin_expires_value < max_window_size_by_day:
                        user_expires_value = (
                            _("永久保存") if origin_expires_value == -1 else (str(origin_expires_value) + _("天"))
                        )
                        raise ValidError(
                            _("当前节点窗口长度(%(window_size)s)应小于结果表(%(result_table_id)s)的过期时间(%(user_expires_value)s)")
                            % {
                                "result_table_id": result_table_id,
                                "user_expires_value": user_expires_value,
                                "window_size": max_window_size_by_day,
                            }
                        )
                else:
                    upstream_system_storage_rts.append(rt)
        return upstream_system_storage_rts

    def update_upstream_default_expires(self, form_data, system_storage_rts, username):
        """
        更新上游默认存储的过期时间，在当前值、所有关联任务最大下游离线节点窗口长度*2、当前任务窗口长度*2、7中取最大者
        TODO: 目前未存在设置默认过期时间的操作，和当前值比较是为降低过期时间调小的风险，若之后引入如 ignite 作为默认存储，可考虑将其去掉
        @param form_data: 当前节点配置信息
        @param system_storage_rts: 上游所有拥有系统(generate_type 为 system)存储的 RT 对象列表
        @param username:
        @return:
        """
        for rt in system_storage_rts:
            # 获取关联数据源之下的离线节点(若有)，计算数据窗口长度，取大者
            origin_expires = rt.get_storage_expires_value(self.default_storage_type)
            if origin_expires == -1:
                upstream_hdfs_expires = origin_expires
            else:
                max_related_window_size = NodeUtils.get_max_related_window_size(rt.result_table_id)
                upstream_hdfs_expires = max(
                    7,
                    NodeUtils.get_max_window_size_by_day(form_data) * 2,
                    max_related_window_size * 2,
                    origin_expires,
                )
            if upstream_hdfs_expires != -1 and upstream_hdfs_expires != origin_expires:
                # 更新上游 HDFS 过期时间
                physical_table_args = self.build_hdfs_storage_config(rt.result_table_id, upstream_hdfs_expires)
                StorekitHelper.update_physical_table(**physical_table_args)

    def update_batch_job(self, operator, cluster_group=None):
        """
        每一个离线节点都会对应一个离线任务
        若离线任务还未创建，则新增，有则更新
        """
        rt_params = self.build_rt_params(self.get_config(), self.from_node_ids, operator)
        exc_info = {"rt_params": rt_params}
        job_id = None
        if self.get_job() is not None:
            job_id = self.get_job().job_id
        geog_area_code = self.geog_area_codes[0]
        params = {
            "processing_id": self.processing_id,
            "code_version": "0.1.0",
            "cluster_group": cluster_group,
            # 'cluster_name': cluster_name,
            "deploy_mode": "yarn",
            "deploy_config": "{executor_memory:1024m}",
            "job_config": {
                "job_name": self.name,
                "job_type": "flow",
                "description": self.name,
                "exc_info": json.dumps(exc_info),
                "workers": "",
            },
            "project_id": self.project_id,
            "jobserver_config": {
                "geog_area_code": geog_area_code,
                "cluster_id": JobNaviHelper.get_jobnavi_cluster("batch"),
            },
        }
        if "api_version" in rt_params:
            params["api_version"] = rt_params["api_version"]

        if job_id is None:
            if self.node_type in [
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                NodeTypes.TDW_BATCH,
            ]:
                o_job = BatchJob.create(params, operator)
            elif self.node_type in [NodeTypes.MODEL_APP]:
                o_job = ModelAppJob.create(params, operator)
            job_id = o_job.job_id
            NodeUtils.update_job_id(self.flow_id, self.node_id, o_job.job_id, "batch")
        else:
            if self.node_type in [
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                NodeTypes.TDW_BATCH,
            ]:
                o_job = BatchJob(job_id=job_id)
            elif self.node_type in [NodeTypes.MODEL_APP]:
                o_job = ModelAppJob(job_id=job_id)
            o_job.update(params, operator)
        return job_id

    def get_status_list(self, start_time, end_time):
        """
        @param start_time: "2018-12-12 11:00:00"
        @param end_time: "2018-12-12 11:00:00"
        @return:
        """
        # 未启动过的离线任务直接返回空列表
        if not self.running_version:
            return []
        data_start = BatchStatusListSerializer.get_format_time(start_time)
        data_end = BatchStatusListSerializer.get_format_time(end_time)
        if data_start >= data_end:
            raise NodeError(_("起始时间应小于截止时间"))
        status_list = BatchHelper.status_list(self.processing_id, data_start, data_end, self.geog_area_codes[0])
        for _job_info in status_list:
            _job_info["schedule_time"] = BatchStatusListSerializer.format_timestamp(_job_info["schedule_time"])
            _job_info["start_time"] = BatchStatusListSerializer.format_timestamp(_job_info["created_at"])
            _job_info["end_time"] = BatchStatusListSerializer.format_timestamp(_job_info["updated_at"])
        return status_list

    def check_execution(self, schedule_time):
        return BatchHelper.check_execution(self.processing_id, schedule_time, self.geog_area_codes[0])

    def get_schedule_period_from_batch_node(self):
        """
        获取离线类节点的窗口周期参数
        """
        node_type = self.node_type
        node_config = self.get_config(False)
        schedule_period = "hour"
        if node_type == NodeTypes.BATCHV2:
            schedule_period = node_config["dedicated_config"]["schedule_config"]["schedule_period"]
        elif node_type == NodeTypes.BATCH or node_type == NodeTypes.MODEL_APP:
            # 老batch节点
            schedule_period = node_config["schedule_period"]
        elif node_type == NodeTypes.DATA_MODEL_BATCH_INDICATOR:
            schedule_period = node_config["window_config"]["schedule_period"]

        return schedule_period

    @property
    def processing_type(self):
        # 处理类型
        return "batch"

    @property
    def batch_type(self):
        # batch子类型，分为 default / tdw / tdw_jar / spark_mllib
        return "default"

    @property
    def start_time(self):
        """
        返回节点启动的时间
        """
        _config = self.get_config(False)
        advanced = _config.get("advanced", {}) or {}
        return advanced.get("start_time", "")

    @property
    def delay_time(self):
        """
        返回任务延迟的时间，延迟单位都是小时
        """
        _config = self.get_config(loading_latest=False)
        if "node_type" in _config and _config["node_type"] == "batchv2":
            return 0
        return _config.get("delay", 0) if _config.get("accumulate", True) else _config.get("fixed_delay")

    @property
    def count_freq(self):
        _config = self.get_config(False)
        if "node_type" in _config and _config["node_type"] == "batchv2":
            # 新batch节点
            return CountFreq(
                _config["dedicated_config"]["schedule_config"]["count_freq"],
                _config["dedicated_config"]["schedule_config"]["schedule_period"],
            )
        else:
            # 老batch节点
            return CountFreq(_config["count_freq"], _config["schedule_period"])
