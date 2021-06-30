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

from dataflow.flow import exceptions as Errors
from dataflow.flow.handlers import forms
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_rt_node import RTNode
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.stream.stream_helper import StreamHelper


class StreamNode(RTNode):
    default_storage_type = "hdfs"
    node_type = NodeTypes.STREAM
    node_form = forms.FlowRealtimeNodeForm
    config_attrs = [
        "bk_biz_id",
        "sql",
        "table_name",
        "name",
        "count_freq",
        "waiting_time",
        "window_time",
        "window_type",
        "counter",
        "output_name",
        "from_result_table_ids",
        "session_gap",
        "expired_time",
        "window_lateness",
        "correct_config_id",
        "is_open_correct",
    ]

    # 是否需要写channel由子节点决定
    def add_after(self, username, from_node_ids, form_data):
        # 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params["processing_id"] = self.build_processing_id(form_data["bk_biz_id"], form_data["table_name"])
        rt_params["component_type"] = self.get_new_node_component_type(True)
        response_data = StreamHelper.create_processing(**rt_params)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        self.check_update_param(prev, after)
        # 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        response_data = StreamHelper.update_processing(**rt_params)

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["heads"],
            "tails": response_data["tails"],
        }
        return rts_heads_tails

    def check_update_param(self, prev, after):
        pre_node_config = json.loads(prev["node_config"])
        after_node_config = json.loads(after["node_config"])
        # 若从无窗改为有窗，或从有窗改为无窗
        has_change_window_attr = (pre_node_config["window_type"] == "none") ^ (
            after_node_config["window_type"] == "none"
        )
        # 已经启动过的 storm 任务不允许进行无窗和有窗之间的相互切换
        if has_change_window_attr and self.component_type == "storm" and self.get_job():
            raise Errors.NodeError(_("当前节点不允许在无窗和有窗之间相互切换"))

    # 当前计算节点remove操作无需调用remove_after
    def remove_before(self, username):
        """
        先删processing再删存储，防止processing删除失败存储却删了
        @param username:
        @return:
        """
        super(StreamNode, self).remove_before(username)
        # 删除数据修正，如果有
        self.del_data_correct()
        # 可能存在这样的场景
        # stream -> stream_sdk，从而为 stream 补充 kafka，这时候 stream 就有 kafka 没用户存储
        self.delete_dp_with_rollback(StreamHelper.delete_processing)

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(StreamNode, self).build_before(from_node_ids, form_data, is_create)
        # 当前是节点更新
        if not is_create:
            # 数据修正逻辑
            form_data = self.create_or_update_data_correct(form_data)
        return form_data

    def build_rt_params(self, form_data, from_node_ids, username):
        """
        生成 RT 参数
        """
        (
            not_static_rt_ids,
            static_rt_ids,
            source_rt_ids,
        ) = NodeUtils.build_from_nodes_list(from_node_ids)

        # 保险起见，再校验一次
        rt_dict = self.clean_config(form_data, False)

        # 无窗口和滚动窗口不要求输入window_time，默认转为0
        if rt_dict["window_time"] is None:
            rt_dict["window_time"] = 0
        if rt_dict["count_freq"] is None:
            rt_dict["count_freq"] = 0
        if rt_dict["waiting_time"] is None:
            rt_dict["waiting_time"] = 0
        if rt_dict["session_gap"] is None:
            rt_dict["session_gap"] = 0

        # TODO：目前一个processing只有一个输出
        outputs = [{"bk_biz_id": rt_dict["bk_biz_id"], "table_name": rt_dict["table_name"]}]
        window_lateness = rt_dict["window_lateness"] or {
            "allowed_lateness": False,
            "lateness_time": 0,
            "lateness_count_freq": 0,
        }
        send_dict = {
            "description": rt_dict["output_name"],
            "window_time": rt_dict["window_time"],
            "count_freq": rt_dict["count_freq"],
            "waiting_time": rt_dict["waiting_time"],
            "window_type": rt_dict["window_type"],
            "session_gap": rt_dict["session_gap"],
            "expired_time": rt_dict["expired_time"] or 0,
            "allowed_lateness": window_lateness["allowed_lateness"],
            "lateness_time": window_lateness["lateness_time"],
            "lateness_count_freq": window_lateness["lateness_count_freq"],
        }
        # 增加数据修正逻辑, 替换sql
        sql = form_data["sql"]
        is_open_correct = None
        correct_config_id = None
        if "is_open_correct" in form_data:
            is_open_correct = form_data["is_open_correct"]
        if "correct_config_id" in form_data:
            correct_config_id = form_data["correct_config_id"]
        if is_open_correct and correct_config_id:
            correct_data = DatamanageHelper.get_data_correct({"correct_config_id": correct_config_id})
            sql = correct_data["correct_sql"]
        rt_params = {
            "project_id": self.flow.project_id,
            "sql": sql,
            "dict": send_dict,
            "input_result_tables": not_static_rt_ids,
            "static_data": static_rt_ids,
            "source_data": source_rt_ids,
            "outputs": outputs,
            "tags": self.geog_area_codes,
        }
        return rt_params

    @property
    def processing_type(self):
        return "stream"

    @property
    def component_type(self):
        """
        节点已创建方可调用
        @return:
        """
        o_job = self.get_job()
        if o_job:
            # 若job_id存在，则获取旧任务的component_type
            component_type = StreamHelper.get_job_component_type(o_job.job_id)
        else:
            component_type = StreamHelper.get_processing_component_type(self.processing_id)
        return component_type

    def get_new_node_component_type(self, is_create=False):
        """
        根据当前旧节点的 component_type，决定新节点，新任务的 component_type
        @return:
        """
        component_type = "storm"
        o_job = self.get_job()
        if o_job:
            # 若job_id存在，则获取旧任务的component_type
            component_type = StreamHelper.get_job_component_type(o_job.job_id)
        elif self.is_support_flink(is_create):
            component_type = "flink"
        return component_type

    def is_support_flink(self, is_create=False):
        """
        判断当前节点所在的 flow 是否支持启动为 flink 任务
        原则：
            对于指定项目，若该项目之前启动过为非 flink 任务，则当前不支持启动为 flink 任务
            其它项目都不支持 flink 任务
        @return:
        """
        support_flink = True
        for _node in self.flow.nodes:
            if _node.node_type == NodeTypes.STREAM:
                # 创建过程无法通过自身信息得知是否支持启动为 flink
                if is_create and _node.node_id == self.node_id:
                    continue
                node_handler = NODE_FACTORY.get_node_handler_by_node(_node)
                o_job = node_handler.get_job()
                # 1. 若节点已启动的实时任务为非 flink job, 表示当前 flow 不支持启动为 flink 任务
                if o_job:
                    if StreamHelper.get_job_component_type(o_job.job_id) != "flink":
                        return False
                    else:
                        return True
                # 2. 若当前 flow 存在其他节点是非 flink 节点，表示当前 flow 不支持启动为 flink 任务
                elif StreamHelper.get_processing_component_type(node_handler.processing_id) != "flink":
                    return False
                else:
                    return True
        return support_flink

    @property
    def is_belong_to_topology_job(self):
        return True
