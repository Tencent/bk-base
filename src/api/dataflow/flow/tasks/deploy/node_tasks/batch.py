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

import random
import time

from common.exceptions import ApiRequestError
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowTaskError
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.batch.batch_helper import BatchHelper

from . import BaseNodeTaskHandler


class BatchNodeTask(BaseNodeTaskHandler):
    def build_start_context(self, flow_task_handler, o_nodes):
        operator = flow_task_handler.operator
        task_context = flow_task_handler.context
        o_node = o_nodes[0]
        cluster_group = task_context["cluster_group"] if "cluster_group" in task_context else None
        job_id = o_node.update_batch_job(operator, cluster_group)
        return {
            "job_id": job_id,
            "api_version": "v2" if o_node.node_type == NodeTypes.BATCHV2 else "",
            "result_table_ids": o_node.result_table_ids,
            "is_restart": True if flow_task_handler.action_type == "restart" else False,
            "operator": operator,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        # operator = flow_task_handler.operator
        o_node = o_nodes[0]
        job_id = o_node.get_job().job_id
        # if not job_id:
        #     job_id = o_node.update_batch_job(operator)
        return {
            "job_id": job_id,
            "api_version": "v2" if o_node.node_type == NodeTypes.BATCHV2 else "",
            "result_table_ids": o_node.result_table_ids,
            "operator": flow_task_handler.operator,
        }

    def tdw_storage_action(self, op_type):
        o_node = self.nodes[0]
        # 若下游未包含 tdw 存储节点，创建隐含的 TDW 存储
        if o_node.node_type in [
            NodeTypes.TDW_BATCH,
            NodeTypes.TDW_JAR_BATCH,
        ]:
            storage_node_type = o_node.default_storage_type
            if op_type == "start":
                for result_table_id in self.context["result_table_ids"]:
                    tdw_batch_node_rt = ResultTable(result_table_id)
                    if not tdw_batch_node_rt.has_storage(storage_node_type):
                        raise FlowTaskError(
                            _("TDW离线节点({rt_id})下游未接TDW存储节点，也不存在指定默认存储({storage_type})，流程中止").format(
                                rt_id=result_table_id, storage_type=storage_node_type
                            )
                        )
                    self.log(Bilingual(ugettext_noop("检查 TDW(%s) 存储" % result_table_id)))

    # def stream_shipper_action(self, op_type):
    #     parent_nodes = self.nodes[0].get_from_nodes_handler()
    #     storage_node_type = self.nodes[0].default_storage_type
    #     storage_node_type_map = {value: key for key, value in NodeTypes.STORAGE_NODES_MAPPING.items()}
    #     for parent_node in parent_nodes:
    #         # 旧规则支持实时计算/实时数据源直接连接离线计算的情况
    #         if parent_node.node_type in [NodeTypes.STREAM, NodeTypes.DATA_MODEL_APP,
    #                                      NodeTypes.DATA_MODEL_STREAM_INDICATOR,
    #                                      NodeTypes.STREAM_SOURCE]:
    #             # TODO：实时节点暂时只有一个输出
    #             parent_result_table_id = parent_node.result_table_ids[0]
    #             parent_rt = ResultTable(parent_result_table_id)
    #             # 需要启动和停止离线计算节点上游的结果表到默认存储(目前是HDFS)分发任务的情况如下
    #             # 1. 只有 parent_result_table_id 拥有可见且为 running 的 storage_node_type 节点，才不启分发任务(PS: 重复启动没关系)
    #             # 2. 若 parent_result_table_id 拥有可见的且为非 no-start 的 storage_node_type 节点，不可停分发任务
    #             # 3. 上游 RT 并非由离线 processing 产生(当前离线 processing 的默认存储数据写入不需要启动分发进程)
    #             origin_node = parent_node if parent_node.node_type != NodeTypes.STREAM_SOURCE \
    #                 else parent_node.origin_node
    #             if not origin_node or origin_node.is_batch_node:
    #                 continue
    #             if parent_rt.has_storage(parent_node.channel_storage_type) \
    #                 and parent_rt.has_storage(storage_node_type):
    #                 # downstream_nodes 为 origin_node 下游指定存储节点，最多只有一个
    #                 downstream_nodes = filter(
    #                     lambda _to_node: _to_node.node_type == storage_node_type_map.get(storage_node_type),
    #                     origin_node.get_to_nodes_handler())
    #                 if op_type == 'start':
    #                     if downstream_nodes and downstream_nodes[0].status == FlowInfo.STATUS.RUNNING:
    #                         logger.warning(u'上游结果表(%s)的分发任务正在运行，无需启动' % parent_result_table_id)
    #                         continue
    #                     logger.info(u'启动离线计算上游结果表(%s)对应的分发任务' % parent_result_table_id)
    #                     if storage_node_type == 'hdfs':
    #                         self.log(Bilingual(ugettext_noop(u"检查上游 HDFS 存储")))
    #                         StorekitHelper.prepare(parent_result_table_id, 'hdfs')
    #                     DatabusHelper.create_task(
    #                         parent_result_table_id,
    #                         [storage_node_type]
    #                     )
    #                 elif op_type == 'stop':
    #                     if downstream_nodes and downstream_nodes[0].status != FlowInfo.STATUS.NO_START:
    #                         logger.warning(u'上游结果表(%s)存在运行中的 %s 节点，无需停止'
    #                                        % (parent_result_table_id, storage_node_type))
    #                         continue
    #                     logger.info(u'停止离线计算上游结果表(%s)对应的分发任务' % parent_result_table_id)
    #                     DatabusHelper.stop_task(
    #                         parent_result_table_id,
    #                         [storage_node_type]
    #                     )
    #             elif op_type == 'start':
    #                 raise FlowTaskError(_(u"父实时节点({rt_id})不存在指定数据源({channel_type},{storage_type})，流程中止").
    #                                     format(rt_id=parent_result_table_id,
    #                                            channel_type=parent_node.channel_storage_type,
    #                                            storage_type=storage_node_type))

    def start_before(self):
        """
        1. 负责离线任务启动时的检查以及相关分发进程的启动
        父节点若是实时节点、实时数据源节点：
           1. 确保数据链Stream -> (kafka >>> HDFS) -> Batch存在
           2. 启动HDFS分发进程
        简单处理：若父节点是实时节点，尝试启动父RT对应的HDFS分发进程
        2. tdw 离线节点
            需要先将隐含的 tdw 存储节点启动，保证执行离线任务时表已经建成功
        @return:
        """
        # self.stream_shipper_action('start')
        super(BatchNodeTask, self).start_before()
        self.tdw_storage_action("start")

    def start_after(self):
        """
        负责离线任务启动后的检查以及相关分发进程的启动，例如 tdw 节点，若下游未接 tdw 存储节点，需要将隐含的 tdw 存储节点启动
        @return:
        """
        pass

    def start_inner(self):
        self.log(Bilingual(ugettext_noop("启动离线调度进程")))

        # 重试 3 次，提高成功率
        index = 1
        max_times = 3

        while True:
            try:
                api_param = {
                    "job_id": self.context["job_id"],
                    "is_restart": self.context["is_restart"],
                    "api_version": self.context["api_version"] if "api_version" in self.context else "",
                }
                BatchHelper.start_job(**api_param)

                _msg = Bilingual(ugettext_noop("第{n}次尝试 batch.start_job 成功，无错误信息")).format(n=index)
                self.log(_msg)
                break
            except ApiRequestError as e:

                _msg = Bilingual(ugettext_noop("第{n}次尝试 batch.start_job 失败，error={err}")).format(n=index, err=e.message)
                self.log(_msg, "WARNING")

                index += 1
                if index > max_times:
                    raise FlowTaskError(Bilingual(ugettext_noop("连续{n}次尝试 batch.start_job 均失败，流程中止")).format(n=index))
                time.sleep(random.uniform(1, 2))

        self.start_after()

    def stop_after(self):
        """
        与start_before想对应，负责离线任务停止后的检查以及相关停止进程的启动
        @return:
        """
        # self.stream_shipper_action('stop')

    def stop_inner(self):
        self.log(Bilingual(ugettext_noop("停止离线调度进程")))
        api_param = {
            "job_id": self.context["job_id"],
            "api_version": self.context["api_version"] if "api_version" in self.context else "",
        }
        BatchHelper.stop_job(**api_param)
        self.stop_after()

    def start_ok_callback(self):
        super(BatchNodeTask, self).start_ok_callback()

    def stop_ok_callback(self):
        super(BatchNodeTask, self).stop_ok_callback()
