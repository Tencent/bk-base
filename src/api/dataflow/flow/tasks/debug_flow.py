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
from django.utils.translation import ugettext_noop

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowDebugLog
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.tasks.debug import BaseNodeDebugger, DebuggerTerminatedError, NodeDebuggerError
from dataflow.flow.tasks.debug.batch import BatchNodeDebugger
from dataflow.flow.tasks.debug.flink_streaming import FlinkStreamingNodeDebugger
from dataflow.flow.tasks.debug.model_app import ModelAppNodeDebugger
from dataflow.flow.tasks.debug.stream import StreamNodeDebugger
from dataflow.flow.utils.language import Bilingual
from dataflow.flow.utils.local import activate_request
from dataflow.shared.log import flow_logger as logger


class SourceDebugger(BaseNodeDebugger):
    def build_context(self):
        return {
            "node_id": self.nodes[0].node_id,
            "result_table_id": self.nodes[0].result_table_id,
            "node_type": self.nodes[0].node_type,
            "operator": self.flow_debugger.debugger.created_by,
        }

    def execute_inner(self):
        """
        清洗数据节点，检查是否存在上报数据
        """
        node_id = self.context["node_id"]
        result_table_id = self.context["result_table_id"]
        node_type = self.context["node_type"]

        # 检查数据源是否正常输出数据
        o_rt = ResultTable(result_table_id=result_table_id)

        if node_type in [NodeTypes.STREAM_SOURCE]:
            kafka_msg = o_rt.get_kafka_msg()
            if kafka_msg is not None:
                self.log(
                    Bilingual(ugettext_noop("正常输出至KAFKA，获取最新一条数据 >>> {}")).format(kafka_msg),
                    stage=node_id,
                )
            else:
                raise NodeDebuggerError(
                    node_id,
                    _("未检测到 {} 数据输出").format(NodeTypes.DISPLAY.get(node_type, node_type)),
                )
        elif node_type in [
            NodeTypes.BATCH_SOURCE,
            NodeTypes.HDFS_STORAGE,
        ]:
            hdfs_msg = o_rt.get_hdfs_msg()
            if hdfs_msg is not None:
                self.log(
                    Bilingual(ugettext_noop("正常输出至HDFS，获取最新一条数据 >>> {}")).format(hdfs_msg),
                    stage=node_id,
                )
            else:
                raise NodeDebuggerError(
                    node_id,
                    _("未检测到 {} 数据输出").format(NodeTypes.DISPLAY.get(node_type, node_type)),
                )


class FlowDebuggerHandler(object):
    M_NODE_TASK = {
        NodeTypes.STREAM_SOURCE: SourceDebugger,
        NodeTypes.BATCH_SOURCE: SourceDebugger,
        # HDFS若作为中间节点，角色相当于离线数据源
        NodeTypes.HDFS_STORAGE: SourceDebugger,
        NodeTypes.FLINK_STREAMING: FlinkStreamingNodeDebugger,
    }

    def query_debug_detail(self, node_id):
        node_debug = self.debugger.get_node_debug(node_id)
        return node_debug

    @classmethod
    def create(cls, flow_id, operator, context=None, version=None):
        kwargs = {"flow_id": flow_id, "created_by": operator}
        if context is not None:
            kwargs["context"] = json.dumps(context)
        if version is not None:
            kwargs["version"] = version

        o_debugger = FlowDebugLog.objects.create(**kwargs)
        return cls(o_debugger.id)

    def __init__(self, debugger_id):
        self.debugger_id = debugger_id
        self.debugger = FlowDebugLog.objects.get(id=debugger_id)

        # 缓存 FlowHandler 对象
        self._flow = None

    def execute(self):
        try:
            # 重新初始化线程变量
            activate_request(self.debugger.created_by)

            self.lock()

            self.log(
                Bilingual(ugettext_noop("用户{operator}启动调试")).format(operator=self.debugger.created_by),
                level="INFO",
            )

            self.debugger.set_status(FlowDebugLog.STATUS.RUNNING)

            # 主逻辑
            self.split()

            self.execute_before()
            self.split()

            node_tasks = self.build_debug_node_tasks()
            for _nt in node_tasks:
                _nt.execute()

            self.ok_callback()
        except DebuggerTerminatedError:
            self.log(Bilingual(ugettext_noop("中止调试")), level="ERROR", check_terminated=False)
        except NodeDebuggerError as e:
            logger.exception("[Flow Debugger] 节点调试异常")
            self.fail_callback()
            self.log(
                Bilingual(ugettext_noop("调试过程出现非预期异常，明细({})")).format(e.message),
                level="ERROR",
                stage=e.node_id,
            )
        except FlowError as e:
            logger.exception("[Flow Debugger] 调试过程出现自定义异常（FlowError）")
            self.fail_callback()
            if e.bilingual_message is not None:
                self.log(e.bilingual_message, level="ERROR")
            else:
                self.log(" | ".join(e.args), level="ERROR")
        except Exception as e:
            logger.exception("[Flow Debugger] 调试过程出现非预期异常")
            self.fail_callback()
            self.log(Bilingual(ugettext_noop("调试过程出现非预期异常，明细({})")).format(e), level="ERROR")
        finally:
            try:
                self.clear()
            finally:
                self.unlock()

    def lock(self):
        self.log(Bilingual(ugettext_noop("锁定任务")))
        self.flow.lock("调试任务中")

    def unlock(self):
        self.log(Bilingual(ugettext_noop("解锁任务")), check_terminated=False)
        self.flow.unlock()

    def ok_callback(self):
        self.debugger.set_status(FlowDebugLog.STATUS.SUCCESS)

    def fail_callback(self):
        self.debugger.set_status(FlowDebugLog.STATUS.FAILURE)

    def build_debug_node_tasks(self):
        """
        组装启动节点任务列表
        """
        tasks = []

        nodes = self.flow.get_ordered_nodes()

        add_stream_debugger = False
        add_batch_debugger = False
        add_model_app_debugger = False
        for _n in nodes:
            if _n.node_type in list(self.M_NODE_TASK.keys()):
                # 若存储节点(如HDFS节点后面可以再接节点)后面未接其它节点，不进行调试
                if _n.node_type in [NodeTypes.HDFS_STORAGE] and len(_n.get_to_nodes_handler()) <= 0:
                    continue
                _task = self.M_NODE_TASK[_n.node_type](self, [_n])
                tasks.append(_task)

            # 实时节点打包一起启动，实时调试任务只需要生成一个
            if (
                _n.node_type
                in [
                    NodeTypes.STREAM,
                    NodeTypes.DATA_MODEL_APP,
                    NodeTypes.DATA_MODEL_STREAM_INDICATOR,
                ]
                and not add_stream_debugger
            ):
                _task = StreamNodeDebugger(self, [])
                add_stream_debugger = True
                tasks.append(_task)

            # 离线节点打包一起启动，离线调试任务只需要生成一个
            if (
                _n.node_type
                in [
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                    NodeTypes.DATA_MODEL_BATCH_INDICATOR,
                ]
                and not add_batch_debugger
            ):
                _task = BatchNodeDebugger(self, [])
                add_batch_debugger = True
                tasks.append(_task)

            # 模型应用节点打包一起启动，模型应用调试任务只需要生成一个
            if _n.node_type in [NodeTypes.MODEL_APP] and not add_model_app_debugger:
                _task = ModelAppNodeDebugger(self, [])
                add_model_app_debugger = True
                tasks.append(_task)

        return tasks

    def build_nodes_to_update(self, nodes):
        nodes_to_update = {}
        for _n in nodes:
            if _n.node_id not in nodes_to_update and (_n.is_new_version() or _n.is_modify_version(True)):
                _nodes = [_n]
                if _n.node_type not in NodeTypes.PROCESSING_CATEGORY:
                    _nodes = list(
                        filter(
                            lambda _f_n: _f_n.node_id not in nodes_to_update
                            and _f_n.node_type in NodeTypes.PROCESSING_CATEGORY,
                            _n.get_from_nodes_handler(),
                        )
                    )
                    _nodes.append(_n)
                for _n_n in _nodes:
                    nodes_to_update[_n.node_id] = _n_n
                    self.flow.fetch_all_to_nodes(_n_n, nodes_to_update)
        return nodes_to_update

    def execute_before(self):
        """
        执行前的回调函数，以下为需要完成的检查工作

        1. 按顺序保存每个节点，确保能成功才能启动flow
        2. 与节点的运行版本(若有)进行比较，若与当前版本不同，表明进行过修改，才需要重新保存节点及其下游节点
        """
        o_flow = self.flow

        # 检查是否存在回路
        o_flow.check_circle()

        ordered_nodes = o_flow.check_isolated_nodes()
        nodes_to_update = self.build_nodes_to_update(ordered_nodes)
        for node in ordered_nodes:
            if node.node_id not in nodes_to_update:
                continue
            node = NODE_FACTORY.get_node_handler(node_id=node.node_id)
            node_name = node.name
            type_display = node.get_node_type_display()

            self.log(Bilingual(ugettext_noop("检查{display}节点（{output}）")).format(output=node_name, display=type_display))
            # from_node_ids = node.from_node_ids
            # frontend_info = node.frontend_info
            # form_data = NodeValidate.form_param_validate(node.get_config(),
            #                                              node.node_form,
            #                                              False)
            # node.update_node_info(get_request_username(),
            #                       from_node_ids,
            #                       form_data,
            #                       frontend_info)

    def split(self):
        """
        在对应位置，似的日志可以分块显示
        """
        self.log("-" * 80)

    def log(self, msg, level="INFO", time=None, stage="system", check_terminated=True):
        """
        记录任务执行日志，流程的推进是以日志的方式来展示的，故暂时在该位置加入额外判断，
        调试进程是否在外部被中止

        @todo 探讨是否更加合适的的流程中止方式，log方法负责的逻辑更为纯粹
        @param {Boolean} level 日志级别
        @param {String} time 日志时间
        @param {Boolean} check_terminated 是否在记录日志的时候检查中止
        """
        if check_terminated and self.check_terminated():
            raise DebuggerTerminatedError(Bilingual(ugettext_noop("当前调试任务被中止")))

        self.debugger.add_log(msg, level=level, time=time, stage=stage)

    def clear(self):
        """
        整个调试任务的清理工作
        """
        self.log(
            Bilingual(ugettext_noop("清除调试结果...")),
            stage="system",
            check_terminated=False,
        )

        node_tasks = self.build_debug_node_tasks()
        for _nt in node_tasks:
            _nt.clear()

    def terminate(self):
        """
        中止调试，调试轮询过程会检验当前调试任务的状态，从而主动停止当前调试任务
        """
        # 当调试状态已然结束，则不需要进行额外的中止动作
        if self.debugger.status in FlowDebugLog.DONE_STATUS_SET:
            return

        self.debugger.set_status(FlowDebugLog.STATUS.TERMINATED)

    def check_terminated(self):
        """
        检查当前调试任务是否被中止，实时从 DB 获取
        """
        return FlowDebugLog.objects.get(id=self.debugger_id).status == FlowDebugLog.STATUS.TERMINATED

    @property
    def flow(self):
        if self._flow is None:
            self._flow = FlowHandler(flow_id=self.flow_id)
        return self._flow

    @property
    def flow_id(self):
        return self.debugger.flow_id
