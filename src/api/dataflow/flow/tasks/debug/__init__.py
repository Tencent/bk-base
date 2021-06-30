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

import time
from datetime import datetime, timedelta

from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.exceptions import DebuggerError
from dataflow.flow.models import FlowDebugLog
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.shared.stream.stream_helper import StreamHelper


class NodeDebuggerError(DebuggerError):
    def __init__(self, node_id, *args):
        # 比默认调试异常保留 node_id 字段
        self.node_id = node_id
        super(NodeDebuggerError, self).__init__(*args)


class DebuggerTerminatedError(DebuggerError):
    pass


class BaseNodeDebugger(object):
    def __init__(self, flow_debugger, o_nodes):
        """
        @param {FlowDebuggerHandler} flow_debugger
        @param {[NodeHandler]} o_nodes 节点对象列表
        """
        self.nodes = o_nodes
        self.flow_debugger = flow_debugger
        self.context = None

    def build_context(self):
        """
        节点调试任务，组装执行过程所需要的上下文
        """
        raise NotImplementedError

    def execute(self):
        """
        调试入口函数

        @note 当执行了 execute 函数，才会对 self.context 字段进行赋值
        """
        try:
            self.context = self.build_context()
            self.save_context(self.context)

            self.execute_before()

            self.log(
                Bilingual(ugettext_noop("调试（{display}）{node_type}节点")).format(
                    display=self.nodes_display,
                    node_type=Bilingual.from_lazy(self.nodes_type_display),
                )
            )

            self.execute_inner()
            self.flow_debugger.split()

            self.ok_callback()
        except NodeDebuggerError as e:
            logger.exception("[Flow Debugger] 节点调试异常")
            self.fail_callback()
            raise e
        except Exception as e:
            logger.exception("[Flow Debugger] 节点调试异常")
            self.fail_callback()
            raise e

    def execute_inner(self):
        """
        主体调试逻辑，待子节点完善
        """
        raise NotImplementedError

    def execute_before(self):
        """
        节点调试前回调
        """
        self.save_status(FlowDebugLog.STATUS.RUNNING)

    def ok_callback(self):
        """
        节点调试成功回调
        """
        self.save_status(FlowDebugLog.STATUS.SUCCESS)

    def fail_callback(self):
        """
        节点调试失败回调
        """
        self.save_status(FlowDebugLog.STATUS.FAILURE)

    def save_status(self, status, node_id=None):
        """
        保存节点调试状态
        """
        _nodes = [self.get_node(node_id)] if node_id else self.nodes

        for _n in _nodes:
            self.flow_debugger.debugger.save_node_info(_n.node_id, {"status": status})

    def save_context(self, context, node_id=None):
        """
        保存节点调试上下文（输入参数）
        """
        _nodes = [self.get_node(node_id)] if node_id else self.nodes

        for _n in _nodes:
            self.flow_debugger.debugger.save_node_info(_n.node_id, {"context": context})

    def save_output(self, output, node_id=None):
        """
        保存节点调试输出结果
        """
        nodes = [self.get_node(node_id)] if node_id else self.nodes

        for _n in nodes:
            self.flow_debugger.debugger.save_node_info(_n.node_id, {"output": output})

    def get_node(self, node_id):
        """
        获取节点对象
        """
        for _n in self.nodes:
            if _n.node_id == node_id:
                return _n
        return None

    def get_node_info(self, node_id):
        """
        获取节点调试信息
        @note 支持查询不在任务中的节点
        """
        return self.flow_debugger.debugger.get_node_info(node_id)

    def log(self, msg, level="INFO", time=None, stage="system"):
        self.flow_debugger.log(msg, level=level, time=time, stage=stage)

    def clear(self):
        """
        节点调试任务清除工作

        @note 该函数可能会被外部调用，故函数内不允许执行 self.log 方法，或者设置
              check_terminated 参数
        """
        pass

    @property
    def nodes_display(self):
        return ", ".join([_node.name for _node in self.nodes])

    @property
    def nodes_type_display(self):
        return self.nodes[0].get_node_type_display()


class ComputingNodeDebugger(BaseNodeDebugger):
    NODE_MAX_WAITING_SECONDS = 60 * 5
    # 标识是子调试任务类型，如实时debug还是离线debug
    debug_type = None

    def execute_inner(self):
        """
        调试入口函数，实时调试模式无法套用默认框架
        """
        try:

            self.log(Bilingual(ugettext_noop("将所有 {display} 节点合并提交调试")).format(display=self.nodes_type_display))
            debug_id = self.start_debug()

            # 包含调试任务ID，一起打包填入context字段
            self.context["debug_id"] = debug_id

            # 更新上下文信息，停止debug任务时需要从其中取debug_id
            self.save_context(self.context)

            # 进入debug任务轮询阶段
            self.check_nodes()
        finally:
            # 在实时调试结束后自动清除（如果后面的调试任务不依赖于当前实时调试任务）
            self.clear()

    def start_debug(self):
        """
        启动调试任务
        """
        pass

    def check_nodes(self):
        """
        按照节点次序检查调试结果
        """
        for _node in self.nodes:
            self.check_node(_node)

    def check_node(self, node):
        """
        检查单个节点调试结果
        """

        # 每个节点的检查时间限制在 NODE_MAX_WAITING
        start_time = datetime.now()
        while datetime.now() - start_time <= timedelta(seconds=self.NODE_MAX_WAITING_SECONDS):
            # 检查单个节点前，需要检查整个实时任务是否出现异常
            self.check_node_error(node)

            # 执行前初始化状态
            self.save_status(FlowDebugLog.STATUS.RUNNING, node.node_id)

            # 后台调试任务以结果表为单位
            # TODO: 目前计算节点仅对应一个输出，之后为适配多输出的方式调试功能需要改造
            _node_details = self.query_debug_detail(
                self.context["job_id"], self.context["debug_id"], node.result_table_ids
            )
            # TODO：目前计算节点仅取出一个输出rt对应的调试数据
            _node_detail = _node_details[node.result_table_ids[0]]
            _warning_message = _node_detail["warning_message"]
            _output_count = _node_detail["output_count"]

            # 节点检查结果，简要输出
            if self.debug_type in ["stream"]:
                b_message = Bilingual(
                    ugettext_noop(
                        "总输出{output_count}条，"
                        "转换丢弃{transformer_discard_count}条，"
                        "过滤丢弃{filter_discard_count}条，"
                        "聚合丢弃{aggregator_discard_count}条"
                    )
                ).format(
                    output_count=_output_count,
                    transformer_discard_count=_node_detail["transformer_discard_count"],
                    filter_discard_count=_node_detail["filter_discard_count"],
                    aggregator_discard_count=_node_detail["aggregator_discard_count"],
                )
            else:
                b_message = Bilingual(ugettext_noop("总输入{input_count}条，" "总输出{output_count}条")).format(
                    input_count=_node_detail["input_count"], output_count=_output_count
                )
            self.log(b_message, stage=node.node_id)

            # 有关警告的信息需要返回给用户
            for _w_msg in _warning_message:
                self.log(_w_msg, "WARNING", stage=node.node_id)

            # 保留最新查询结果
            self.save_output({"detail": _node_detail}, node.node_id)

            # 当节点有输出条数，则可认为节点调试成功
            if _output_count > 0 and _node_detail["output_data"]:
                # 获取到节点对应的调试数据，再提示用户即将去获取调试相关数据
                self.log(
                    Bilingual(ugettext_noop("获取（{name}）节点调试相关数据")).format(name=node.name),
                    stage=node.node_id,
                )
                self.log(
                    Bilingual(ugettext_noop("获取一条输出数据 >>> {msg}")).format(msg=_node_detail["output_data"][0]),
                    stage=node.node_id,
                )

                self.save_status(FlowDebugLog.STATUS.SUCCESS, node.node_id)
                break

            self.log("...")
            time.sleep(5)

        else:
            raise NodeDebuggerError(
                node.node_id,
                _("最大限制时间（{max}秒）内未检测到输出数据，请检查配置").format(max=self.NODE_MAX_WAITING_SECONDS),
            )

    def check_node_error(self, node):
        """
        检查整个调试任务是否出现异常信息
        """
        nodes_data = self.query_debug_node_data(node)

        # 全局检查，检查是否存在 ERROR 节点
        error_node_ids = [_node_id for _node_id, _data in list(nodes_data.items()) if _data["has_error"]]
        if error_node_ids:
            # 选择第一个 ERROR 节点输出报错信息，抛异常，中断流程
            error_node_id = error_node_ids[0]

            error_node = self.get_node(error_node_id)

            # 后台调试任务以结果表为单位
            # TODO: 目前实时节点仅对应一个输出，之后为适配多输出的方式调试功能需要改造
            error_details = self.query_debug_detail(
                self.context["job_id"],
                self.context["debug_id"],
                error_node.result_table_ids,
            )
            result_table_id = error_node.result_table_ids[0]
            error_detail = error_details[result_table_id]
            error_messages = error_detail["error_message"]
            if not error_messages:
                logger.exception("调试中发现RT(%s)异常，但未获取到其明细信息" % result_table_id)
            raise NodeDebuggerError(error_node_id, ", ".join(error_messages))

    def fail_callback(self, node_id=None):
        """
        实时调试失败，可能某一节点位置失败，有可能是全局挂掉，出现未在预期内的错误
        """
        for _n in self.nodes:
            if _n.node_id == node_id:
                self.save_status(FlowDebugLog.STATUS.FAILURE, node_id)
            else:
                # 任务已经失败，检查是否处于运行中节点，更新状态为中断
                _info = self.get_node_info(_n)
                if _info["status"] in (FlowDebugLog.STATUS.RUNNING,):
                    self.save_status(FlowDebugLog.STATUS.TERMINATED, node_id)

    @classmethod
    def query_debug_detail(self, job_id, debug_id, result_table_ids):
        """
        查询一个节点的调试状态
        @successExample
            {
                "error_message": [],
                "warning_message": [],

                # 输出数据
                "output_data": [],
                "total_discard_data": [],
                "filter_discard_data": [],
                "transformer_discard_data": [],
                "aggregator_discard_data": [],

                # 统计条数以及各种丢弃率
                "output_count": 111,
                "transformer_discard_count": 1,
                "filter_discard_count": 2,
                "aggregator_discard_count": 3,
                "transformer_discard_rate": 0.10,
                "filter_discard_rate": 0.10,
                "aggregator_discard_rate": 0.20
            }
        """
        # 后台调试任务以结果表为单位
        result_data = {}
        query_dict = {
            "debug_id": debug_id,
        }
        # TODO: 如何取列表形式的debug数据
        for result_table_id in result_table_ids:
            # 查询这个任务的调试信息
            if self.debug_type == "stream":
                query_dict.update({"result_table_id": result_table_id})
                query_result = StreamHelper.get_debug_node_info(**query_dict)
            elif self.debug_type == "batch":
                query_dict.update({"result_table_id": result_table_id})
                query_result = BatchHelper.get_debug_node_info(**query_dict)
            elif self.debug_type == "model_app":
                query_dict.update({"result_table_id": result_table_id})
                query_result = ModelingHelper.get_debug_node_info(**query_dict)
            # 构造返回的内容
            rt_debug_data = {
                "error_message": [],
                "warning_message": [],
                "output_data": [],
                "filter_discard_data": [],
                "transformer_discard_data": [],
                "aggregator_discard_data": [],
            }

            # 以下代码请联合调试接口说明服用更佳 : P
            debug_metric = query_result.get("debug_metric", {})
            debug_errcode = query_result.get("debug_errcode", {})
            debug_data = query_result.get("debug_data", {})

            # 1, 先遍历构造所有的输出信息
            rt_debug_data["warning_message"] = debug_metric.get("warning_info", [])

            # 获取所有条数的信息
            rt_debug_data["input_count"] = debug_metric["input_total_count"]
            rt_debug_data["output_count"] = debug_metric["output_total_count"]
            rt_debug_data["transformer_discard_count"] = debug_metric.get("transformer_discard_count", 0)
            rt_debug_data["filter_discard_count"] = debug_metric.get("filter_discard_count", 0)
            rt_debug_data["aggregator_discard_count"] = debug_metric.get("aggregator_discard_count", 0)
            rt_debug_data["transformer_discard_rate"] = debug_metric.get("transformer_discard_rate", 0)
            rt_debug_data["filter_discard_rate"] = debug_metric.get("filter_discard_rate", 0)
            rt_debug_data["aggregator_discard_rate"] = debug_metric.get("aggregator_discard_rate", 0)

            # 2, 遍历构造所有的详细数据信息,仅保留10条
            rt_debug_data["output_data"] = debug_data["result_data"]
            # 2.1, 获取丢弃数据信息
            rt_debug_data["total_discard_data"] = []
            if "discard_data" in debug_data:
                rt_debug_data["filter_discard_data"] = debug_data["discard_data"]["filter"]
                rt_debug_data["transformer_discard_data"] = debug_data["discard_data"]["transformer"]
                rt_debug_data["aggregator_discard_data"] = debug_data["discard_data"]["aggregator"]

                # 获取所有丢失数据的集合
                for reason, data_list in (
                    ("filter_discard", rt_debug_data["filter_discard_data"]),
                    ("transformer_discard", rt_debug_data["transformer_discard_data"]),
                    ("aggregator_discard", rt_debug_data["aggregator_discard_data"]),
                ):
                    for row in data_list:
                        row["reason"] = reason
                        rt_debug_data["total_discard_data"].append(row)

                rt_debug_data["total_discard_data"].sort(key=lambda row_data: row_data["dtEventTime"])

            # 3, 判断是否有告警信息需要展示
            if not debug_errcode:
                # 注意,这里返回的错误信息是一个数组,为了后面的扩展方便
                rt_debug_data["error_message"] = [debug_errcode["error_message"]]

            result_data[result_table_id] = rt_debug_data
        return result_data

    def query_debug_node_data(self, node):
        """
        查询调试任务所有节点的状态
        @successExample
            {
                111: {
                    'output_count': 11,
                    'warning_count': 1,
                    'has_error': True
                }
            }
        """
        # 查询这个任务的调试信息
        if self.debug_type == "stream":
            query_dict = {"debug_id": self.context["debug_id"]}
            query_result = StreamHelper.get_debug_basic_info(**query_dict)
        elif self.debug_type == "batch":
            query_dict = {"debug_id": self.context["debug_id"]}
            query_result = BatchHelper.get_debug_basic_info(**query_dict)
        elif self.debug_type == "model_app":
            query_dict = {"debug_id": self.context["debug_id"]}
            query_result = ModelingHelper.get_debug_basic_info(**query_dict)
        # 构造返回的内容
        result_data = {}

        # 以下代码请联合调试接口说明服用更佳 :P
        # 遍历构造所有的输出信息
        result_table = query_result["result_tables"]
        debug_error = query_result["debug_error"]

        # 实时计算无法判断是哪个rt出现错误，用all表示所有rt调试错误
        has_error = False
        if debug_error and debug_error["error_result_table_id"] == "all":
            has_error = True

        for result_table_id, result_table_info in list(result_table.items()):
            result_data[result_table_id] = {
                "output_count": result_table_info["output_total_count"],
                "warning_count": result_table_info.get("warning_count", 0),
                "has_error": has_error,
            }

        # 判断是否有告警信息需要展示
        if debug_error and debug_error["error_result_table_id"] in result_data:
            result_data[debug_error["error_result_table_id"]]["has_error"] = True

        # 将 result_data 转换为以 node_id 为 KEY 的对象
        node_data = {}
        for _rt_id, _rt_info in list(result_data.items()):
            if _rt_id in node.result_table_ids:
                node_data[node.node_id] = _rt_info
        return node_data

    def get_geog_area_code(self):
        return self.flow_debugger.flow.geog_area_codes[0]

    def clear(self):
        """
        清除工作
        """
        #
        debug_history_context = self.get_node_info(self.nodes[0].node_id)["context"]
        if "debug_id" in debug_history_context:
            debug_id = debug_history_context["debug_id"]
            api_params = {
                "debug_id": debug_id,
                "geog_area_code": self.context["geog_area_code"],
            }
            if self.debug_type == "stream":
                StreamHelper.stop_debug(**api_params)
            elif self.debug_type == "batch":
                BatchHelper.stop_debug(**api_params)
            elif self.debug_type == "model_app":
                ModelingHelper.stop_debug(**api_params)
