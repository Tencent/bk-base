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

from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.models import FlowInfo
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.storekit.storekit_helper import StorekitHelper


def step(func):
    def _wrap(*args, **kwargs):
        return func(*args, **kwargs)

    return _wrap


class BaseNodeTaskHandler(object):
    task_type = None

    def __init__(
        self,
        flow_task_handler,
        o_nodes,
        op_type,
        start_slightly=False,
        log_type="task_log",
    ):
        """
        初始化 task 对象
        @param flow_task_handler:
        @param o_nodes:
        @param op_type:
        @param log_type: 任务日志打印，默认task_log（task_execution_log，DB中）,
                        可选 file_log (及sys_flow.log)
        @param start_slightly: 若为重启过程，可标识作业是否需要硬启动
        """
        self.nodes = o_nodes
        self.flow_task_handler = flow_task_handler
        self.start_slightly = start_slightly
        self.log_type = log_type

        build_method = "build_%s_context" % op_type
        self.context = getattr(self, build_method)(flow_task_handler, o_nodes)

    def build_start_context(self, flow_task_handler, o_nodes):
        """
        @param o_flow_task {FlowTaskHandler}
        @param o_nodes {array.<NodeHandler>} 节点对象列表
        """
        raise NotImplementedError

    def build_stop_context(self, flow_task_handler, o_nodes):
        raise NotImplementedError

    def add_context(self, context):
        """
        添加额外 context
        """
        self.context.update(context)

    def start(self):
        try:
            self.log(
                Bilingual(ugettext_noop("启动（{display}）{node_type}节点")).format(
                    display=self.nodes_display,
                    node_type=Bilingual.from_lazy(self.nodes_type_display),
                )
            )
            self.start_before()
            self.start_inner()
            self.start_ok_callback()
        except Exception as e:
            logger.exception(_("节点异常"))
            self.start_fail_callback()
            raise e

    def stop(self):
        try:
            # 不加冒号调试时调用format会报错，猜测是format格式化的bug
            self.log(
                Bilingual(ugettext_noop("停止（{display:}）{node_type}节点")).format(
                    display=self.nodes_display,
                    node_type=Bilingual.from_lazy(self.nodes_type_display),
                )
            )
            self.stop_before()
            self.stop_inner()
            self.stop_ok_callback()
        except Exception as e:
            logger.exception(_("节点异常"))
            self.stop_fail_callback()
            raise e

    def start_inner(self):
        raise NotImplementedError

    def stop_inner(self):
        raise NotImplementedError

    def stop_before(self):
        self.update_nodes_status(FlowInfo.STATUS.STOPPING)

    def start_before(self):
        self.update_nodes_status(FlowInfo.STATUS.STARTING)
        self.default_storage_action("start")

    def start_ok_callback(self):
        self.update_nodes_status(FlowInfo.STATUS.RUNNING)

    def start_fail_callback(self):
        self.update_nodes_status(FlowInfo.STATUS.FAILURE)

    def stop_ok_callback(self):
        self.update_nodes_status(FlowInfo.STATUS.NO_START)

    def stop_fail_callback(self):
        self.update_nodes_status(FlowInfo.STATUS.FAILURE)

    def update_nodes_status(self, status):
        self.log(Bilingual(ugettext_noop("更新节点状态，状态={status}")).format(status=status))
        for _n in self.nodes:
            _n.set_status(status)

    def log(self, msg, level="INFO", time=None):
        if self.log_type == "task_log":
            self.flow_task_handler.log(msg, level=level, time=time)
        elif self.log_type == "file_log":
            if level == "INFO":
                logger.info(msg)
            elif level == "WARN":
                logger.warning(msg)
            elif level == "ERROR":
                logger.error(msg)
            elif level == "CRITICAL":
                logger.critical(msg)
            elif level == "EXCEPTION":
                logger.exception(msg)

    def default_storage_action(self, op_type):
        """
        离线节点启动前，视下游是否有可见的存储节点，决定是否对默认存储进行一系列的操作
        @param op_type:
        @return:
        """
        o_node = self.nodes[0]
        if o_node.is_batch_node and o_node.default_storage_type == "hdfs":
            # # 如果下游没有 HDFS 存储节点，当前离线类型节点需要调用 prepare 接口
            # if not filter(lambda _n: _n.node_type == NodeTypes.HDFS_STORAGE, o_node.to_nodes()):
            for result_table_id in o_node.result_table_ids:
                if op_type == "start":
                    self.log(Bilingual(ugettext_noop("检查 HDFS 存储")))
                    StorekitHelper.prepare(result_table_id, "hdfs")
                elif op_type == "stop":
                    pass

    @property
    def nodes_display(self):
        return ", ".join([_node.name for _node in self.nodes])

    @property
    def nodes_type_display(self):
        return self.nodes[0].get_node_type_display()


class ShipperNodeTask(BaseNodeTaskHandler):
    """
    该存储节点对应的抽象任务类构造简单，启动前无需建库建表，直接启动数据分发进程
    """

    def get_task_type(self):
        return self.nodes[0].storage_type

    def get_display_name(self):
        """
        任务类型名称
        @return:
        """
        display_name = Bilingual.from_lazy(self.nodes[0].get_node_type_display())
        return display_name

    def prepare(self):
        pass

    def build_start_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]
        return {
            "operator": flow_task_handler.operator,
            "result_table_id": result_table_id,
        }

    def build_stop_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]

        return {
            "operator": flow_task_handler.operator,
            "result_table_id": result_table_id,
        }

    def is_shipper_task(self):
        """
        HDFS 比较特殊，若上游节点包括离线节点，表明改节点数据是有离线计算直接写进去的，所以不用启分发任务
        @return:
        """
        return not (
            self.get_task_type().lower() == "hdfs"
            and [_n for _n in self.nodes[0].get_from_nodes_handler() if _n.is_batch_node]
        )

    def start_inner(self):
        task_type = self.get_task_type()
        self.prepare()
        if self.is_shipper_task():
            self.log(Bilingual(ugettext_noop("启动 {display_name} 分发进程")).format(display_name=self.get_display_name()))
            DatabusHelper.create_task(self.context["result_table_id"], [task_type.lower()])

    def stop_inner(self):
        task_type = self.get_task_type()
        if self.is_shipper_task():
            self.log(Bilingual(ugettext_noop("停止 {task_type} 分发进程")).format(task_type=task_type))
            DatabusHelper.stop_task(self.context["result_table_id"], [task_type.lower()])
