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
from datetime import datetime

from common.base_utils import model_to_dict
from common.bklanguage import BkLanguage
from common.exceptions import ApiResultError
from common.local import get_local_param, get_request_username
from django.utils import translation
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowError, FlowTaskError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowExecuteLog, FlowInfo, FlowInstance
from dataflow.flow.utils.decorators import ignore_exception
from dataflow.flow.utils.language import Bilingual
from dataflow.flow.utils.local import activate_request
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.send_message import get_common_message_title, send_message

from ...shared.datamanage.datamanage_helper import DatamanageHelper
from ..node_types import NodeTypes
from .deploy.node_tasks import BaseNodeTaskHandler, ShipperNodeTask
from .deploy.node_tasks.batch import BatchNodeTask
from .deploy.node_tasks.clickhouse_storage import ClickHouseNodeTask
from .deploy.node_tasks.druid_storage import DruidNodeTask
from .deploy.node_tasks.elasticsearch_storage import ESNodeTask
from .deploy.node_tasks.hdfs_storage import HdfsNodeTask
from .deploy.node_tasks.model import ModelNodeTask
from .deploy.node_tasks.model_app import ModelAppTask
from .deploy.node_tasks.mysql_storage import MysqlNodeTask
from .deploy.node_tasks.process_model import ProcessModelNodeTask
from .deploy.node_tasks.queue_storage import QueueNodeTask
from .deploy.node_tasks.stream import StreamNodeTask
from .deploy.node_tasks.stream_sdk import StreamSDKNodeTask
from .deploy.node_tasks.transform import TransformNodeTask
from .deploy.storage_task import StorageNodeTask


def step(func):
    def _wrap(*args, **kwargs):
        return func(*args, **kwargs)

    return _wrap


class SourceNodeTask(BaseNodeTaskHandler):
    def build_stop_context(self, flow_task_handler, o_nodes):
        pass

    def build_start_context(self, flow_task_handler, o_nodes):
        pass

    def start_inner(self):
        self.log(Bilingual(ugettext_noop("启动数据源分发进程")))

    def stop_inner(self):
        self.log(Bilingual(ugettext_noop("停止数据源分发进程")))


class TDWSourceNodeTask(BaseNodeTaskHandler):
    def build_start_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]
        return {"result_table_id": result_table_id}

    def build_stop_context(self, flow_task_handler, o_nodes):
        result_table_id = o_nodes[0].result_table_ids[0]
        return {"result_table_id": result_table_id}

    def start_inner(self):
        self.log(Bilingual(ugettext_noop("启动数据源分发进程")))
        self.log(Bilingual(ugettext_noop("检查TDW数据源是否同步")))
        result_table_id = self.context["result_table_id"]
        o_rt = ResultTable(result_table_id)
        # 若是 TDW 标准化表(非受控)
        if not o_rt.is_managed:
            # 检查是否已同步
            ResultTableHelper.get_result_table(result_table_id, check_usability=True, extra=True)
        else:
            # 对于 TDW 存储节点产生的 RT，需要保证 lz_id 是存在的，否则启动运行时会报错
            rt_tdw_conf = o_rt.get_extra("tdw")
            if not rt_tdw_conf:
                raise FlowTaskError(Bilingual(ugettext_noop("当前结果表({})TDW信息未配置")).format(result_table_id))
            if o_rt.processing_type == "storage":
                if not rt_tdw_conf["associated_lz_id"] or not rt_tdw_conf["associated_lz_id"]["import"]:
                    raise FlowTaskError(
                        Bilingual(ugettext_noop("当前结果表({})TDW信息不完整，请启动该结果表所在任务")).format(result_table_id)
                    )

    def stop_inner(self):
        self.log(Bilingual(ugettext_noop("停止数据源分发进程")))


class FlowTaskHandler(object):
    M_NODE_TASK = {
        NodeTypes.TSDB_STORAGE: StorageNodeTask,
        NodeTypes.DRUID_STORAGE: DruidNodeTask,
        NodeTypes.HERMES_STORAGE: StorageNodeTask,
        NodeTypes.QUEUE_STORAGE: QueueNodeTask,
        NodeTypes.CLICKHOUSE_STORAGE: ClickHouseNodeTask,
        NodeTypes.MYSQL_STORGAE: MysqlNodeTask,
        NodeTypes.HDFS_STORAGE: HdfsNodeTask,
        NodeTypes.BATCH: BatchNodeTask,
        NodeTypes.BATCHV2: BatchNodeTask,
        NodeTypes.DATA_MODEL_BATCH_INDICATOR: BatchNodeTask,
        NodeTypes.SPARK_STRUCTURED_STREAMING: StreamSDKNodeTask,
        NodeTypes.FLINK_STREAMING: StreamSDKNodeTask,
        NodeTypes.ES_STORAGE: ESNodeTask,
        NodeTypes.MODEL: ModelNodeTask,
        NodeTypes.PROCESS_MODEL: ProcessModelNodeTask,
        NodeTypes.MODEL_TS_CUSTOM: ProcessModelNodeTask,
        NodeTypes.MERGE_KAFKA: TransformNodeTask,
        NodeTypes.SPLIT_KAFKA: TransformNodeTask,
        NodeTypes.TDW_BATCH: BatchNodeTask,
        NodeTypes.TDW_JAR_BATCH: BatchNodeTask,
        NodeTypes.QUEUE_PULSAR_STORAGE: ShipperNodeTask,
        NodeTypes.IGNITE_STORAGE: StorageNodeTask,
        NodeTypes.MODEL_APP: ModelAppTask,
    }
    for node_type in NodeTypes.SOURCE_CATEGORY:
        if node_type in NodeTypes.TDW_SOURCE:
            M_NODE_TASK[node_type] = TDWSourceNodeTask
        else:
            M_NODE_TASK[node_type] = SourceNodeTask

    @classmethod
    def create(cls, flow_id, operator, action, context=None, version=None):
        kwargs = {
            "flow_id": flow_id,
            "created_by": operator,
            "action": action,
        }
        if context is not None:
            kwargs["context"] = json.dumps(context)
        if version is not None:
            kwargs["version"] = version

        o_flow_task = FlowExecuteLog.objects.create(**kwargs)
        return cls(o_flow_task.id)

    def __init__(self, id):
        self.id = id
        self.flow_task = FlowExecuteLog.objects.get(id=id)
        self._operator = self.flow_task.created_by
        # 缓存 FlowHandler 对象，FlowInstance 对象
        self._flow = None
        self._instance = None

        # 上一次启动的版本
        self.pre_version = None

        # 如果是重启过程，记录需要重启的节点
        self.nodes_to_restart = {}

        # 如果是重启过程，记录需要"轻量级重启"的节点
        self.nodes_to_restart_slightly = {}

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, operator):
        self._operator = operator

    def execute(self):
        # 重新初始化线程变量
        activate_request(self.flow_task.created_by)
        DEPLOY_FUNS_DISPATCH = {
            FlowExecuteLog.ACTION_TYPES.START: self.start,
            FlowExecuteLog.ACTION_TYPES.STOP: self.stop,
            FlowExecuteLog.ACTION_TYPES.RESTART: self.restart,
        }

        try:
            self.lock()
            self.flow_task.set_status(FlowExecuteLog.STATUS.RUNNING)
            DEPLOY_FUNS_DISPATCH[self.action_type]()

            self.ok_callback(Bilingual(ugettext_noop("无")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
            self.log(
                Bilingual(ugettext_noop("部署过程异常，errors=({})")).format(e.format_msg()),
                level="ERROR",
            )
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
            self.log(
                Bilingual(ugettext_noop("部署过程出现非预期异常，errors={}")).format(str(e).replace("\n", " ")),
                level="ERROR",
            )
        finally:
            self.unlock()

    def execute_stream(self):
        # 重新初始化线程变量
        activate_request(self.flow_task.created_by)
        try:
            self.lock()
            # 停止实时任务
            node_tasks = self.build_stop_stream_node_tasks()
            for _nt in node_tasks:
                _nt.stop()
            # 启动实时任务
            node_tasks = self.build_start_stream_node_tasks()
            for _nt in node_tasks:
                _nt.start()

            self.ok_callback(Bilingual(ugettext_noop("重启实时节点成功")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
        finally:
            self.unlock()

    def execute_stream_stop(self):
        # 重新初始化线程变量
        activate_request(self.flow_task.created_by)
        try:
            self.lock()
            # 停止实时任务
            node_tasks = self.build_stop_stream_node_tasks()
            for _nt in node_tasks:
                _nt.stop()

            self.ok_callback(Bilingual(ugettext_noop("停止实时节点成功")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
        finally:
            self.unlock()

    def execute_stream_start(self):
        # 重新初始化线程变量
        activate_request(self.flow_task.created_by)
        try:
            self.lock()
            # 启动实时任务
            node_tasks = self.build_start_stream_node_tasks()
            for _nt in node_tasks:
                _nt.start()

            self.ok_callback(Bilingual(ugettext_noop("启动实时节点成功")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
        finally:
            self.unlock()

    @ignore_exception
    def send_message(self, message):
        """
        发送消息通知，根据用户语言发送相关信息
        对于详细信息，若有报错，信息用英文(celery任务默认语言为英文)，不报错则根据用户语言发送详细信息
        """
        flow_task = self.flow_task
        operator = flow_task.created_by
        # 若获取不到用户的语言配置(如内部版)，采用用户访问时的语言环境
        language = BkLanguage.get_user_language(operator, default=get_local_param("request_language_code"))
        translation.activate(language)
        receivers = [operator]
        flow_name = FlowInfo.objects.get(flow_id=flow_task.flow_id).flow_name
        title = get_common_message_title()
        content = _(
            "通知类型: {message_type}<br>"
            "任务名称: {flow_name}<br>"
            "部署类型: {op_type}<br>"
            "部署状态: {result}<br>"
            "详细信息: <br>{message}"
        ).format(
            message_type=_("任务部署"),
            flow_name=flow_name,
            op_type=dict(FlowExecuteLog.ACTION_TYPES_CHOICES).get(flow_task.action, flow_task.action),
            result=dict(FlowExecuteLog.STATUS_CHOICES).get(flow_task.status, flow_task.status),
            message=message,
        )
        send_message(receivers, title, content)
        translation.activate("en")

    def ok_callback(self, message):
        self.flow_task.set_status(FlowExecuteLog.STATUS.SUCCESS)
        self.flow.sync_status()
        self.send_message(message)

    def fail_callback(self, message):
        self.flow_task.set_status(FlowExecuteLog.STATUS.FAILURE)
        self.flow.sync_status()
        self.send_message(message)

    def build_start_node_tasks(self):
        """
        组装启动节点任务列表
        """
        tasks = []
        nodes = self.flow.get_ordered_nodes()

        stream_tasks = []
        stream_index = -1
        for _n in nodes:
            start_slightly = False
            if (
                _n.node_type in list(self.M_NODE_TASK.keys())
                and self.restart_check_node(_n)
                or _n.node_id in self.nodes_to_restart_slightly
            ):
                if _n.node_id in self.nodes_to_restart_slightly:
                    start_slightly = True
                _task = self.M_NODE_TASK[_n.node_type](self, [_n], "start", start_slightly)
                tasks.append(_task)

            # 实时节点打包一起启动，实时任务只需要生成一个
            if _n.node_type in [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ]:
                stream_tasks.append(_n)
                if stream_index == -1:
                    stream_index = len(tasks)

        if stream_tasks and (
            self.restart_check_node(stream_tasks[0]) or stream_tasks[0].node_id in self.nodes_to_restart_slightly
        ):
            if stream_tasks[0].node_id in self.nodes_to_restart_slightly:
                start_slightly = True
            _task = StreamNodeTask(self, stream_tasks, "start", start_slightly)
            tasks.insert(stream_index, _task)

        return tasks

    def build_start_stream_node_tasks(self):
        """
        只组装启动实时节点
        """
        tasks = []
        nodes = self.flow.get_ordered_nodes()

        stream_tasks = []
        for _n in nodes:
            start_slightly = False
            # 实时节点打包一起启动，实时任务只需要生成一个
            if _n.node_type in [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ]:
                # 实时节点打包一起启动，实时任务只需要生成一个
                if _n.node_type in [
                    NodeTypes.STREAM,
                    NodeTypes.DATA_MODEL_APP,
                    NodeTypes.DATA_MODEL_STREAM_INDICATOR,
                ]:
                    stream_tasks.append(_n)

        if stream_tasks and (
            self.restart_check_node(stream_tasks[0]) or stream_tasks[0].node_id in self.nodes_to_restart_slightly
        ):
            if stream_tasks[0].node_id in self.nodes_to_restart_slightly:
                start_slightly = True
            _task = StreamNodeTask(self, stream_tasks, "start", start_slightly)
            tasks.append(_task)

        return tasks

    def is_restart(self):
        return self.action_type == FlowExecuteLog.ACTION_TYPES.RESTART

    def restart_check_node(self, node):
        """
        返回 True 表示节点所在的任务需要启动或停止，False 则表示不需要启动或停止
        @param node:
        @return:
        """
        if (
            not self.is_restart()
            or node.node_id in self.nodes_to_restart
            or (
                node.is_belong_to_topology_job
                and node.node_type in [_n.node_type for _n in list(self.nodes_to_restart.values())]
            )
        ):
            return True
        else:
            return False

    def build_stop_node_tasks(self):
        """
        组装停止节点任务列表
        """
        tasks = []
        nodes = self.flow.get_ordered_nodes(reverse=True, isolated_excluded=False)
        stream_tasks = []
        stream_index = -1
        for _n in nodes:
            # 可能需要停止的节点:
            #   1. 运行中、停止失败、正在停止状态的节点
            #   2. 在重启操作中，被鉴定为需要重启的节点，即节点存在于 nodes_to_restart 中，且当前至少存在一个正在运行的关联作业
            #       * 当前画布存在一个实时作业，增加新节点，需要停止
            #       * 当前画布不存在一个实时作业，增加新节点，不需要停止
            if _n.status not in [
                FlowInfo.STATUS.RUNNING,
                FlowInfo.STATUS.FAILURE,
                FlowInfo.STATUS.STOPPING,
            ]:
                # 该节点不需要停止
                _n.set_status(FlowInfo.STATUS.NO_START)
                continue

            if _n.node_type in list(self.M_NODE_TASK.keys()) and self.restart_check_node(_n):
                _task = self.M_NODE_TASK[_n.node_type](self, [_n], "stop")
                tasks.append(_task)

            # 实时节点打包一起启动，实时任务只需要生成一个
            if _n.node_type in [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ]:
                stream_tasks.append(_n)
                if stream_index == -1:
                    stream_index = len(tasks)

        if stream_tasks and self.restart_check_node(stream_tasks[0]):
            _task = StreamNodeTask(self, stream_tasks, "stop")
            tasks.insert(stream_index, _task)

        return tasks

    def build_stop_stream_node_tasks(self):
        """
        组装停止实时节点任务列表
        """
        tasks = []
        nodes = self.flow.get_ordered_nodes(reverse=True)
        stream_tasks = []
        for _n in nodes:
            # 实时节点打包一起启动，实时任务只需要生成一个
            if _n.node_type in [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ]:
                stream_tasks.append(_n)

        if stream_tasks and self.restart_check_node(stream_tasks[0]):
            _task = StreamNodeTask(self, stream_tasks, "stop")
            tasks.append(_task)

        return tasks

    def lock(self):
        self.log(Bilingual(ugettext_noop("锁定任务")))
        self.flow.lock("启停任务中")

    def unlock(self):
        self.log(Bilingual(ugettext_noop("解锁任务")))
        self.flow.unlock()

    def start(self, max_progress=100.0):
        self.log(Bilingual(ugettext_noop("******************** 开始检查任务 ********************")))
        self.update_instance()
        self.check_before_start(self.progress + (max_progress - self.progress) / 2)
        self.log(Bilingual(ugettext_noop("******************** 成功检查任务 ********************")))

        self.log(Bilingual(ugettext_noop("******************** 开始启动任务 ********************")))
        self.flow.status = FlowInfo.STATUS.STARTING
        node_tasks = self.build_start_node_tasks()
        _current_range_progress = max_progress - self.progress
        step_progress = 0
        if _current_range_progress > 0 and node_tasks:
            step_progress = 1.0 * _current_range_progress / len(node_tasks)
        for _nt in node_tasks:
            _nt.start()
            self.progress = self.progress + step_progress
        self.log(Bilingual(ugettext_noop("******************** 成功启动任务 ********************")))
        self.progress = max_progress

    def stop_after(self):
        # 检查是否需要停止 TDW 存量数据表对应数据源的检查任务
        for _n in self.flow.nodes:
            # 当前节点是 TDW 相关节点并且下游节点都停止了(不存在没停止的)
            if _n.node_type in [NodeTypes.TDW_SOURCE, NodeTypes.TDW_STORAGE] and not list(
                filter(
                    lambda _n: _n.status != FlowInfo.STATUS.NO_START,
                    _n.get_to_nodes_handler(),
                )
            ):
                result_table_id = _n.result_table_ids[0]
                o_rt = ResultTable(result_table_id)
                # RT是受控 TDW 数据源、且 processing_type 为 batch(即TDW离线产生的表)，无需处理
                if _n.node_type == NodeTypes.TDW_SOURCE and o_rt.is_managed and o_rt.processing_type == "batch":
                    continue
                related_running_nodes = self.flow.related_running_nodes(
                    result_table_id,
                    [
                        NodeTypes.TDW_SOURCE,
                        NodeTypes.TDW_STORAGE,
                    ],
                )
                should_stop_source = True
                for related_running_node in related_running_nodes:
                    if NODE_FACTORY.get_node_handler(related_running_node).get_to_nodes_handler():
                        should_stop_source = False
                        break
                # 若该 RT 未被其它正在运行的 Flow 引用，或即使引用了但下游未接其它节点，应该调用 stop_source
                if should_stop_source:
                    BatchHelper.stop_source(result_table_id)

    def stop(self, max_progress=100.0):
        """
        @param max_progress: 当前阶段完成后所能达到的最大临界值
        @return:
        """
        self.log(Bilingual(ugettext_noop("******************** 开始停止任务 ********************")))
        self.flow.status = FlowInfo.STATUS.STOPPING
        node_tasks = self.build_stop_node_tasks()
        _current_range_progress = max_progress - self.progress
        step_progress = 0
        if _current_range_progress > 0 and node_tasks:
            step_progress = 1.0 * _current_range_progress / len(node_tasks)
        for _nt in node_tasks:
            _nt.stop()
            self.progress = self.progress + step_progress
        self.stop_after()
        self.log(Bilingual(ugettext_noop("******************** 成功停止任务 ********************")))
        self.progress = max_progress

    def build_nodes_to_restart(self):
        """
        将所有需要重启的节点检查出来，规则为:
            1. 与运行中的版本相比(不能存在运行中节点删除的情况)为新版本的节点
                1. 新增的节点/连线的所有下游节点(除非其一级下游是支持轻量级重启的节点，并且不向下追溯更下游节点)
                2. 支持轻量级重启的节点及其自身除外，当前仅做了修改的节点/连线及其所有下游节点
            2. 状态不正常/未启动的节点(需要启动，不一定需要停止)
            3. 修改了数据消费模式、计算集群等
            * 除此之外，若非 processing 节点符合上述条件，其所有上游节点中也需要重启(上游合流节点若不符合上述条件，就不需要重启)
        """
        (
            self.nodes_to_restart,
            self.nodes_to_restart_slightly,
        ) = self.flow.build_nodes_to_restart(self.context)

    def restart(self):
        self.build_nodes_to_restart()
        # 对于重启操作，需要停止的节点必然需要重启(重启的节点还包括不处于 RUNNING 状态的节点，即未启动或未正常启动的节点)
        self.stop(max_progress=50.0)
        self.start()

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

    def check_before_start(self, max_progress):
        """
        启动前按顺序保存每个节点，确保能成功才能启动flow
        """
        o_flow = self.flow

        # 检查是否存在回路
        o_flow.check_circle()

        # 校验数据模型节点
        is_exist_data_model_app = False
        stream_nodes = o_flow.get_stream_nodes()
        for node in stream_nodes:
            if node.node_type == NodeTypes.DATA_MODEL_APP:
                is_exist_data_model_app = True
                break
        if is_exist_data_model_app:
            res = DatamanageHelper.check_data_model_instance({"flow_id": o_flow.flow_id})
            if not res:
                raise ApiResultError(message=_("检查【数据模型应用】节点返回为空"))
            if not res.is_success():
                raise ApiResultError(Bilingual(ugettext_noop("检查【数据模型应用】节点（{output}）")).format(output=res.message))

        # 按从数据源节点获取所有待启动的节点
        ordered_nodes = o_flow.check_isolated_nodes()
        nodes_to_update = self.build_nodes_to_update(ordered_nodes)
        _current_range_progress = max_progress - self.progress
        step_progress = 0
        if _current_range_progress > 0 and len(ordered_nodes) > 0:
            step_progress = 1.0 * _current_range_progress / len(ordered_nodes)
        for index, node in enumerate(ordered_nodes):
            if node.node_id not in nodes_to_update:
                continue
            node = NODE_FACTORY.get_node_handler(node_id=node.node_id)
            self.log(
                Bilingual(ugettext_noop("检查{display}节点（{output}）")).format(
                    output=node.name,
                    display=Bilingual.from_lazy(node.get_node_type_display()),
                )
            )
            """
            实时和离线计算节点，需要更新 processing
            1. udf 重新发布后，启动节点需要重新保存以更新 processing 信息
            2. 其它计算节点元数据发生变化
            """
            if node.node_type in NodeTypes.PROCESSING_CATEGORY:
                from_node_ids = node.from_node_ids
                frontend_info = node.frontend_info
                prev_node = model_to_dict(node.node_info)
                prev_node["result_table_ids"] = node.result_table_ids
                form_data = node.clean_config(json.loads(prev_node["node_config"]), False)
                operator = get_request_username()
                build_form_data = node.update_node_info(operator, from_node_ids, form_data, frontend_info=frontend_info)
                after_node = model_to_dict(node.node_info)
                node.update_after(
                    operator,
                    from_node_ids,
                    build_form_data,
                    prev=prev_node,
                    after=after_node,
                )

            self.progress = self.progress + step_progress
        self.progress = max_progress

    def log(self, msg, level="INFO", time=None):
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.flow_task.add_log(msg, level=level, time=_time, progress=self.progress)

    def update_instance(self):
        """
        更新 DataFlow 实例的版本，并且记录下上一次启动版本
        """
        if self.instance is None:
            FlowInstance.objects.create(flow_id=self.flow_id, running_version=self.version)
        else:
            self.pre_version = self.instance.running_version
            self.instance.running_version = self.version
            self.instance.save()

    @property
    def flow(self):
        if self._flow is None:
            self._flow = FlowHandler(flow_id=self.flow_id)
        return self._flow

    @property
    def flow_id(self):
        return self.flow_task.flow_id

    @property
    def action_type(self):
        return self.flow_task.action

    @property
    def version(self):
        return self.flow_task.version

    @property
    def context(self):
        _context = self.flow_task.get_context()
        return {} if _context is None else _context

    def add_context(self, context):
        """
        添加额外 context
        """
        _context = self.context
        _context.update(context)
        self.flow_task.save_context(_context)

    @property
    def progress(self):
        # 默认最小进度10，停止作业时取的就是该初始值
        return self.context.get("progress", 10)

    @progress.setter
    def progress(self, progress):
        self.add_context({"progress": progress})

    @property
    def instance(self):
        """
        DataFlow 对应的作业实例
        """
        if self._instance is None:
            try:
                self._instance = FlowInstance.objects.get(flow_id=self.flow_id)
            except FlowInstance.DoesNotExist:
                pass
        return self._instance


"""
    using for add partition only
"""


class PlainFlowTaskHandler(object):
    M_NODE_TASK = FlowTaskHandler.M_NODE_TASK

    def __init__(self, flow_id, nodes_need_operation, context, operator):
        self.flow_id = flow_id
        self.nodes_need_operation = nodes_need_operation
        self.context = context
        self._flow = None
        self._operator = operator

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, operator):
        self._operator = operator

    def execute_stream_stop(self):
        # 重新初始化线程变量
        activate_request("admin")
        try:
            self.lock()
            # 停止实时任务
            node_tasks = self.build_op_stream_node_tasks("stop")
            for _nt in node_tasks:
                _nt.stop()

            self.ok_callback(Bilingual(ugettext_noop("停止实时/算法节点成功")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
            raise e
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
            raise e
        finally:
            self.unlock()

    def execute_stream_start(self):
        # 重新初始化线程变量
        activate_request("admin")
        try:
            self.lock()
            # 启动实时任务
            node_tasks = self.build_op_stream_node_tasks("start")
            for _nt in node_tasks:
                _nt.start()

            self.ok_callback(Bilingual(ugettext_noop("启动实时/算法节点成功")))
        except FlowError as e:
            logger.exception(_("自定义异常"))
            self.fail_callback(e.format_msg())
            raise e
        except Exception as e:
            logger.exception(_("非预期异常"))
            self.fail_callback(str(e))
            raise e
        finally:
            self.unlock()

    def build_op_stream_node_tasks(self, action):
        """
        组装停止实时节点任务列表
        """
        tasks = []
        nodes = self.nodes_need_operation
        stream_tasks = []
        stream_index = -1
        for _n in nodes:
            if _n.node_type in list(self.M_NODE_TASK.keys()):
                _task = self.M_NODE_TASK[_n.node_type](self, [_n], action, log_type="task_log")
                tasks.append(_task)

            # 实时节点打包一起启动，实时任务只需要生成一个
            if _n.node_type in [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ]:
                stream_tasks.append(_n)
                if stream_index == -1:
                    stream_index = len(tasks)

        if stream_tasks:
            _task = StreamNodeTask(self, stream_tasks, action, log_type="task_log")
            tasks.insert(stream_index, _task)
        return tasks

    @property
    def flow(self):
        if self._flow is None:
            self._flow = FlowHandler(flow_id=self.flow_id)
        return self._flow

    def lock(self):
        # self.log(Bilingual(ugettext_noop(u"锁定任务")))
        self.flow.lock("启停任务中")

    def unlock(self):
        # self.log(Bilingual(ugettext_noop(u"解锁任务")))
        self.flow.unlock()

    def ok_callback(self, message):
        self.log(message)

    def fail_callback(self, message):
        self.log(message, "ERROR")

    def log(self, msg, level="INFO", time=None):
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
