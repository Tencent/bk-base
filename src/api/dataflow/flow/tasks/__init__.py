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

import hashlib

from celery import shared_task
from celery.signals import celeryd_init
from common.bklanguage import BkLanguage
from common.local import set_local_param
from django.conf import settings
from django.utils import translation
from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import (
    EnvironmentError,
    FlowCustomCalculateError,
    FlowDebuggerNotAllowed,
    FlowDeployNotAllowed,
    FlowTaskNotAllowed,
    NodeValidError,
)
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.modeling_handler import ModelingDatalabTaskHandler
from dataflow.flow.handlers.nodes.base_node.base_storage_node import StorageNode
from dataflow.flow.handlers.signals import generate_version
from dataflow.flow.models import FlowDebugLog, FlowExecuteLog, FlowInfo, FlowNodeRelation
from dataflow.flow.tasks.udf.debug_udf import UdfBaseHandler, UdfDebuggerHandler
from dataflow.modeling.exceptions.comp_exceptions import SQLSubmitError
from dataflow.shared.lock import Lock
from dataflow.shared.log import flow_logger

from ..handlers.node_utils import NodeUtils
from ..handlers.nodes.base_node.base_source_node import SourceNode
from ..node_types import NodeTypes
from .custom_calculate_flow import FlowCustomCalculateHandler
from .debug_flow import FlowDebuggerHandler
from .deploy_flow import FlowTaskHandler, PlainFlowTaskHandler


def submit_task(task, task_id, operator, delay=True):
    """
    若调用失败，优化抛出的异常信息
    @param task:
    @param task_id:
    @param operator:
    @param delay:
    @return:
    """
    try:
        if delay:
            return task.delay(task_id, BkLanguage.current_language())
        else:
            return task(task_id, BkLanguage.current_language())
    except Exception as e:
        flow_logger.error(e)
        raise EnvironmentError(_("系统环境出现异常，请联系管理员处理."))


def _check_flow_nodes_to_restart(o_flow, context):
    """
    重启前判断任务是否需要重启，画布或启动方式(集群组、数据处理模式)未修改
    @return:
    """
    nodes_to_restart, nodes_to_restart_slightly = o_flow.build_nodes_to_restart(context)
    if not nodes_to_restart and not nodes_to_restart_slightly:
        raise FlowDeployNotAllowed(_("画布未进行更改，无需重启"))


def _check_flow_topology_to_start(o_flow, operator):
    """
    检查 flow 拓扑是否合理，可启动
    @param o_flow:
    @param operator:
    @return:
    """
    if operator and operator in getattr(settings, "DEVELOPERS", []):
        return

    if not o_flow.nodes:
        raise FlowDeployNotAllowed(_("空任务不能启动"))

    if all([isinstance(_n, SourceNode) for _n in o_flow.nodes]):
        raise FlowDeployNotAllowed(_("只有源数据节点，不能启动"))

    # 任务有 TDW 离线存储也是允许启动的
    if not [
        _n
        for _n in o_flow.nodes
        if isinstance(_n, StorageNode)
        or _n.node_type
        in [
            NodeTypes.MERGE_KAFKA,
            NodeTypes.SPLIT_KAFKA,
            NodeTypes.TDW_JAR_BATCH,
            NodeTypes.TDW_BATCH,
        ]
    ]:
        raise FlowDeployNotAllowed(_("没有存储节点，不能启动"))

    # 1. 算法节点特殊处理，需要接显式的存储节点才可以启动
    # 2. 实时节点需要接其它节点才可以启动(如实时、存储、kafka固化节点等)
    for _n in filter(
        lambda _n: _n.node_type
        in [
            NodeTypes.STREAM,
            NodeTypes.DATA_MODEL_APP,
            NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            NodeTypes.MODEL,
            NodeTypes.PROCESS_MODEL,
            NodeTypes.MODEL_TS_CUSTOM,
            NodeTypes.SPARK_STRUCTURED_STREAMING,
            NodeTypes.FLINK_STREAMING,
        ],
        o_flow.nodes,
    ):
        to_nodes = _n.get_to_nodes_handler()
        # 若算法节点的下游节点都不是存储节点，不允许启动
        if _n.node_type == NodeTypes.MODEL and not [
            _u_t for _u_t in to_nodes if _u_t.node_type in NodeTypes.STORAGE_CATEGORY
        ]:
            raise FlowDeployNotAllowed(_("模型节点(%s)没有下接存储节点，不能启动") % _n.name)
        elif _n.node_type in [
            NodeTypes.STREAM,
            NodeTypes.DATA_MODEL_APP,
            NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            NodeTypes.PROCESS_MODEL,
            NodeTypes.MODEL_TS_CUSTOM,
            NodeTypes.SPARK_STRUCTURED_STREAMING,
            NodeTypes.FLINK_STREAMING,
        ]:
            # 判断当前节点下游节点是否使用了当前节点的所有输出
            all_from_result_table_ids = set()
            for to_node in to_nodes:
                all_from_result_table_ids.update(to_node.from_result_table_ids)
            result_table_ids_not_used = set(_n.result_table_ids) - all_from_result_table_ids
            if result_table_ids_not_used:
                raise FlowDeployNotAllowed(
                    _("%(display)s节点(%(node_name)s)的数据输出(%(result_table_ids)s)未被使用，不可启动")
                    % {
                        "display": _n.get_node_type_display(),
                        "result_table_ids": ", ".join(result_table_ids_not_used),
                        "node_name": _n.name,
                    }
                )


def start_flow(flow_id, operator, consuming_mode="continue", cluster_group=None):
    """
    启动任务
    @param flow_id:
    @param operator:
    @param consuming_mode: 指定从最新/继续/最早位置消费
    @param cluster_group: 计算集群组
    @return:
    """
    o_flow = FlowHandler(flow_id=flow_id)
    if cluster_group:
        o_flow.perm_check_cluster_group(cluster_group)
    o_flow.perm_check_tdw_app_group()

    _check_flow_topology_to_start(o_flow, operator)
    o_flow.check_locked()

    if o_flow.status in [FlowInfo.STATUS.RUNNING, FlowInfo.STATUS.STARTING]:
        raise FlowTaskNotAllowed(_("处于运行状态，不能启动"))

    context = {
        "consuming_mode": consuming_mode,
        "cluster_group": cluster_group,
        "progress": 10.0,  # 启动作业初始进度值
    }
    # 确保有版本号，避免update_instance触发running_version写入为null的异常
    running_version = o_flow.version
    if not o_flow.version:
        running_version = generate_version(operator, o_flow.flow_id)
    task = FlowTaskHandler.create(
        flow_id=flow_id,
        operator=operator,
        action="start",
        context=context,
        version=running_version,
    )
    task_id = task.id
    submit_task(execute_flow_task, task_id, operator)
    return task_id


def stop_flow(flow_id, operator):
    o_flow = FlowHandler(flow_id=flow_id)
    o_flow.perm_check_tdw_app_group()

    o_flow.check_locked()

    if o_flow.status in [FlowInfo.STATUS.NO_START]:
        raise FlowTaskNotAllowed(_("处于未运行状态，不能停止"))

    context = {"progress": 10.0}  # 停止作业初始进度值
    task = FlowTaskHandler.create(flow_id=flow_id, operator=operator, action="stop", context=context)
    task_id = task.id
    submit_task(execute_flow_task, task_id, operator)
    return task_id


def restart_flow(flow_id, operator, consuming_mode="continue", cluster_group=None):
    """
    重启任务
    @param flow_id:
    @param operator:
    @param consuming_mode: 指定从最新/继续/最早位置消费
    @param cluster_group:
    @return:
    """
    o_flow = FlowHandler(flow_id=flow_id)
    if cluster_group:
        o_flow.perm_check_cluster_group(cluster_group)
    o_flow.perm_check_tdw_app_group()

    _check_flow_topology_to_start(o_flow, operator)
    o_flow.check_locked()

    if o_flow.status in [FlowInfo.STATUS.NO_START]:
        raise FlowTaskNotAllowed(_("处于未运行状态，不能重启"))

    context = {
        "consuming_mode": consuming_mode,
        "cluster_group": cluster_group,
        "progress": 10.0,  # 重启作业初始进度值
    }
    _check_flow_nodes_to_restart(o_flow, context)
    task = FlowTaskHandler.create(
        flow_id=flow_id,
        operator=operator,
        action="restart",
        context=context,
        version=o_flow.version,
    )
    task_id = task.id
    submit_task(execute_flow_task, task_id, operator)
    return task_id


def op_with_all_flow_and_nodes(result_table_id, action, token, operator, downstream_node_types):
    """
    扩分区时，对result_table_id相关的计算任务进行操作，可能涉及到N个flow下的计算任务
    :param result_table_id: result_table_id
    :param action: start/stop
    :param token: token
    :param operator: operator
    :param downstream_node_types: 下游操作的节点类型集合
    :return: 操作成功/失败的flow列表 {"succ_op_flow_list": xxx, "fail_op_flow_list": yyy}
    """
    token_str = hashlib.new("md5", result_table_id).hexdigest()
    if token_str != token:
        raise Exception("当前请求result_table_id对应的token有误%s")

    flow_op_nodes_map = {}

    # 查询当前 node_id(正在运行的) 一个数据源result_table_id可能被多个stream_source使用;只查询正在运行的节点
    flow_relation_list = FlowNodeRelation.objects.filter(
        result_table_id=result_table_id, node_type=NodeTypes.STREAM_SOURCE
    )
    running_flow_list = []
    for flow_relation in flow_relation_list:
        node_id = flow_relation.node_id
        flow_id = flow_relation.flow_id
        o_flow = FlowHandler(flow_id=flow_id)
        # 只操作正则运行的任务
        if o_flow.status == FlowInfo.STATUS.RUNNING:
            running_flow_list.append(flow_relation)
            # 查询downstream_node_types类型的节点
            to_nodes = NodeUtils.get_to_nodes_handler_by_id(node_id)
            nodes_need_operation = []
            for one_node in to_nodes:
                if one_node.node_type in downstream_node_types:
                    nodes_need_operation.append(one_node)

            if flow_id not in flow_op_nodes_map:
                flow_op_nodes_map[flow_id] = nodes_need_operation

    flow_logger.info(
        "以当前结果表(%s)作为source且正在运行的flow列表(%s)"
        % (
            result_table_id,
            ",".join(["{}-{}".format(i.project_id, i.flow_id) for i in running_flow_list]),
        )
    )

    succ_op_flow_list = []
    fail_op_flow_list = []
    for flow_id in flow_op_nodes_map:
        nodes_need_operation = flow_op_nodes_map[flow_id]
        op_res = op_flow_with_nodes(flow_id, nodes_need_operation, action, operator, "continue", True)
        if op_res:
            flow_logger.info("操作({}) flow({})中的({})类型节点成功".format(action, flow_id, ",".join(downstream_node_types)))
            succ_op_flow_list.append(flow_id)
        else:
            fail_op_flow_list.append(flow_id)
            flow_logger.info("操作({}) flow({})中的({})类型节点失败".format(action, flow_id, ",".join(downstream_node_types)))
    flow_logger.info(
        "result_table_id(%s)下游的(%s)类型的任务，操作(%s)成功flow列表(%s)，失败flow列表(%s)"
        % (
            result_table_id,
            ",".join(downstream_node_types),
            action,
            ",".join("%s" % fid for fid in succ_op_flow_list),
            ",".join("%s" % fid for fid in fail_op_flow_list),
        )
    )

    return {
        "succ_op_flow_list": succ_op_flow_list,
        "fail_op_flow_list": fail_op_flow_list,
    }


def op_with_flow_task_by_id(flow_id, action, op_type, operator):
    """
    迁移flow时，重启该flow下指定类型的计算节点
    :param flow_id:
    :param action:start/stop
    :param op_type:realtime/offline
    :param operator:
    :return:
    """
    if op_type not in NodeTypes.PROCESSING_CATEGORY:
        raise NodeValidError(_("操作非法的节点类型,合法的类型为:%s" % ",".join(NodeTypes.PROCESSING_CATEGORY)))
    if action not in ["start", "stop"]:
        raise NodeValidError(_("非法的action,合法范action为:start/stop"))
    o_flow = FlowHandler(flow_id=flow_id)
    # 当前需要操作的集群
    op_node_handlers = o_flow.nodes
    nodes_need_operation = []
    for node_handler in op_node_handlers:
        if op_type == node_handler.node_type:
            nodes_need_operation.append(node_handler)
    if not nodes_need_operation:
        raise NodeValidError(_("待操作的flow任务无节点类型:%s" % op_type))
    op_res = op_flow_with_nodes(flow_id, nodes_need_operation, action, operator, "continue", True)
    if op_res:
        flow_logger.info("操作({}) flow({})中的实时计算节点成功".format(action, flow_id))
        return True
    else:

        flow_logger.info("操作({}) flow({})中的实时计算节点失败".format(action, flow_id))
        return False


def op_flow_with_nodes(flow_id, nodes_need_op, action, operator, consuming_mode="continue", check_lock=True):
    """
    启动任务(仅某个特定flow下面的实时/算法任务)
    @param flow_id: flow_id
    @param nodes_need_op: 需要操作的节点列表
    @param action: start/stop
    @param operator: operator
    @param consuming_mode: 指定从最新/继续/最早位置消费
    @return:
    """
    o_flow = FlowHandler(flow_id=flow_id)

    # 循环检查状态次数120（每次间隔5秒，总共检查约10分钟）
    total_check_cnt = 120
    if check_lock:
        import time

        i = 0
        while o_flow.is_locked:
            time.sleep(5)
            i = i + 1
            if i >= total_check_cnt:
                break

        o_flow.check_locked()

    if o_flow.status in [FlowInfo.STATUS.NO_START]:
        raise FlowTaskNotAllowed(_("处于未运行状态，不能重启"))

    context = {"consuming_mode": consuming_mode}
    task = PlainFlowTaskHandler(
        flow_id=flow_id,
        nodes_need_operation=nodes_need_op,
        context=context,
        operator=operator,
    )
    if action == "stop":
        try:
            task.execute_stream_stop()
        except Exception as ex:
            node_ids = [str(i.node_id) for i in nodes_need_op]
            flow_logger.warning("flow({}) nodes({}) stop ex({})".format(flow_id, ", ".join(node_ids), ex))
            return None
        return task.flow_id
    elif action == "start":
        try:
            task.execute_stream_start()
        except Exception as ex:
            node_ids = [str(i.node_id) for i in nodes_need_op]
            flow_logger.warning("flow({}) nodes({}) start ex({})".format(flow_id, ", ".join(node_ids), ex))
            return None
        return task.flow_id


def start_debugger(flow_id, operator, delay=True):
    """
    调试任务
    """
    o_flow = FlowHandler(flow_id=flow_id)
    if not o_flow.nodes:
        raise FlowDebuggerNotAllowed(_("空任务不能调试"))

    o_flow.check_locked()

    # 中止同一个 Flow 的调试任务
    debuggers = FlowDebugLog.objects.filter(flow_id=flow_id).exclude(status__in=FlowDebugLog.DONE_STATUS_SET)

    for _debugger in debuggers:
        terminate_debugger(_debugger.id, operator)

    task = FlowDebuggerHandler.create(flow_id=flow_id, operator=operator, version=o_flow.version)

    debugger_id = task.debugger_id
    submit_task(execute_flow_debugger, debugger_id, operator, delay)
    return debugger_id


def terminate_debugger(debugger_id, operator):
    """
    中止调试任务
    """
    FlowDebuggerHandler(debugger_id=debugger_id).terminate()
    return True


@Lock.func_lock(Lock.LockTypes.CHECK_CUSTOM_CALCULATE)
def check_custom_calculate():
    custom_calculate_flow_tasks = FlowExecuteLog.objects.filter(
        action=FlowExecuteLog.ACTION_TYPES.CUSTOM_CALCULATE,
        status__in=FlowExecuteLog.ACTIVE_STATUS,
    )
    flow_logger.info("当前正在补算任务(%s)" % ",".join([str(_task.id) for _task in custom_calculate_flow_tasks]))
    for _flow_task in custom_calculate_flow_tasks:
        task = FlowCustomCalculateHandler(id=_flow_task.id)
        task.execute()


def start_custom_calculate(flow_id, operator):
    """
    启动补算任务
    @param flow_id:
    @param operator:
    @return:
    """
    o_flow = FlowHandler(flow_id=flow_id)
    if o_flow.status not in [FlowInfo.STATUS.RUNNING]:
        raise FlowCustomCalculateError(_("不处于运行状态，不可补算"))

    if o_flow.custom_calculate_status not in [
        FlowExecuteLog.STATUS.READY,
        FlowExecuteLog.STATUS.PENDING,
    ]:
        flow_logger.info("当前flow({})补算状态({})".format(flow_id, o_flow.custom_calculate_status))
        raise FlowCustomCalculateError(
            _("当前补算任务%s，不处于审批通过状态，不可补算")
            % dict(FlowExecuteLog.STATUS_CHOICES).get(o_flow.custom_calculate_status, o_flow.custom_calculate_status)
        )

    task_id = o_flow.custom_calculate_id
    # 打印当前即将补算的任务
    task = FlowCustomCalculateHandler(id=task_id)
    task.execute_before(operator)
    node_info = {}
    for task_type in ["batch"]:
        node_ids = task.get_task_context(task_type, "custom_calculate_node_ids")
        node_ids = node_ids if node_ids else []
        for node_id in node_ids:
            node_info[node_id] = {
                "status": FlowExecuteLog.STATUS.RUNNING,
            }
    res = {
        "task_id": task_id,
        "status": FlowExecuteLog.STATUS.RUNNING,
        "node_info": node_info,
    }
    return res


def stop_custom_calculate(flow_id, operator):
    """
    停止补算任务
    @param flow_id:
    @param operator:
    @return:
    """
    o_flow = FlowHandler(flow_id=flow_id)
    FlowCustomCalculateHandler(o_flow.custom_calculate_id).terminate()
    return True


def start_udf_debugger(function_name, operator, context, delay=True):
    """
    调试函数
    """
    task = UdfBaseHandler.create(
        func_name=function_name,
        operator=operator,
        action="start_debug",
        context=context,
    )

    debugger_id = task.debugger_id
    submit_task(execute_udf_debugger, debugger_id, operator, delay)
    return debugger_id


@celeryd_init.connect
def configure_workers(sender=None, conf=None, **kwargs):
    from common.api import base

    base.logger = flow_logger


@shared_task(queue="flow")
def custom_calculate_flow_task(task_id, language="en"):
    """
    @param task_id:
    @param language:
    @return:
    """
    set_local_param("request_language_code", language)
    BkLanguage.set_language("en")
    translation.activate("en")
    task = FlowCustomCalculateHandler(id=task_id)
    task.execute()


@shared_task(queue="flow")
def execute_flow_task(task_id, language="en"):
    """
    celery 任务部署日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param task_id:
    @param language:
    @return:
    """
    # request_language_code 用于保存用户当前通过界面请求的语言环境
    # 任务部署后，pizza common获取不到内部版用户的个人信息而返回默认的语言——中文，会导致内部版用户发送的邮件都是中文
    # 增加线程变量记录用户的接口访问语言，以此作为通知语言
    set_local_param("request_language_code", language)
    # 设置API调用 header 语言，若调用异常，返回信息应该是英文
    BkLanguage.set_language("en")
    # 默认django翻译用英文，因为存在字符串拼接的方式(format)获取完整的报错信息并双语存入DB，无法保证双语准确，因此统一用英语
    translation.activate("en")
    task = FlowTaskHandler(id=task_id)
    task.execute()


# 和 execute_flow_task 功能一样 但只允许实时任务
# @shared_task(queue='flow')
# def execute_flow_stream_task(task_id, language='en'):
#     set_local_param('request_language_code', language)
#     BkLanguage.set_language('en')
#     translation.activate('en')
#     task = FlowTaskHandler(id=task_id)
#     task.execute_stream()


#
# # 和 execute_flow_task 功能一样 但只允许'停止'实时任务
# @shared_task(queue='flow')
# def execute_flow_stream_task_stop(task_id, language='en'):
#     set_local_param('request_language_code', language)
#     BkLanguage.set_language('en')
#     translation.activate('en')
#     task = PlainNodeTaskHandler(id=task_id)
#     task.execute_stream_stop()
#
#
# # 和 execute_flow_task 功能一样 但只允许'启动'实时任务
# @shared_task(queue='flow')
# def execute_flow_stream_task_start(task_id, language='en'):
#     set_local_param('request_language_code', language)
#     BkLanguage.set_language('en')
#     translation.activate('en')
#     task = PlainNodeTaskHandler(id=task_id)
#     task.execute_stream_start()


@shared_task(queue="flow")
def execute_flow_debugger(debugger_id, language="en"):
    """
    celery 任务调试日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param debugger_id:
    @param language:
    @return:
    """
    BkLanguage.set_language("en")
    translation.activate("en")
    task = FlowDebuggerHandler(debugger_id=debugger_id)
    task.execute()


@shared_task(queue="flow")
def execute_udf_debugger(debugger_id, language="en"):
    """
    @param debugger_id:
    @param language:
    @return:
    """
    BkLanguage.set_language(language)
    translation.activate(language)
    task = UdfDebuggerHandler(debugger_id=debugger_id)
    task.execute()


def start_modeling_job(task):
    task_id = task.flow_task.id
    submit_model_task(execute_model_task, task)
    return task_id


def stop_model_job(task):
    submit_model_task(stop_model_task, task)
    return task.id


def submit_model_task(task, task_entity):
    try:
        return task(task_entity, BkLanguage.current_language())
    except Exception as e:
        flow_logger.error(e)
        raise SQLSubmitError()


@shared_task(queue="flow")
def execute_model_task(task, language="en"):
    """
    celery 任务部署日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param task_id:
    @param language:
    @return:
    """
    # request_language_code 用于保存用户当前通过界面请求的语言环境
    # 任务部署后，pizza common获取不到内部版用户的个人信息而返回默认的语言——中文，会导致内部版用户发送的邮件都是中文
    # 增加线程变量记录用户的接口访问语言，以此作为通知语言
    set_local_param("request_language_code", language)
    # 设置API调用 header 语言，若调用异常，返回信息应该是英文
    BkLanguage.set_language("en")
    # 默认django翻译用英文，因为存在字符串拼接的方式(format)获取完整的报错信息并双语存入DB，无法保证双语准确，因此统一用英语
    translation.activate("en")
    # task = MLSqlTaskHandler(task)
    task.execute()


@shared_task(queue="flow")
def stop_model_task(task, language="en"):
    """
    celery 任务部署日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param task_id:
    @param language:
    @return:
    """
    # request_language_code 用于保存用户当前通过界面请求的语言环境
    # 任务部署后，pizza common获取不到内部版用户的个人信息而返回默认的语言——中文，会导致内部版用户发送的邮件都是中文
    # 增加线程变量记录用户的接口访问语言，以此作为通知语言
    set_local_param("request_language_code", language)
    # 设置API调用 header 语言，若调用异常，返回信息应该是英文
    BkLanguage.set_language("en")
    # 默认django翻译用英文，因为存在字符串拼接的方式(format)获取完整的报错信息并双语存入DB，无法保证双语准确，因此统一用英语
    translation.activate("en")
    task = ModelingDatalabTaskHandler(task)
    task.stop()
