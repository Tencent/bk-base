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

import copy
import hashlib
import time
import uuid
from functools import reduce

from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_processing_node import ProcessingNode
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper

try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps  # Python 2.4 fallback.

import json

from common.auth.objects import is_sys_scopes
from common.auth.perms import UserPerm
from common.base_utils import model_to_dict
from common.exceptions import BaseAPIError
from common.local import get_local_param, get_request_username
from common.transaction import auto_meta_sync
from django.db import transaction
from django.db.models import Count, Max
from django.utils.decorators import available_attrs
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow.api_models import ResultTable, StreamJob
from dataflow.flow.exceptions import (
    FlowCustomCalculateError,
    FlowCustomCalculateNotAllowed,
    FlowError,
    FlowExistCircleError,
    FlowExistIsolatedNodesError,
    FlowNotFoundError,
    FlowTaskNotAllowed,
    NodeValidError,
    RelatedFlowRemoveError,
    RunningLinkCreateError,
    RunningLinkRemoveError,
)
from dataflow.flow.handlers.monitor import MonitorHandler
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.nodes.base_node.node_handler import call_func_with_rollback
from dataflow.flow.models import (
    FlowExecuteLog,
    FlowFileUpload,
    FlowInfo,
    FlowLinkInfo,
    FlowNodeInfo,
    FlowNodeInstanceLinkChannel,
    FlowNodeRelation,
)
from dataflow.flow.serializer.serializers import (
    ApplyCustomCalculateJobConfirmSerializer,
    ApplyCustomCalculateJobSerializer,
    LocationSerializer,
)
from dataflow.flow.utils.concurrency import concurrent_call_func
from dataflow.flow.utils.language import Bilingual
from dataflow.pizza_settings import HDFS_UPLOADED_FILE_TMP_DIR
from dataflow.shared import permission
from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.component.component_helper import ComponentHelper
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.project.project_helper import ProjectHelper
from dataflow.shared.meta.result_table.result_table_helper import TDW_RESULT_TABLE_BEEN_USED_ERR, ResultTableHelper
from dataflow.shared.send_message import send_system_message
from dataflow.shared.storekit.storekit_helper import StorekitHelper


def get_multi_wrap():
    """
    ModelViewSet适配器，用于支持GET请求的批量操作写法
    /flows?project_id=1&project_id=2 => /flows?project_id__in=1,2
    """

    def _wrap(func):
        @wraps(func, assigned=available_attrs(func))
        def _deco(self, *args, **kwargs):
            try:
                if not self.request.GET._mutable:
                    self.request.GET._mutable = True
                search_fields = list(self.request.GET.keys())
                for key in search_fields:
                    value_list = self.request.query_params.getlist(key)
                    # 若传入多个相同字段，则转成逗号分隔的字符串
                    if len(value_list) > 1:
                        new_key = key + "__in"
                        if new_key in search_fields:
                            del self.request.query_params[new_key]
                        self.request.query_params[key + "__in"] = ",".join(value_list)
                        del self.request.query_params[key]
                self.request.GET._mutable = False
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise e

        return _deco

    return _wrap


def valid_flow_wrap(lock_description):
    """
    Flow 专用的装饰器，在进行操作时 Flow 的状态是否符合要求，需要满足以下要求

    1. 校验 DataFlow 版本，当前版本与版本参数是否一致
    2. 校验 DataFlow 是否上锁

    @todo: 要求1未完成
    """

    def _wrap(func):
        @wraps(func, assigned=available_attrs(func))
        def _deco(self, *args, **kwargs):
            from dataflow.flow.views.flow_views import FlowViewSet
            from dataflow.flow.views.node_views import NodeViewSet
            from dataflow.flow.views.task_views import CustomCalcalculateViewSet

            use_lock = True
            if not lock_description:
                use_lock = False
            if isinstance(self, NodeViewSet):
                o_flow = FlowHandler(flow_id=kwargs.get("flow_id", None))
                if self.lookup_field in kwargs:
                    node_id = kwargs[self.lookup_field]
                    o_flow.check_node_id_in_flow(int(node_id))
            elif isinstance(self, FlowViewSet):
                o_flow = FlowHandler(flow_id=kwargs.get(self.lookup_field, None))
            elif isinstance(self, CustomCalcalculateViewSet):
                o_flow = FlowHandler(flow_id=kwargs.get("flow_id", None))
            else:
                o_flow = self

            if use_lock:
                o_flow.lock(lock_description)
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise e
            finally:
                if use_lock:
                    o_flow.unlock()

        return _deco

    return _wrap


class FlowHandler(object):
    def __init__(self, flow_id):
        self.flow_id = flow_id
        self.init_cache()

    def init_cache(self):
        """
        初始化数据缓存，这里直接置为None，由Python自动回收垃圾
        @return:
        """
        self._flow = None
        self._nodes = None
        self._node_ids = None
        self._links = None
        self._custom_calculate_task = None
        self._manage_tags = None

    def init_nodes_cache(self):
        """
        初始化节点数据缓存，这里直接置为None，由Python自动回收垃圾
        @return:
        """
        self._nodes = None
        self._node_ids = None
        self._links = None

    def perm_check_data(self, result_table_id):
        """
        基于项目的数据权限校验
        @param result_table_id:
        @return:
        """
        permission.perm_check_data(self.project_id, result_table_id)
        permission.perm_check_tag(self.geog_area_codes, result_table_id)
        return True

    def perm_check_cluster_group(self, cluster_group_id):
        """
        基于项目的集群组权限校验
        @param cluster_group_id:
        @return:
        """
        return permission.perm_check_cluster_group(self.project_id, cluster_group_id)

    def get_latest_deploy_context(self):
        latest_execute_log = self.latest_execute_log(
            action_types=[
                FlowExecuteLog.ACTION_TYPES.START,
                FlowExecuteLog.ACTION_TYPES.RESTART,
            ],
            is_done=True,
        )
        if not latest_execute_log:
            # 不存在最近一条启动/重启日志，应该重启
            return None
        context = latest_execute_log.get_context()
        return context

    def get_latest_deploy_cluster_group(self):
        cluster_group = None
        context = self.get_latest_deploy_context()
        if context and "cluster_group" in context:
            cluster_group = context["cluster_group"]
        return cluster_group

    def list_project_cluster_group(self):
        cluster_groups = NodeUtils.get_project_cluster_groups(
            self.project_id,
            "processing",
            service_type=["stream", "batch"],
            private_only=False,
        )

        # 根据 scope 排序，private -> public
        rule = {"private": 0, "public": 1}
        cluster_groups = sorted(cluster_groups, key=lambda cluster_group: rule[cluster_group["scope"]])
        # 若最近一条提交记录中存在选择集群组，需要将其调到最前面
        latest_cluster_group = self.get_latest_deploy_cluster_group()
        if latest_cluster_group:
            # 若存在当前可选列表中，则进行重排序
            cluster_groups = sorted(
                cluster_groups,
                key=lambda cluster_group: 0 if cluster_group["cluster_group_id"] == latest_cluster_group else 1,
            )
        return cluster_groups

    def perm_check_tdw_app_group(self):
        """
        基于 Flow 的 TDW 应用组权限校验
        若当前 Flow 存在 TDW 节点，需要通过 AUTH 校验当前用户是否有应用组的权限
        @return:
        """
        tdw_nodes = [_n for _n in self.nodes if _n.node_type in NodeTypes.TDW_CATEGORY]
        if tdw_nodes:
            tdw_conf = self.flow.get_tdw_conf()
            if not tdw_conf:
                raise FlowTaskNotAllowed(_("任务存在 TDW 节点，但未启用 TDW 支持"))
            app_group = tdw_conf["tdwAppGroup"]
            # 没权限的提示：没有TDW应用组的权限
            AuthHelper.check_tdw_app_group_perm(app_group)

    def create_alert_config(self, raise_exception=False):
        try:
            DatamanageHelper.create_alert_config(self.flow_id)
        except Exception as e:
            if raise_exception:
                raise e
            content = "任务({})创建告警策略配置详情异常: {}, 操作人员: {}".format(
                self.flow_id,
                e,
                get_request_username(),
            )
            send_system_message(content, raise_exception)

    @classmethod
    def list_flow_monitor_info(cls, result_table_ids):
        """
        根据result_table_id列表获取rt对应的flow相关监控信息
        @param {String[]} result_table_ids
        @return:
            {
                "591_xxx": [
                    {'flow_id': 1, 'is_source': true, 'node_id': 1, 'module': 'xx',
                        'component': 'xx', 'node_type': 'xxx'},
                    {'flow_id': 2, 'is_source': false, 'node_id': 1, 'module': 'xx',
                        'component': 'xx', 'node_type': 'xxx'},
                ]
            }
        """
        # 存储节点和清洗节点(stream_source的一种)需要从中获取其component和module，获取不到则返回为null
        databus_component_info = DatabusHelper.get_component_info_by_rt(result_table_ids)
        flows_rt_node_relation_info = FlowNodeRelation.objects.filter(result_table_id__in=result_table_ids).values(
            "flow_id", "node_id", "node_type", "result_table_id"
        )
        flow_monitor_info_dict = {}
        for relation_item in flows_rt_node_relation_info:
            node_id = relation_item["node_id"]
            result_table_id = relation_item["result_table_id"]
            node_type = relation_item["node_type"]
            # 若为关联数据源节点，不为该 rt 绑定 flow_id 和 node_id
            if node_type in NodeTypes.SOURCE_CATEGORY:
                continue
            rt_flow_info_list = flow_monitor_info_dict.setdefault(result_table_id, [])
            node = NODE_FACTORY.get_node_handler(node_id)
            # 初始化component信息
            module = None
            component = None
            local_component_detail = MonitorHandler.get_monitor_config_by_node(node)
            if local_component_detail:
                # 若本地包含rt对应的component信息，不需要从databus取(实际也取不到这些信息)
                module = local_component_detail["module"]
                component = local_component_detail["component"]
            else:
                component_detail = None
                # 存储节点对应的rt必须由databus取
                if node_type in NodeTypes.STORAGE_CATEGORY:
                    # 存储RT获取module、component信息
                    component_detail = databus_component_info.get(result_table_id, {}).get(node.storage_type, {})
                    # 存储节点对应的RT在运行，但信息为空
                    if not component_detail and node.status == FlowInfo.STATUS.RUNNING:
                        logger.warning(
                            _("RT(%(result_table_id)s)->存储节点(%(storage_type)s)，task信息获取失败")
                            % {
                                "result_table_id": result_table_id,
                                "storage_type": node.storage_type,
                            }
                        )
                if component_detail:
                    module = component_detail["module"]
                    component = component_detail["component"]
            rt_flow_info_list.append(
                {
                    "flow_id": relation_item["flow_id"],
                    "node_id": node_id,
                    "module": module,
                    "component": component,
                    "is_source": True if relation_item["node_type"] in NodeTypes.SOURCE_CATEGORY else False,
                    "node_type": relation_item["node_type"],
                }
            )
        return flow_monitor_info_dict

    @classmethod
    def list_rela_by_rtid(cls, result_table_ids, node_category=None):
        """
        查询使用 result_table_id 的 Flow 列表
        @param {List} result_table_ids
        @param {List} node_category 是否基于节点类型过滤
        """
        if not node_category:
            node_relations = list(FlowNodeRelation.objects.filter(result_table_id__in=result_table_ids).values())
        else:
            node_relations = list(
                FlowNodeRelation.objects.filter(
                    node_type__in=node_category, result_table_id__in=result_table_ids
                ).values()
            )
        related_flow_info = {}
        for node_relation in node_relations:
            node_ids = related_flow_info.setdefault(node_relation["flow_id"], [])
            node_ids.append(node_relation["node_id"])
        flows = FlowInfo.objects.filter(flow_id__in=list(related_flow_info.keys()))
        flows_info = []
        for _flow in flows:
            o_flow = model_to_dict(_flow)
            o_flow["related_nodes"] = list(
                map(
                    lambda node_id: {"node_id": node_id},
                    related_flow_info.get(o_flow["flow_id"], []),
                )
            )
            flows_info.append(o_flow)
        return flows_info

    @classmethod
    def list_running_flow_by_rtid(cls, result_table_id, as_queryset=False):
        """
        查询使用 result_table_id 的 Flow 列表
        @param {String} result_table_id
        @param {Boolean} as_queryset 是否返回 queryset
        """
        flow_ids = list(
            FlowNodeRelation.objects.filter(result_table_id=result_table_id).values_list("flow_id", flat=True)
        )

        running_flow_ids = []
        for flow_id in flow_ids:
            # 判断对应的flow是否在运行
            try:
                o_flow = FlowHandler(flow_id=flow_id)
                if o_flow.status == FlowInfo.STATUS.RUNNING:
                    # if node_id not in active_node_list:
                    running_flow_ids.append(flow_id)
            except FlowNotFoundError:
                pass

        flows = FlowInfo.objects.filter(flow_id__in=running_flow_ids)

        if as_queryset:
            return flows

        flows = [cls.init_by_flow(_flow) for _flow in flows]
        return flows

    @classmethod
    def list_rela_by_rtid_as_stream_source(cls, result_table_id, as_queryset=False):
        """
        查询使用 result_table_id 的 Flow 列表
        @param {String} result_table_id
        @param {Boolean} as_queryset 是否返回 queryset
        """
        flow_ids = list(
            FlowNodeRelation.objects.filter(
                result_table_id=result_table_id,
                node_type=NodeTypes.STREAM_SOURCE,
            ).values_list("flow_id", flat=True)
        )

        flows = FlowInfo.objects.filter(flow_id__in=flow_ids)
        if as_queryset:
            return flows

        flows = [cls.init_by_flow(_flow) for _flow in flows]
        return flows

    @classmethod
    def list_running_flow_by_rtid_as_source(cls, result_table_id, as_queryset=False):
        """
        查询使用 result_table_id 的 Flow 列表
        @param {String} result_table_id
        @param {Boolean} as_queryset 是否返回 queryset
        """
        flow_ids = list(
            FlowNodeRelation.objects.filter(
                result_table_id=result_table_id,
                node_type__in=NodeTypes.SOURCE_CATEGORY,
            ).values_list("flow_id", flat=True)
        )

        running_flow_ids = []
        for flow_id in flow_ids:
            # 判断对应的flow是否在运行
            try:
                o_flow = FlowHandler(flow_id=flow_id)
                if o_flow.status == FlowInfo.STATUS.RUNNING:
                    # if node_id not in active_node_list:
                    running_flow_ids.append(flow_id)
            except FlowNotFoundError:
                pass

        flows = FlowInfo.objects.filter(flow_id__in=running_flow_ids)

        if as_queryset:
            return flows

        flows = [cls.init_by_flow(_flow) for _flow in flows]
        return flows

    @classmethod
    def init_by_flow(cls, flow):
        o_flow = cls(flow_id=flow.flow_id)
        o_flow._flow = flow
        return o_flow

    @classmethod
    def list_rela_count_by_project_id(cls, project_ids):
        """
        通过 project_id 获取关联 flow 数量，支持批量，支持返回不同状态的 flow 数量
        """
        # 若未传入项目ID信息，返回有权限的项目下的 flow 列表
        if not project_ids:
            scopes = UserPerm(get_request_username()).list_scopes("project.retrieve")
            if is_sys_scopes(scopes):
                flows = FlowInfo.objects.values("project_id", "status", "flow_id")
            else:
                auth_project_ids = [_p["project_id"] for _p in scopes]
                flows = FlowInfo.objects.filter(project_id__in=auth_project_ids).values(
                    "project_id", "status", "flow_id"
                )
        else:
            flows = FlowInfo.objects.filter(project_id__in=project_ids).values("project_id", "status", "flow_id")

        # 补充 has_exception 字段
        # for f in flows:
        #     f['has_exception'] = False
        cls.wrap_exception(flows, many=True)

        project_ids_set = set(project_ids)
        m_count = {}
        for _f in flows:
            _project_id = _f["project_id"]
            _status = _f["status"]
            _has_exception = _f["has_exception"]

            project_ids_set.add(_project_id)

            if _project_id not in m_count:
                m_count[_project_id] = {
                    "count": 0,
                    "running_count": 0,
                    "no_start_count": 0,
                    "normal_count": 0,
                    "exception_count": 0,
                }

            m_count[_project_id]["count"] += 1
            if _status == FlowInfo.STATUS.RUNNING:
                m_count[_project_id]["running_count"] += 1
                if _has_exception:
                    m_count[_project_id]["exception_count"] += 1
                else:
                    m_count[_project_id]["normal_count"] += 1

            elif _status == FlowInfo.STATUS.NO_START:
                m_count[_project_id]["no_start_count"] += 1

        return [
            {
                "project_id": _id,
                "count": m_count[_id]["count"] if _id in m_count else 0,
                "running_count": m_count[_id]["running_count"] if _id in m_count else 0,
                "no_start_count": m_count[_id]["no_start_count"] if _id in m_count else 0,
                "normal_count": m_count[_id]["normal_count"] if _id in m_count else 0,
                "exception_count": m_count[_id]["exception_count"] if _id in m_count else 0,
            }
            for _id in project_ids_set
        ]

    @staticmethod
    def validate_related_source_nodes(node, include_current_flow=True):
        """
        校验节点是否被任务的数据源节点使用
        @param node:
        @param include_current_flow: 是否包含当前 flow 对当前待删除RT的使用
        @return:
        """
        related_source_nodes = NodeUtils.get_related_source_nodes(node)
        if related_source_nodes:
            related_flow_info = {}
            related_result_table_ids = set()
            for related_source_node in related_source_nodes:
                flow_id = related_source_node.flow_id
                if not include_current_flow and node.flow_id == flow_id:
                    continue
                related_result_table_ids.add(related_source_node.result_table_id)
                related_flow_info[flow_id] = FlowHandler(flow_id).flow_name
            if not related_flow_info:
                return True
            logger.info(
                "deleting {} is not allowed for flows({}) use it.".format(node.node_id, list(related_flow_info.keys()))
            )
            raise RelatedFlowRemoveError(
                _("结果表(%(result_table_id)s)被任务(%(flow_name)s%(postfix)s)引用，不允许删除")
                % {
                    "postfix": _("等") if len(list(related_flow_info.values())) > 2 else "",
                    "result_table_id": ", ".join(list(related_result_table_ids)[:2]),
                    "flow_name": ", ".join(list(related_flow_info.values())[:2]),
                }
            )

    @staticmethod
    def validate_related_tdw_source(result_table_id):
        """
        校验 tdw 标准化 rt 是否被任务引用
        @param result_table_id:
        @return:
        """
        related_nodes = FlowNodeRelation.objects.filter(result_table_id=result_table_id)
        if related_nodes:
            related_flow_info = {}
            for related_node in related_nodes:
                flow_id = related_node.flow_id
                related_flow_info[flow_id] = FlowHandler(flow_id).flow_name
            if not related_flow_info:
                return True
            logger.info(
                "deleting tdw_source %s is not allowed for flows(%s) use it."
                % (result_table_id, list(related_flow_info.keys()))
            )
            raise RelatedFlowRemoveError(
                _("结果表(%(result_table_id)s)被任务(%(flow_name)s%(postfix)s)引用，不允许删除")
                % {
                    "postfix": _("等") if len(list(related_flow_info.values())) > 2 else "",
                    "result_table_id": result_table_id,
                    "flow_name": ", ".join(list(related_flow_info.values())[:2]),
                }
            )

    def list_versions(self):
        return self.flow.versions

    def list_source_node_status(self):
        """
        获取数据源的实际任务状态
        @return:
        """
        node_status = {}
        if self.status not in [FlowInfo.STATUS.RUNNING]:
            return node_status
        source_rt_info = {}
        for node in self.nodes:
            _node_id = node.node_id
            _status = node.status
            if _status in [FlowInfo.STATUS.RUNNING] and node.node_type in NodeTypes.SOURCE_CATEGORY:
                source_node_info = source_rt_info.setdefault(node.result_table_id, set())
                source_node_info.add(_node_id)
        if source_rt_info:
            # 查询数据源节点对应 RT 的实际运行状态
            # 获取 RT 种类(产生于数据开发 或 其它)
            dataflow_rt_info = FlowNodeRelation.objects.filter(
                result_table_id__in=list(source_rt_info.keys()),
                node_type__in=NodeTypes.PROCESSING_CATEGORY,
            ).values("result_table_id", "node_id")

            dataflow_rt_ids = []
            # 记录 processing 节点及其被使用为数据源结果表的 rt
            dataflow_node_rt_dict = {}
            for dataflow_rt in dataflow_rt_info:
                node_id = dataflow_rt["node_id"]
                result_table_id = dataflow_rt["result_table_id"]
                dataflow_rt_ids.append(result_table_id)
                _result_table_ids = dataflow_node_rt_dict.setdefault(node_id, set())
                _result_table_ids.add(result_table_id)

            # 获取画布产生结果表状态
            if dataflow_node_rt_dict:
                # processing 节点
                _node_objs = FlowNodeInfo.objects.filter(node_id__in=list(dataflow_node_rt_dict.keys())).values(
                    "node_id", "status"
                )
                for _node_obj in _node_objs:
                    if _node_obj["status"] == FlowInfo.STATUS.NO_START:
                        source_result_table_ids = dataflow_node_rt_dict[_node_obj["node_id"]]
                        for source_result_table_id in source_result_table_ids:
                            # 根据被引用为数据源结果表的 rt 获取到的状态信息，更新到 node_status
                            list(
                                map(
                                    lambda _source_node_id: node_status.update(
                                        {
                                            _source_node_id: {
                                                "status": FlowInfo.STATUS.NO_START,
                                                "message": "dataflow job has been stopped.",
                                            }
                                        }
                                    ),
                                    source_rt_info[source_result_table_id],
                                )
                            )
            # 获取其它(清洗)结果表状态
            other_rt_ids = set(source_rt_info.keys()).difference(set(dataflow_rt_ids))
            if other_rt_ids:
                request_info = []
                list(
                    map(
                        lambda result_table_id: request_info.append(
                            [DatabusHelper.get_cleans, {"result_table_id": result_table_id}]
                        ),
                        other_rt_ids,
                    )
                )
                threads_res = concurrent_call_func(request_info)
                for idx, source_result_table_id in enumerate(other_rt_ids):
                    if threads_res[idx] and threads_res[idx]["status"] != "started":
                        list(
                            map(
                                lambda _source_node_id: node_status.update(
                                    {
                                        _source_node_id: {
                                            "status": FlowInfo.STATUS.NO_START,
                                            "message": "data-access job has been stopped.",
                                        }
                                    }
                                ),
                                source_rt_info[source_result_table_id],
                            )
                        )
        return node_status

    def get_tail_rt_info(self):
        _tail_rt_info = {}
        rts = FlowNodeRelation.objects.filter(flow_id=self.flow_id, generate_type="user").only(
            "result_table_id", "node_id", "node_type"
        )
        # 若为计算节点，需要检查其在其它任务是否存在节点，性能考虑统一查询 DB
        check_related_info = {}
        for rt in rts:
            node_id = rt.node_id
            result_table_id = rt.result_table_id
            if node_id not in _tail_rt_info:
                _tail_rt_info[node_id] = {
                    "related": False,
                    "result_table_ids": [result_table_id],
                }
            else:
                _tail_rt_info[node_id]["result_table_ids"].append(result_table_id)
            if rt.node_type in NodeTypes.CALC_CATEGORY:
                check_related_info[result_table_id] = node_id
        if check_related_info:
            related_result_table_ids = (
                FlowNodeRelation.objects.filter(result_table_id__in=list(check_related_info.keys()))
                .exclude(flow_id=self.flow_id)
                .values_list("result_table_id", flat=True)
            )
            for related_result_table_id in related_result_table_ids:
                _tail_rt_info[check_related_info[related_result_table_id]]["related"] = True
        return _tail_rt_info

    def get_nodes_info(self, loading_latest=True):
        """
        获取节点配置信息
        @param loading_latest: 是否结合元数据信息获取节点最新配置信息
        @return:
        """
        nodes_info = {}
        nodes = FlowNodeInfo.objects.filter(flow_id=self.flow_id)
        # 获取节点补算状态信息
        custom_calculate_node_ids = []
        _flow_custom_calculate_status = self.custom_calculate_status
        if _flow_custom_calculate_status in FlowExecuteLog.ACTIVE_STATUS:
            flow_custom_calculate_status = "running"
        elif _flow_custom_calculate_status in FlowExecuteLog.DEPLOY_BEFORE_STATUS:
            flow_custom_calculate_status = _flow_custom_calculate_status
        else:
            flow_custom_calculate_status = None
        if self.custom_calculate_task:
            context = json.loads(self.custom_calculate_task.context)
            tasks = context["tasks"] if "tasks" in context else {}
            for task_type, config in list(tasks.items()):
                custom_calculate_node_ids.extend(config["custom_calculate_node_ids"])
        for node in nodes:
            node_type = node.node_type
            node_config = node.load_config(loading_latest, loading_concurrently=True)
            node_name = node_config["name"] if "name" in node_config else node.node_name
            custom_calculate_status = None
            if flow_custom_calculate_status:
                custom_calculate_status = (
                    flow_custom_calculate_status if node.node_id in custom_calculate_node_ids else None
                )
            # 增加是否修正配置
            if "is_open_correct" in node_config and node_config["is_open_correct"]:
                is_data_corrected = True
            else:
                is_data_corrected = False
            nodes_info[node.node_id] = {
                "flow_id": self.flow_id,
                "node_id": node.node_id,
                "node_name": node_name,
                "node_config": node_config,
                "node_type": node_type,
                "status": node.status,
                "version": "",
                "frontend_info": json.loads(node.frontend_info),
                "has_modify": False,
                "custom_calculate_status": custom_calculate_status,
                "is_data_corrected": is_data_corrected,
            }
        func_info_cache = get_local_param("loading_node_config_concurrently", {})
        if func_info_cache:
            node_configs = concurrent_call_func([_info["func_info"] for _info in func_info_cache])
            for index, func_info in enumerate(func_info_cache):
                _node_id = func_info["node_id"]
                _node_config = node_configs[index]
                if not _node_config:
                    content = "获取任务({})节点({})结果表中文名称失败，操作者: {}".format(
                        self.flow_id,
                        _node_id,
                        get_request_username(),
                    )
                    send_system_message(content, False)
                else:
                    nodes_info[_node_id]["node_config"] = _node_config
        return nodes_info

    @staticmethod
    def get_node_display_status(node_id, from_node_info, cur_node_version, running_node_version, _dict):
        _status = _dict["status"]
        # 节点既存在运行版本，也存在当前版本，且两个版本发生变化
        # 或者由于节点使用的udf发布了新的版本，则当前节点为修改状态
        if _status in FlowInfo.PROCESS_STATUS and node_id in running_node_version and node_id in cur_node_version:
            node_handler = NODE_FACTORY.get_node_handler(node_id)
            if running_node_version[node_id] != cur_node_version[node_id] or (
                node_handler.node_type
                in [
                    NodeTypes.STREAM,
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                ]
                and NodeUtils.is_modify_dp_node_with_udf(node_handler.processing_id)
            ):
                _status = FlowInfo.STATUS.WARNING

        # 若发生连线上的错误，校验出来，更改返回的节点状态
        if _status != FlowInfo.STATUS.FAILURE and _dict["node_type"] not in NodeTypes.SOURCE_CATEGORY:
            current_from_result_table_ids = []
            if _dict["node_type"] in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY + NodeTypes.NEW_INPUTS_CATEGORY:
                for from_node in NodeUtils.get_input_nodes_config(_dict["node_type"], _dict["node_config"]):
                    current_from_result_table_ids.extend(from_node["from_result_table_ids"])
            else:
                current_from_result_table_ids = _dict["node_config"].get("from_result_table_ids", [])
            # 一定是增加了上游节点未保存, 目前大部分为单输出且一个上游节点只能有一个 RT 被在当前节点被使用
            if len(current_from_result_table_ids) < len(list(from_node_info.keys())):
                logger.info("[1]节点(%s)连线配置错误" % node_id)
                return FlowInfo.STATUS.FAILURE
            current_from_node = list(from_node_info.keys())
            _from_nodes_rts = []
            for _from_node_id in current_from_node:
                _from_node_rts = from_node_info.get(_from_node_id, [])
                _from_nodes_rts.extend(_from_node_rts)
                # 若上游节点的 RT 不在当前节点的 from_result_table_ids 中, 增加了连线未保存
                if not set(_from_node_rts) & set(current_from_result_table_ids):
                    logger.info("[2]节点(%s)连线配置错误" % node_id)
                    _status = FlowInfo.STATUS.FAILURE
                    break
            # 若当前节点存在上游节点没有的 result_table_id, 删除了连线未保存
            if _status != FlowInfo.STATUS.FAILURE and set(current_from_result_table_ids) - set(_from_nodes_rts):
                logger.info("[3]节点(%s)连线配置错误" % node_id)
                _status = FlowInfo.STATUS.FAILURE
        return _status

    def get_draft(self):
        """
        返回前端可展示的草稿信息
        @return:
            {
                "version": "V20191121192756#giJUOi9Siql8PmhFo6tQ",
                "lines": [
                    {
                        "to_node_id": 1111,
                        "from_node_id": 222,
                        "frontend_info": {
                            "source": {
                                "node_id": 16929,
                                "id": "ch_16929",
                                "arrow": "Right"
                            },
                            "target": {
                                "id": "ch_16930",
                                "arrow": "Left"
                            }
                        }
                    },
                    {
                        "to_node_id": 1111,
                        "from_node_id": 222,
                        "frontend_info": {
                            "source": {
                                "node_id": 16931,
                                "id": "ch_16931",
                                "arrow": "Right"
                            },
                            "target": {
                                "id": "ch_16932",
                                "arrow": "Left"
                            }
                        }
                    }
                ],
                "locations": [
                    {
                        "status": "running",
                        "node_id": 16929,
                        "id": "ch_16929",
                        "node_type": "tdw_source",
                        "node_name": "ss_tdw[tdw_12497_591_tdw_tdtw]",
                        "flow_id": "2707",
                        "frontend_info": {
                            "y": 258,
                            "x": 477
                        },
                        "related": false,
                        "custom_calculate_status" "running",
                        "result_table_ids"：[],
                        "bk_biz_id": 1,
                        "node_config": {
                            "fixed_deplay": 0,          # 前端需要基于这些值确定新节点的展示情况
                            "count_freq": 1,           # 前端需要基于这些值确定新节点的展示情况
                            "schedule_period": "hour"  # 前端需要基于这些值确定新节点的展示情况
                        }
                    }
                ]
            }
        """

        d_nodes = []
        d_links = []

        # 补充节点运行版本变更与否
        cur_node_version = self.flow.list_node_versions()
        running_node_version = self.flow.list_running_node_versions()

        # 获取flow中所有rt告警状态
        _tail_rt_info = self.get_tail_rt_info()
        _nodes_config = self.get_nodes_info(False)
        _node_link_info = {}
        for _link in self.links:
            current_from_node_info = _node_link_info.setdefault(_link.to_node_id, [])
            current_from_node_info.append(_link.from_node_id)
            d_links.append(
                {
                    "from_node_id": _link.from_node_id,
                    "to_node_id": _link.to_node_id,
                    "frontend_info": _link.get_frontend_info(),
                }
            )
        for _node_id, _dict in list(_nodes_config.items()):
            # 节点既存在运行版本，也存在当前版本，且两个版本发生变化
            from_node_info = {}
            for from_node_id in _node_link_info.get(_node_id, []):
                from_node_info[from_node_id] = _tail_rt_info[from_node_id]["result_table_ids"]
            _status = FlowHandler.get_node_display_status(
                _node_id, from_node_info, cur_node_version, running_node_version, _dict
            )
            _dict["status"] = _status
            _dict["related"] = _tail_rt_info[_node_id]["related"]
            _dict["result_table_ids"] = list(set(_tail_rt_info[_node_id]["result_table_ids"]))
            _dict["bk_biz_id"] = _dict["node_config"].get("bk_biz_id", None)
            delay_config = {
                "fixed_delay": _dict["node_config"].get("fixed_delay", 0),
                "count_freq": _dict["node_config"].get("count_freq", 0),
                "schedule_period": _dict["node_config"].get("schedule_period", "hour"),
            }
            # 覆盖 node_config
            _dict["node_config"] = delay_config

            d_nodes.append(_dict)

        """
        根据 d_links 补齐 channel 表
        删除节点时，会在 dataflow_node_instance_link_channel 表中查询节点 channel 有没有被用到
        但是对于以前创建的任务，表中没有存数据。对于此类任务，做个适配，补齐上下游的 channel 存表
        补齐条件：计算->存储、计算->计算
        """
        for d_link in d_links:
            from_node_id = d_link["from_node_id"]
            to_node_id = d_link["to_node_id"]
            parent_node = NODE_FACTORY.get_node_handler(node_id=from_node_id)
            downstream_node = NODE_FACTORY.get_node_handler(node_id=to_node_id)
            # 节点类型满足条件，上游是计算节点，下游是计算和存储节点
            if (
                parent_node.node_type in NodeTypes.PROCESSING_CATEGORY
                and downstream_node.node_type in NodeTypes.PROCESSING_CATEGORY + NodeTypes.STORAGE_CATEGORY
            ):
                channel_type = NodeInstanceLink.get_link_path_channel(parent_node, downstream_node)
                if channel_type in NodeTypes.CHANNEL_STORAGE_CATEGORY:
                    parent_result_table_ids = parent_node.result_table_ids
                    for result_table_id in parent_result_table_ids:
                        if not FlowNodeInstanceLinkChannel.is_channel_storage_exist(
                            result_table_id, channel_type, downstream_node.node_id
                        ):
                            # 如果 channel 记录不存在，则补上
                            FlowNodeInstanceLinkChannel.save_channel_storage(
                                result_table_id=result_table_id,
                                channel_storage=channel_type,
                                downstream_node_id=downstream_node.node_id,
                                channel_mode=parent_node.channel_mode,
                                created_by=get_request_username(),
                            )

        return {"locations": d_nodes, "lines": d_links, "version": self.version}

    def get_graph(self, version_id=None):
        """
        返回前端可展示的图信息
        """
        d_nodes = []
        d_links = []

        # 补充节点运行版本变更与否
        cur_node_version = self.flow.list_node_versions()
        running_node_version = self.flow.list_running_node_versions()

        # 获取flow中所有rt告警状态
        alert_status = {}
        if self.status not in [FlowInfo.STATUS.RUNNING]:
            alert_status = {}
        _tail_rt_info = self.get_tail_rt_info()
        _nodes_config = self.get_nodes_info()
        for _node_id, _dict in list(_nodes_config.items()):
            _status = _dict["status"]

            # 节点既存在运行版本，也存在当前版本，且两个版本发生变化
            if (
                _status in FlowInfo.PROCESS_STATUS
                and _node_id in running_node_version
                and _node_id in cur_node_version
                and running_node_version[_node_id] != cur_node_version[_node_id]
            ):
                _dict["has_modify"] = True
                _dict["status"] = FlowInfo.STATUS.WARNING
            else:
                _dict["has_modify"] = False

            _dict["related"] = _tail_rt_info[_node_id]["related"]
            _dict["result_table_ids"] = list(set(_tail_rt_info[_node_id]["result_table_ids"]))
            _dict["alert_status"] = alert_status.get(_node_id, "info")

            d_nodes.append(_dict)

        _node_link_info = {}
        for _link in self.links:
            current_from_node_info = _node_link_info.setdefault(_link.to_node_id, [])
            current_from_node_info.append(_link.from_node_id)
            d_links.append(
                {
                    "from_node_id": _link.from_node_id,
                    "to_node_id": _link.to_node_id,
                    "frontend_info": _link.get_frontend_info(),
                }
            )

        # 若发生连线上的错误，校验出来，更改返回的节点状态
        for _node_id, _dict in list(_nodes_config.items()):
            if _dict["node_type"] in NodeTypes.SOURCE_CATEGORY:
                continue
            _status = _dict["status"]
            if _status != FlowInfo.STATUS.FAILURE:
                current_from_result_table_ids = []
                if _dict["node_type"] in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY + NodeTypes.NEW_INPUTS_CATEGORY:
                    for from_node in NodeUtils.get_input_nodes_config(_dict["node_type"], _dict["node_config"]):
                        current_from_result_table_ids.extend(from_node["from_result_table_ids"])
                else:
                    current_from_result_table_ids = _dict["node_config"].get("from_result_table_ids", [])
                # 一定是增加了上游节点未保存, 目前大部分为单输出且一个上游节点只能有一个 RT 被在当前节点被使用
                if len(current_from_result_table_ids) < len(_node_link_info.get(_node_id, [])):
                    logger.info("[1]节点(%s)连线配置错误" % _node_id)
                    _dict["status"] = FlowInfo.STATUS.FAILURE
                    continue
                current_from_node = _node_link_info.get(_node_id, [])
                _from_nodes_rts = []
                for _from_node_id in current_from_node:
                    _from_node_rts = _tail_rt_info[_from_node_id]["result_table_ids"]
                    _from_nodes_rts.extend(_from_node_rts)
                    # 若上游节点的 RT 不在当前节点的 from_result_table_ids 中, 增加了连线未保存
                    if set(_from_node_rts) & set(current_from_result_table_ids):
                        logger.info("[2]节点(%s)连线配置错误" % _node_id)
                        _dict["status"] = FlowInfo.STATUS.FAILURE
                        break
                # 若当前节点存在上游节点没有的 result_table_id, 删除了连线未保存
                if _status != FlowInfo.STATUS.FAILURE and set(current_from_result_table_ids) - set(_from_nodes_rts):
                    logger.info("[3]节点(%s)连线配置错误" % _node_id)
                    _dict["status"] = FlowInfo.STATUS.FAILURE
        return {"nodes": d_nodes, "links": d_links, "version": self.version}

    def get_node(self, node_id):
        """
        在 DataFlow 数据结构中获取某一节点对象
        """
        for _node in self.nodes:
            if _node.node_id == node_id:
                return _node

        raise Exception(_("非法节点ID，node_id=%s") % node_id)

    def get_ordered_nodes(self, reverse=False, isolated_excluded=True):
        """
        获取排序的节点列表

        @note 检查是否存在回路
        @param reverse {boolean} 是否倒序
        @param isolated_excluded {boolean} 是否排除孤立节点
        """
        iter_arr = []
        order_arr = []
        # 先获取头部节点
        source_ids = self.get_source_node_ids()
        iter_arr.extend(source_ids)
        order_arr.extend(source_ids)

        # from_node -> [to_node]
        links_index = self._get_links_index()
        links_index_reversed = self._get_links_index(reverse=True)
        if isolated_excluded:
            # to_node -> [from_node]
            # 检测孤立节点
            isolated_nodes = []
            for one_node in self.nodes:
                if one_node.node_id not in links_index and one_node.node_id not in links_index_reversed:
                    # 孤立节点
                    isolated_nodes.append(one_node.node_id)

            for one_isolated_node in isolated_nodes:
                if one_isolated_node in iter_arr:
                    iter_arr.remove(one_isolated_node)
                    order_arr.remove(one_isolated_node)

        while iter_arr:
            iter_node_id = iter_arr.pop(0)
            if iter_node_id not in links_index:
                continue

            # 扫描单个节点的下游是否可以加入结果队列
            # 需满足：
            #   （该下游节点的）上游节点都已经在结果队列中，则可以加入结果队列
            #   否则，将上游节点加入内部处理队列，继续同样逻辑处理处理上游，再处理当前节点
            inner_up_arr = []
            inner_up_arr.extend(links_index[iter_node_id])
            while inner_up_arr:
                innner_node_id = inner_up_arr.pop(0)
                if innner_node_id in links_index_reversed:
                    diff_arr = [i for i in links_index_reversed[innner_node_id] if i not in order_arr]
                    if diff_arr:
                        inner_up_arr.extend(diff_arr)
                        inner_up_arr.append(innner_node_id)
                    else:
                        if innner_node_id not in order_arr:
                            order_arr.append(innner_node_id)
                            iter_arr.append(innner_node_id)

        node_ids = order_arr[::-1] if reverse else order_arr
        return [self.get_node(_node_id) for _node_id in node_ids]

    def get_source_node_ids(self):
        """
        获取头部节点ID列表
        """
        return list(
            map(
                lambda _n: _n.node_id,
                list(
                    filter(
                        lambda _n: _n.node_type in NodeTypes.SOURCE_CATEGORY,
                        self.nodes,
                    )
                ),
            )
        )

    def check_isolated_nodes(self):
        """
        检查当前flow是否存在孤立的节点
        @return:
        """
        ordered_nodes = self.get_ordered_nodes()
        ordered_node_ids = [_n.node_id for _n in ordered_nodes]
        invalid_nodes = []
        for _node_id in self.node_ids:
            if _node_id not in ordered_node_ids:
                # 当前节点不在Flow待运行节点中
                invalid_nodes.append(NODE_FACTORY.get_node_handler(_node_id))
        if invalid_nodes:
            # 日志最多提示五个节点
            invalid_nodes_to_display = invalid_nodes[:5]
            display_msg = "、".join(
                map(
                    lambda _n: str(
                        Bilingual(ugettext_noop("{display}节点（{output}）")).format(
                            output=_n.name, display=_n.get_node_type_display()
                        )
                    ),
                    invalid_nodes_to_display,
                )
            )
            display_msg = display_msg + _("等节点为孤立节点，请重新构建拓扑后操作")
            raise FlowExistIsolatedNodesError(display_msg)
        return ordered_nodes

    def check_circle(self):
        """
        图中的所有节点做一个标记，对于图中的某个节点 n，其标记 marks[n] 的值可能为以下三种值，其分别代表：

        0：该节点还未被遍历过。
        1：该节点或者该节点的后代正在被遍历
        2：该节点已经被遍历过，且经过该节点的链路上没有检测出环

        1. 首先，需要将所有节点的 mark 置为 0。
        2. 对图进行一次 DFS。
        3. 将当前遍历到的节点的 mark 置为 1。
        4. 当完成某个子图的遍历时，将其父节点的 mark 置为 2。
        5. 若在遍历过程中遇到 mark 为 1 的节点，则说明该有向图中存在环。
        """
        INIT = 0

        marks = {node.node_id: INIT for node in self.nodes}
        source_ids = self.get_source_node_ids()

        for _source_id in source_ids:
            try:
                self.check_node_circle(self._get_links_index(), _source_id, marks)
            except FlowExistCircleError as e:
                _circle_node = self.get_node(e.node_id)
                raise FlowExistCircleError(_("存在回路节点（[{}] {}）".format(_circle_node.node_id, _circle_node.name)))

    def check_node_circle(self, graph, cur_node_id, marks):

        ITERATING = 1
        ITERATED = 2

        print("-" * 10)
        print(cur_node_id, marks[cur_node_id])
        print("-" * 10)

        # skip when iterate to a node marked as 2
        if marks[cur_node_id] == ITERATED:
            return True

        # raise FlowExistCircleError when iterate to a node which been marked as 1
        if marks[cur_node_id] == ITERATING:
            _err = FlowExistCircleError()
            _err.node_id = cur_node_id
            raise _err

        # mark as iterating
        marks[cur_node_id] = ITERATING

        # DFS
        if cur_node_id in graph:
            for to_node_id in graph[cur_node_id]:
                # return immediately when circle be detected
                self.check_node_circle(graph, to_node_id, marks)

        # mark as iterated
        marks[cur_node_id] = ITERATED

        return True

    def check_locked(self):
        self.flow.check_locked()

    @auto_meta_sync(using="default")
    def lock(self, description):
        self.flow.lock(description)

    @auto_meta_sync(using="default")
    def unlock(self):
        self.flow.unlock()

    def to_dict(self):
        pass

    def get_stream_nodes(self, ordered=False):
        """
        获取 DataFlow 中存在实时计算节点，支持排序功能
        """
        return self.get_nodes_by_node_type(
            [
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            ],
            ordered,
        )

    def get_batch_nodes(self, ordered=False):
        """
        获取 DataFlow 中存在离线计算节点，支持排序功能
        """
        return self.get_nodes_by_node_type(NodeTypes.BATCH_CATEGORY, ordered)

    def get_model_app_nodes(self, ordered=False):
        """
        获取 DataFlow 中存在离线计算节点，支持排序功能
        """
        return self.get_nodes_by_node_type([NodeTypes.MODEL_APP], ordered)

    def get_nodes_by_node_type(self, node_types, ordered=False):
        """
        获取从数据源起始的节点列表
        @param node_types:
        @param ordered:
        @return:
        """
        nodes = self.get_ordered_nodes() if ordered else self.nodes
        return [n for n in nodes if n.node_type in node_types]

    def grafting_head_tail_rts(self, topo_nodes, head_rts, tail_rts):
        """
        对head, tail 基于树的结构进行嫁接规整，避免画布中tail RT 被引用为数据源而无法基于 head, tail 完全完整拓扑结构
        1. 取出通用拓扑的 head, tail
        2. 规整
            A1 -> A2 -> A3 -> storage
            A2 -> B1 -> B2 -> storage
            C1 -> A1 -> storage
            ====>
            C1 -> A1 -> A2 -> B1 -> B2 -> storage
                          \
                          A3 -> storage
            即 head = [A1, A2, C1], tail = [A1, A3, B2]
            规整为 head = [C1], tail = [A3, B2]
        * 基本规则为：
        a. tail_rt 不可作为当前画布下游拥有实时节点的数据源的 head_rt，根据该规则构建 must_not_be_tails
        b. head_rt 不可为当前画布实时节点产生的 tail_rt，根据该规则构建 must_not_be_heads
        @param topo_nodes: 同类型节点，实时或离线
        @param head_rts: 未规整前 head 列表
        @param tail_rts: 未规整前 tail 列表
        @return:
        """
        must_not_be_tails = []
        must_not_be_heads = []
        for topo_node in topo_nodes:
            must_not_be_heads.extend(topo_node.result_table_ids)
            for _n in topo_node.get_from_nodes_handler():
                # 实时数据源有可能可以接离线计算
                # 离线数据源有可能可以接实时计算
                if _n.node_type in [
                    NodeTypes.STREAM_SOURCE,
                    NodeTypes.BATCH_SOURCE,
                ]:
                    must_not_be_tails.extend(_n.result_table_ids)
        return list(set(head_rts) - set(must_not_be_heads)), list(set(tail_rts) - set(must_not_be_tails))

    def get_topo_head_tail_rts(self, topo_node_type="stream"):
        """
        获取所有计算节点组合起来的 head_rts、tail_ids
        @param topo_node_type: stream / batch / model_app
        @return:
        """
        if topo_node_type == "stream":
            topo_nodes = self.get_stream_nodes()
            not_head_category = [
                NodeTypes.KV_SOURCE,
                NodeTypes.STREAM,
                NodeTypes.DATA_MODEL_APP,
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
                NodeTypes.UNIFIED_KV_SOURCE,
            ]
        elif topo_node_type == "batch" or topo_node_type == "batchv2":
            topo_nodes = self.get_batch_nodes()
            not_head_category = [
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
            ]
        elif topo_node_type == "model_app":
            topo_nodes = self.get_model_app_nodes()
            not_head_category = [NodeTypes.MODEL_APP]
        head_nodes, tail_nodes = self.get_heads_tails_from_nodes(topo_nodes)
        head_rts = set()
        for _n in head_nodes:
            _from_nodes = _n.get_from_nodes_handler()
            # 当前节点可作为 head 的上游节点的所有 rt 列表
            _n_from_result_table_ids = []
            for _from_node in _from_nodes:
                # 除了静态关联数据源、实时计算节点对应的RT，其它的只要符合连线规则都可作为 head
                if _from_node.node_type not in not_head_category:
                    _n_from_result_table_ids.extend(_from_node.result_table_ids)
            head_rts = head_rts | (set(_n.from_result_table_ids) & set(_n_from_result_table_ids))
        head_rts = list(head_rts)
        tail_rts = list(set(reduce(lambda prev, n: prev + n.result_table_ids, tail_nodes, [])))
        return self.grafting_head_tail_rts(topo_nodes, head_rts, tail_rts)

    # def get_stream_head_rts(self):
    #     """
    #     获取在 DataFlow 中，实时启动真正会依赖的 head_rts
    #     """
    #     source_nodes = [
    #         n for n in self.nodes
    #         if n.node_type in NodeTypes.STREAM_SOURCE_CATEGORY
    #     ]
    #     head_rts = []
    #     for _node in source_nodes:
    #         head_rts.extend(_node.result_table_ids)
    #     return head_rts

    def get_heads_tails_from_nodes(self, nodes):
        """
        从节点中提取头结点和尾节点，提取方式演示(前提：传入 nodes 为正常拓扑中的节点)：
             E -> F -> G -> H
             ^
             |
        A -> B -> C -> D

        1. 假设传入 nodes=[CEF]
        2. 返回 ([CE], [F])
        3. 假设传入 nodes=[CEGH]
        4. 返回 ([CEG], [CEH])
             B -> C
           /     /
        A   -->
        1. 假设 A 是数据源，传入 nodes=[BC]
        2. 返回 ([BC], [C])
        @param nodes:
        @return:
            head_nodes: nodes中的头部节点
            tail_nodes: nodes中的尾部节点
        """
        node_ids = [n.node_id for n in nodes]

        source_ids = list(
            map(
                lambda _n: _n.node_id,
                list(
                    filter(
                        lambda _n: _n.node_type in NodeTypes.SOURCE_CATEGORY,
                        self.nodes,
                    )
                ),
            )
        )
        links_index = self._get_links_index()
        reverse_links_index = self._get_links_index(reverse=True)

        # 结合nodes，得出`源节点`id列表
        for node in nodes:
            _from_ids = reverse_links_index[node.node_id]
            # 若当前节点没有上游节点或存在上游节点但该节点的所有上游节点中，存在一个上游节点是在 source_ids 或 node_ids 中
            # 表明当前节点:
            # 1. 要么是 source(已在 source_ids 中)
            # 2. 要么其中一个上游节点在 source_ids 中(下面根据 source_ids 找 head 的逻辑自然会将当前节点归为 head)
            # 3. 要么其中一个上游节点在传入节点中，当前节点肯定不会是 head
            if not _from_ids or set(_from_ids) & (set(source_ids) | set(node_ids)):
                continue
            else:
                # 否则将其中一个上游节点加入 source_ids 中
                # source_ids 下一级是 head，因此取其中一个，下面的处理逻辑会将当前节点归为 head
                source_ids.append(_from_ids[0])

        # 遍历节点容器
        arr = source_ids
        # 存放连通路径下的头结点和尾节点
        heads = []
        tails = []

        # 从源数据开始，向下查找，当子节点在 nodes 列表中，则子节点为 heads 成员
        while arr:
            _id = arr.pop()
            _to_ids = links_index[_id] if _id in links_index else []
            for _to_id in _to_ids:
                if _to_id in node_ids and _to_id not in heads:
                    heads.append(_to_id)
                elif _to_id not in heads:
                    # 若_to_id不在heads里, 才允许将其作为源节点继续找
                    arr.append(_to_id)

        # 从 heads 开始，向下查找，当子节点不在 nodes 列表中，则该节点为 tails 成员
        arr = copy.deepcopy(heads)
        while arr:
            _id = arr.pop()
            _to_ids = links_index[_id] if _id in links_index else []
            _mine_to_ids = [_id for _id in _to_ids if _id in node_ids]
            if _mine_to_ids:
                arr.extend(_mine_to_ids)
            elif _id not in tails:
                tails.append(_id)

        head_nodes = []
        tail_nodes = []
        for _n in nodes:
            if _n.node_id in heads:
                head_nodes.append(_n)
            if _n.node_id in tails:
                tail_nodes.append(_n)

        return head_nodes, tail_nodes

    @classmethod
    def get_exc_info(cls, consuming_mode):
        offset = 1
        if consuming_mode == "from_tail":
            offset = 0
        elif consuming_mode == "from_head":
            offset = -1
        return offset

    @classmethod
    def get_or_create_steam_job_id(
        cls,
        project_id,
        o_nodes,
        head_rts,
        tail_rts,
        geog_area_code,
        operator,
        consuming_mode="continue",
        cluster_group=None,
    ):
        """
        获取或创建 stream / stream_skd 任务的 job_id 公共方法
        @param project_id:
        @param o_nodes:
        @param head_rts:
        @param tail_rts:
        @param geog_area_code:
        @param operator:
        @param consuming_mode:
        @param cluster_group:
        @return:
        """
        job_id = None
        concurrency = None
        # 收集实时所有job_id，理论上应该一样
        for _stream_node in o_nodes:
            node_job = _stream_node.get_job()
            # 若获取到job_id，则返回
            # 这里不作抛出异常，保证任务可启动
            if node_job is not None and node_job.job_id is not None:
                job_id = node_job.job_id
                break

        offset = cls.get_exc_info(consuming_mode)
        job_type = "stream"
        params = {
            "project_id": project_id,
            "code_version": None,
            "cluster_group": cluster_group,
            "job_config": {
                "heads": ",".join(head_rts),
                "tails": ",".join(tail_rts),
                "concurrency": concurrency,
                "offset": offset,  # 0表示从尾部消费，1表示从继续消费(默认), -1表示从最早数据消费
            },
            "deploy_config": None,
            "processings": [_n.processing_id for _n in o_nodes],
            "jobserver_config": {
                "geog_area_code": geog_area_code,
                "cluster_id": JobNaviHelper.get_jobnavi_cluster("stream"),
            },
        }

        if job_id is None:
            # 若是创建新作业，则指定component_type为flink
            # 为什么创建processing时传入component_type，这里还要传一次，因为"以后可能需要通过job这个层面决定计算引擎"
            # 窃以为通过processing配置去决定计算引擎更好
            params["component_type"] = o_nodes[0].component_type
            o_job = StreamJob.create(params, operator)
            job_id = o_job.job_id
        else:
            o_job = StreamJob(job_id=job_id)
            o_job.update(params, operator)

        # 将job_id保存到flow表，仅记录实时节点
        for _n in o_nodes:
            NodeUtils.update_job_id(_n.flow_id, _n.node_id, job_id, job_type)

        return job_id

    @staticmethod
    def get_stream_job_id(o_nodes):
        """
        get_or_create_stream_job_id 做的事情太多，停止任务的时候不应该调用 ger_or_create_job_id 来获取 job_id
        """
        # 收集实时所有job_id，理论上应该一样
        job_id = None
        for _stream_node in o_nodes:
            node_job = _stream_node.get_job()
            if node_job is not None and node_job.job_id is not None:
                job_id = node_job.job_id
                break
        return job_id

    @auto_meta_sync(using="default")
    def sync_status(self):
        """
        将节点状态同步至 DataFlow，若节点中有 running，则 DataFlow.status=RUNNING，否则
        DataFlow.status = NO_START
        """
        self.status = FlowInfo.STATUS.NO_START
        for _n in self.nodes:
            if _n.status in [FlowInfo.STATUS.RUNNING, FlowInfo.STATUS.FAILURE]:
                self.status = FlowInfo.STATUS.RUNNING
                break

    def update_nodes(self, username, nodes):
        """
        图操作，批量更新节点前端配置
        """
        for _n in nodes:
            _o_node = self.get_node(_n["node_id"])
            params = LocationSerializer(data=_n["frontend_info"])
            params.is_valid(raise_exception=True)
            _o_node.frontend_info = params.validated_data
        return {"version": self.version}

    def check_node_id_in_flow(self, node_id):
        """
        检查节点是否在 Flow 中
        node_id: int类型
        """
        if node_id in self.node_ids:
            return True
        else:
            raise NodeValidError(_("节点(id:{node_id})不属于该Flow").format(node_id=node_id))

    def delete_lines(self, username, lines):
        """
        图操作，批量移除连线
        """
        status = {}
        for _l in lines:
            from_node_id = int(_l["from_node_id"])
            to_node_id = int(_l["to_node_id"])
            self.check_node_id_in_flow(from_node_id)
            self.check_node_id_in_flow(to_node_id)

            # 检查节点状态
            from_node = self.get_node(from_node_id)
            to_node = self.get_node(to_node_id)

            if from_node.status in [FlowInfo.STATUS.RUNNING] and to_node.status in [FlowInfo.STATUS.RUNNING]:
                raise RunningLinkRemoveError()

            self.flow.delete_line(from_node_id, to_node_id)
            status[to_node_id] = to_node.display_status

        return {"version": self.version, "status": status}

    def create_lines(self, username, lines):
        """
        图操作，批量创建连线
        """
        status = {}
        for _l in lines:
            # 检查node_id是否合法
            from_node_id = int(_l["from_node_id"])
            to_node_id = int(_l["to_node_id"])
            self.check_node_id_in_flow(from_node_id)
            self.check_node_id_in_flow(to_node_id)

            # 检查节点状态
            from_node = self.get_node(from_node_id)
            to_node = self.get_node(to_node_id)

            # 运行节点之间不允许连线
            if from_node.status in [FlowInfo.STATUS.RUNNING] and to_node.status in [FlowInfo.STATUS.RUNNING]:
                raise RunningLinkCreateError()

            self.flow.create_line(from_node_id, to_node_id, _l["frontend_info"])
            status[to_node_id] = to_node.display_status

        return {"version": self.version, "status": status}

    def save_draft(self, draft):
        """
        保存草稿信息，回显图形时需要
        """
        self.flow.draft_info = draft
        self.flow.save()

    @property
    def custom_calculate_id(self):
        return self.flow.custom_calculate_id

    def get_custom_calculate_info(self):
        if self.custom_calculate_task:
            context = json.loads(self.custom_calculate_task.context)
            batch_context = context["tasks"]["batch"]
            status = self.custom_calculate_task.status
            if status in FlowExecuteLog.ACTIVE_STATUS:
                status = FlowExecuteLog.STATUS.RUNNING
            elif status not in FlowExecuteLog.DEPLOY_BEFORE_STATUS:
                status = "none"
            custom_calculate_info = {
                "status": status,
                "config": {
                    "node_ids": batch_context["node_ids"],
                    "start_time": batch_context["start_time"],
                    "end_time": batch_context["end_time"],
                    "with_child": batch_context["with_child"],
                },
                # 若存在上次补算审批不通过的情况，返回描述信息
                "message": self.custom_calculate_task.description,
            }
        else:
            custom_calculate_info = {"status": "none", "config": {}, "message": ""}
        return custom_calculate_info

    def cancel_custom_calculate_job(self):
        task_info = self.custom_calculate_task
        if not task_info:
            logger.error("撤销审批操作错误，返回为空，当前任务补算ID(%s)" % self.custom_calculate_id)
        else:
            if task_info.status not in [
                FlowExecuteLog.STATUS.APPLYING,
                FlowExecuteLog.STATUS.READY,
            ]:
                if task_info.status in FlowExecuteLog.ACTIVE_STATUS:
                    raise FlowCustomCalculateNotAllowed(_("当前补算任务开始执行，不可撤销，请手动执行停止操作"))
                elif task_info.status == FlowExecuteLog.STATUS.CANCELLED:
                    raise FlowCustomCalculateNotAllowed(_("当前补算任务已撤销，不可重复撤销"))
                elif task_info.status == FlowExecuteLog.STATUS.TERMINATED:
                    raise FlowCustomCalculateNotAllowed(_("当前补算任务已终止，不可撤销"))
                else:
                    raise Exception(_("未定义补算状态(%s)") % task_info.STATUS)
            if task_info.status == FlowExecuteLog.STATUS.APPLYING:
                context = task_info.get_context()
                if context:
                    # 补算作业有上下文信息就必须有相应的数据格式信息
                    ticket_id = context["settings"]["ticket_id"]
                    process_message = "DataFlow cancel custom calculate job"
                    AuthHelper.tickets_withdraw(ticket_id, process_message, check_success=False)
            task_info.set_status(FlowExecuteLog.STATUS.CANCELLED)
            self.unlock()

    @staticmethod
    def get_custom_cal_downstream_nodes(node):
        """
        获取节点的所有下游节点(仅支持补算类型的)
        @param node:
        @return:
        """
        filter_node_types = NodeTypes.SUPPORT_RECALC_CATEGORY + [NodeTypes.PROCESS_MODEL + NodeTypes.MODEL_TS_CUSTOM]
        arr = [node]
        nodes = {node.node_id: node}
        # 这里采用循环，主要是为了同时避免环的存在导致死循环，不建议使用递归
        while arr:
            _n = arr.pop()
            for _to_node in _n.get_to_nodes_handler():
                if _to_node.node_id in nodes:
                    continue
                arr.append(_to_node)
                if _to_node.node_type not in filter_node_types:
                    continue
                if _to_node.node_type in [
                    NodeTypes.PROCESS_MODEL,
                    NodeTypes.MODEL_TS_CUSTOM,
                ]:
                    if not DataProcessingHelper.is_model_serve_mode_offline(
                        ProcessingNode(_to_node.node_id).processing_id
                    ):
                        continue
                nodes[_to_node.node_id] = _to_node
        return list(nodes.values())

    @staticmethod
    def build_custom_calculate_job_context(operator, flow_id, node_ids, start_time, end_time, with_child):
        custom_calculate_nodes = []
        iceberg_transform_rts = DatabusHelper.get_json_conf_value("iceberg.transform.rts")

        for node_id in set(node_ids):
            node = NODE_FACTORY.get_node_handler(node_id)
            if str(node.flow_id) != str(flow_id):
                logger.error("node_id: {}, node flow_id: {}, apply flow_id: {}".format(node_id, node.flow_id, flow_id))
                raise FlowCustomCalculateNotAllowed(_("所选补算节点%(node_name)s不属于当前任务") % {"node_name": node.name})
            if node.node_type not in NodeTypes.SUPPORT_RECALC_CATEGORY:
                if node.node_type in [
                    NodeTypes.PROCESS_MODEL,
                    NodeTypes.MODEL_TS_CUSTOM,
                ]:
                    if not DataProcessingHelper.is_model_serve_mode_offline(ProcessingNode(node.node_id).processing_id):
                        raise FlowCustomCalculateNotAllowed(
                            _("所选补算节点%(node_name)s为实时任务，不支持重算") % {"node_name": node.name}
                        )
                else:
                    nodes_display = ", ".join(
                        [_(NodeTypes.DISPLAY.get(_type, _type)) for _type in NodeTypes.SUPPORT_RECALC_CATEGORY]
                    )
                    raise FlowCustomCalculateNotAllowed(
                        _("所选补算节点%(node_name)s不支持重算，当前仅支持%(nodes_display)s节点")
                        % {"node_name": node.name, "nodes_display": nodes_display}
                    )
            if node.status not in [FlowInfo.STATUS.RUNNING]:
                raise FlowCustomCalculateNotAllowed(_("所选补算节点%(node_name)s不处于运行状态，不支持补算") % {"node_name": node.name})
            if iceberg_transform_rts:
                transform_rts = [item for item in node.result_table_ids if item in iceberg_transform_rts]
                if transform_rts:
                    raise FlowCustomCalculateNotAllowed(
                        _("补算的节点%(node_name)s存在iceberg迁移中的数据%(rt)s（暂不支持补算），请重新调整补算申请")
                        % {"node_name": node.name, "rt": ",".join(transform_rts)}
                    )
            custom_calculate_nodes.append(node)
        downstream_nodes = []
        if with_child:
            for _n in custom_calculate_nodes:
                _n_downstream_nodes = FlowHandler.get_custom_cal_downstream_nodes(_n)
                downstream_nodes.extend(_n_downstream_nodes)
        custom_calculate_nodes.extend(downstream_nodes)
        graph_custom_calculate_nodes = {}
        # 去除 custom_calculate_nodes 中的重复节点及下游非运行中节点
        for _n in custom_calculate_nodes:
            if _n.status in [FlowInfo.STATUS.RUNNING] and _n.node_id not in graph_custom_calculate_nodes:
                graph_custom_calculate_nodes[_n.node_id] = _n
        rerun_processings = []
        for _n in list(graph_custom_calculate_nodes.values()):
            rerun_processings.append(_n.processing_id)
        data_start = ApplyCustomCalculateJobSerializer.get_format_time(start_time)
        data_end = ApplyCustomCalculateJobSerializer.get_format_time(end_time)
        if data_start >= data_end:
            raise FlowCustomCalculateError(_("补算起始时间应小于截止时间"))
        # 06:59 -> 07:00，07:00 -> 08:00，07:01 -> 08:00
        if data_end > (int(time.time()) / 3600 + 1) * 3600 * 1000:
            raise FlowCustomCalculateError(_("补算截止时间应不能大于当前小时"))
        return {
            "settings": {"operators": [operator], "ticket_id": None},
            "tasks": {
                "batch": {
                    "custom_calculate_node_ids": list(graph_custom_calculate_nodes.keys()),
                    "start_time": start_time,
                    "end_time": end_time,
                    "data_start": data_start,
                    "data_end": data_end,
                    "rerun_processings": rerun_processings,
                    "node_ids": node_ids,
                    "with_child": with_child,
                }
            },
            "progress": 10.0,  # 补算作业初始进度值
        }

    def apply_confirm_custom_calculate_job(self, custom_calculate_id, operator, message, confirm_type):
        logger.info("审批结果回调或权限管理撤销：{}, {}, {}, {}".format(custom_calculate_id, operator, message, confirm_type))
        if not self.custom_calculate_task:
            raise FlowCustomCalculateError("Custom calculate job of current flow(%s) is not exist." % self.flow_id)
        if self.custom_calculate_task.id != int(custom_calculate_id):
            # 若旧审批的补算任务已经被取消，正常返回即可
            old_custom_calculate = FlowExecuteLog.objects.filter(id=int(custom_calculate_id))
            if old_custom_calculate.exists() and old_custom_calculate[0].status == FlowExecuteLog.STATUS.CANCELLED:
                return {"message": "The custom_calcuate task has been cancelled."}
            raise FlowCustomCalculateError(
                "Apply confirm is not allowed for the custom_calculate_id(%s) of flow_id(%s) and id(%s) are mismatch."
                % (self.custom_calculate_task.id, self.flow_id, custom_calculate_id)
            )
        if self.custom_calculate_task.status != FlowExecuteLog.STATUS.APPLYING:
            msg = (
                "Apply_confirm with id(%s) is not allowed for the status of custom_calculate task is not "
                "applying." % custom_calculate_id
            )
            logger.info(msg)
            return {"message": msg}
        if confirm_type == ApplyCustomCalculateJobConfirmSerializer.STATUS.APPROVED:
            logger.info("{}审批通过: {}".format(operator, custom_calculate_id))
            self.custom_calculate_task.status = FlowExecuteLog.STATUS.READY
            self.custom_calculate_task.save()
        else:
            logger.info("{}审批不通过: {}, {}, {}".format(operator, custom_calculate_id, message, confirm_type))
            self.custom_calculate_task.status = FlowExecuteLog.STATUS.CANCELLED
            if confirm_type == ApplyCustomCalculateJobConfirmSerializer.STATUS.REJECTED:
                # description 用于记录返回给前端的审批信息
                self.custom_calculate_task.description = _("上次补算审批不通过，审批人：%s") % operator
            self.custom_calculate_task.save()
            self.unlock()

    def apply_custom_calculate_job(self, node_ids, start_time, end_time, with_child):
        operator = get_request_username()
        context = FlowHandler.build_custom_calculate_job_context(
            operator, self.flow_id, node_ids, start_time, end_time, with_child
        )
        kwargs = {
            "flow_id": self.flow_id,
            "created_by": operator,
            "action": FlowExecuteLog.ACTION_TYPES.CUSTOM_CALCULATE,
            "status": FlowExecuteLog.STATUS.APPLYING,
            "context": json.dumps(context),
        }
        task = FlowExecuteLog.objects.create(**kwargs)
        custom_calculate_id = task.id
        self.flow.partial_update({"custom_calculate_id": custom_calculate_id})
        process_id = "{}#{}".format(self.flow_id, custom_calculate_id)

        ticket_type = "batch_recalc"
        # 当前补算仅有离线计算
        result_table_ids = []
        custom_calculate_node_ids = context["tasks"]["batch"]["custom_calculate_node_ids"]
        rerun_processings = ",".join(context["tasks"]["batch"]["rerun_processings"])
        data_start = context["tasks"]["batch"]["data_start"]
        data_end = context["tasks"]["batch"]["data_end"]
        for custom_calculate_node_id in custom_calculate_node_ids:
            result_table_ids.extend(NODE_FACTORY.get_node_handler(custom_calculate_node_id).result_table_ids)

        content = _("任务(%(flow_id)s)申请对结果表(%(result_table_ids)s)进行补算，时间范围(%(start_time)s, %(end_time)s)") % {
            "flow_id": self.flow_id,
            "result_table_ids": "、".join(result_table_ids),
            "start_time": start_time,
            "end_time": end_time,
        }
        geog_area_code = self.geog_area_codes[0]
        try:
            custom_calculate_res = BatchHelper.analyze_custom_calculate(
                rerun_processings, data_start, data_end, geog_area_code
            )
        except Exception as e:
            task.set_status(FlowExecuteLog.STATUS.CANCELLED)
            task.description = _("内部模块调用异常，BatchHelper.analyze_custom_calculate调用异常{}".format(e))
            task.save()
            raise FlowCustomCalculateNotAllowed(_("%(reason)s") % {"reason": task.description})
        job_count = len(custom_calculate_res)
        reason = _("用户 %(operator)s 申请对项目(%(project_name)s)-任务(%(flow_name)s)进行补算，总计%(job_count)s个作业数") % {
            "operator": operator,
            "project_name": self.project_name,
            "flow_name": self.flow_name,
            "job_count": job_count,
        }
        if job_count >= 1000:
            task.set_status(FlowExecuteLog.STATUS.CANCELLED)
            task.description = _("作业数(%s)大于等于1000，请重新调整分批次申请补算") % job_count
            task.save()
            raise FlowCustomCalculateNotAllowed(_("%(reason)s，作业数大于等于1000，请重新调整分批次申请补算") % {"reason": reason})

        has_month_calc = False
        calc_job_count = 0
        for one_custom_calculate_res in custom_calculate_res:
            one_processing_id = one_custom_calculate_res["schedule_id"]
            schedule_period = None
            processing_batch_info = ProcessingJobInfoHandler.get_proc_job_info(one_processing_id)
            if processing_batch_info.job_config is not None:
                job_config = json.loads(processing_batch_info.job_config)
                if "submit_args" in job_config:
                    submit_args = json.loads(job_config["submit_args"])
                    if "schedule_period" in submit_args:
                        schedule_period = submit_args["schedule_period"]
            if schedule_period == "hour":
                calc_job_count = calc_job_count + 1
            elif schedule_period == "day":
                calc_job_count = calc_job_count + 1
            elif schedule_period == "week":
                calc_job_count = calc_job_count + 7
            elif schedule_period == "month":
                has_month_calc = True
                break

        auto_approve = False
        ticket_step_params = {}
        if not has_month_calc and calc_job_count < 65:
            auto_approve = True
        if auto_approve:
            ticket_step_params["0&paased_by_system"] = True

        logger.info(
            "calc_job_count(%s), has_month_calc(%s), auto_approve(%s), reason(%s)"
            % (calc_job_count, has_month_calc, auto_approve, reason)
        )

        res_data = AuthHelper.tickets_create_common(
            process_id, ticket_type, reason, content, ticket_step_params, auto_approve
        )
        context["settings"]["ticket_id"] = res_data["id"]
        task.save_context(context)
        node_info = {}
        for node_id in context["tasks"]["batch"]["custom_calculate_node_ids"]:
            node_info[node_id] = {
                "status": FlowExecuteLog.STATUS.APPLYING,
            }
        res = {
            "status": FlowExecuteLog.STATUS.READY if auto_approve else FlowExecuteLog.STATUS.APPLYING,
            "node_info": node_info,
        }
        return res

    def export(self):
        nodes = []
        nodes_obj = {}
        # 格式: {'to_node_id': ['from_node_id1', 'from_node_id2']}
        to_link_info = self._get_links_index(reverse=True)
        for _n in self.nodes:
            # 缓存节点类
            nodes_obj[_n.node_id] = _n
            config = _n.get_config()

            # get current node's from_result_table_id
            current_from_result_table_ids = []
            for from_result_table in NodeUtils.get_input_nodes_config(_n.node_type, config):
                current_from_result_table_ids.extend(from_result_table["from_result_table_ids"])

            # if _n.node_type in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY:
            #     for from_result_table in config['from_nodes']:
            #         current_from_result_table_ids.extend(from_result_table['from_result_table_ids'])
            # elif 'from_result_table_ids' in config:
            #     current_from_result_table_ids = config['from_result_table_ids']

            config["id"] = _n.node_id
            from_nodes = []
            for from_node_id in to_link_info.get(_n.node_id, []):
                from_obj = nodes_obj.setdefault(from_node_id, NODE_FACTORY.get_node_handler(from_node_id))
                from_nodes.append(
                    {
                        "id": from_node_id,
                        "from_result_table_ids": list(
                            set(from_obj.result_table_ids) & set(current_from_result_table_ids)
                        ),
                    }
                )
            # config = NodeUtils.set_input_nodes_config(_n.node_type, config, from_nodes)
            config["from_nodes"] = from_nodes
            config["node_type"] = _n.node_type
            config["frontend_info"] = _n.frontend_info
            # 移除一些不必要的返回信息，防止后续使用该字段
            if "from_result_table_ids" in config:
                del config["from_result_table_ids"]
            if "inputs" in config:
                del config["inputs"]
            nodes.append(config)
        return {"project_id": self.project_id, "flow_name": self.name, "nodes": nodes}

    def _get_links_index(self, reverse=False):
        """
        连线加索引, 默认 from_id -> [to_id, ]
        @param reverse: 是否倒序
        @return:
        """
        links_index = {}
        for link in self.links:
            if not reverse:
                from_id, to_id = link.from_node_id, link.to_node_id
            else:
                from_id, to_id = link.to_node_id, link.from_node_id
            if from_id not in links_index:
                links_index[from_id] = []
            links_index[from_id].append(to_id)
        return links_index

    def _gene_default_frontend_info(self):
        self.init_cache()
        node_count = len(self.nodes)
        if node_count > 0:
            pre_x = self.nodes[node_count - 1].get_frontend_info()["x"]
        else:
            pre_x = 0
        # 测试中发现，若x太大，画布显示不了，为了避免这种情况，加一个最大值进行限制
        if pre_x < 1200:
            x = pre_x + 150
        else:
            x = 1200
        return {"x": x, "y": 100}

    @property
    def nodes(self):
        """
        @return {Array.<NodeHandler>}
        """
        if self._nodes is None:
            self._nodes = [NODE_FACTORY.get_node_handler_by_node(_node) for _node in self.flow.nodes]
        return self._nodes

    @property
    def node_ids(self):
        if self._node_ids is None:
            self._node_ids = [_node.node_id for _node in self.flow.nodes]
        return self._node_ids

    @property
    def links(self):
        if self._links is None:
            self._links = self.flow.links

        return self._links

    @property
    def flow(self):
        if self._flow is None:
            try:
                self._flow = FlowInfo.objects.get(flow_id=self.flow_id)
            except FlowInfo.DoesNotExist:
                raise FlowNotFoundError()

        return self._flow

    @property
    def version(self):
        return self.flow.latest_version

    @property
    def code_version(self):
        return "master"

    @property
    def workers(self):
        return self.flow.workers

    @property
    def manage_tags(self):
        if self._manage_tags is None:
            manage_tags = ProjectHelper.get_project(self.project_id).get("tags").get("manage", {})
            self._manage_tags = manage_tags
        return self._manage_tags

    @property
    def project_id(self):
        return self.flow.project_id

    @property
    def flow_name(self):
        return self.flow.flow_name

    @property
    def project_name(self):
        return ProjectHelper.get_project(self.project_id)["project_name"]

    @property
    def geog_area_codes(self):
        tag_codes = [_info["code"] for _info in self.manage_tags.get("geog_area", [])]
        if not tag_codes:
            raise FlowError(_("当前任务所属项目地域信息不存在"))
        if len(tag_codes) > 1:
            raise FlowError(_("当前任务所属项目地域信息不唯一"))
        return tag_codes

    @property
    def recalculate_id(self):
        return self.flow.recalculate_id

    @recalculate_id.setter
    @auto_meta_sync(using="default")
    def recalculate_id(self, recalculate_id):
        self.flow.recalculate_id = recalculate_id
        self.flow.save()

    def get_recalculate_basic_info(self):
        if not self.recalculate_id:
            return {"status": "none"}
        data = BatchHelper.get_custom_calculate_basic_info(self.recalculate_id)
        return data

    @property
    def custom_calculate_task(self):
        if self._custom_calculate_task is not None:
            return self._custom_calculate_task
        if not self.custom_calculate_id:
            return None
        try:
            self._custom_calculate_task = FlowExecuteLog.objects.get(id=int(self.custom_calculate_id))
            return self._custom_calculate_task
        except FlowExecuteLog.DoesNotExist:
            return None

    def set_custom_calculate_status(self, status):
        self.custom_calculate_task.set_status(status)

    @property
    def custom_calculate_status(self):
        if not self.custom_calculate_id:
            return None
        return self.custom_calculate_task.status

    @property
    def name(self):
        return self.flow.flow_name

    @property
    def status(self):
        return self.flow.status

    @status.setter
    @auto_meta_sync(using="default")
    def status(self, _s):
        self.flow.status = _s
        self.flow.save()

    @property
    def process_status(self):
        return self.flow.process_status

    @property
    def has_exception(self):
        """
        标识当前flow是否处于异常状态，不考虑孤立的节点
        异常条件：
            1. flow启动完成(状态为RUNNING)，存在节点不为RUNNING
        @return:
        """
        if self.status == FlowInfo.STATUS.RUNNING:
            try:
                # 并非flow中所有的节点都需要启动，譬如
                # 1. A -> B, 删除了A，则B无需启动(此时B不属于nodes_to_execute)
                nodes_to_execute = [_n.node_id for _n in self.get_ordered_nodes()]
                # 2. A -> B -> C, A和B处于运行状态，C在运行时增加，这是C的状态不是STARTING
                # 因此，如果当前节点仍处于 STARTING 或 STOPPING 或 FAILURE 状态且是需要运行的节点，表示该节点异常，整个flow也就异常
                if list(
                    filter(
                        lambda _n: _n.status
                        in [
                            FlowInfo.STATUS.STARTING,
                            FlowInfo.STATUS.STOPPING,
                            FlowInfo.STATUS.FAILURE,
                        ]
                        and _n.node_id in nodes_to_execute,
                        self.nodes,
                    )
                ):
                    return True
            except Exception as e:
                # 该方法的本意为判断 flow 是否异常，为避免在获取 flow 节点是否异常的过程中抛出了未知异常
                # 导致无法获取 flow 的异常状态，这里主动进行 try...except...操作
                logger.error("获取 flow 状态过程捕获未知异常: {}".format(e))
                return True
        return False

    @classmethod
    def filter_exception_flows(cls, flow_ids):
        """
        根据状态信息批量过滤出有异常的 flow，同时考虑孤立的节点(从查询性能的角度，批量接口暂不考虑孤立节点的情况)
        @return:
        """
        running_flow_ids = FlowInfo.objects.filter(flow_id__in=flow_ids, status=FlowInfo.STATUS.RUNNING).values_list(
            "flow_id", flat=True
        )
        has_exception_flow_ids = FlowNodeInfo.objects.filter(
            status__in=[
                FlowInfo.STATUS.STARTING,
                FlowInfo.STATUS.STOPPING,
                FlowInfo.STATUS.FAILURE,
            ],
            flow_id__in=running_flow_ids,
        ).values_list("flow_id", flat=True)

        return has_exception_flow_ids

    @property
    def node_count(self):
        return self.flow.node_count

    @property
    def is_locked(self):
        return self.flow.is_locked

    @staticmethod
    def wrap_exception(data, many=False):
        """
        封装 Flow 内容，添加异常字段 has_exception，标识 Flow 是否异常

        @param {Dict | [Dict]} data 字典数据或者列表数据
        @param {Boolean} many 是否为列表数据
        """
        if many:
            m_flows = {f["flow_id"]: f for f in data}
            flow_ids = list(m_flows.keys())
            # 非运行中flow无需获取告警信息
            alert_flows = {}
            for flow_id in m_flows:
                if m_flows[flow_id]["status"] == FlowInfo.STATUS.RUNNING:
                    alert_flows[flow_id] = m_flows[flow_id]
            exception_flow_ids = FlowHandler.filter_exception_flows(flow_ids)
            for flow_id in flow_ids:
                m_flows[flow_id]["has_exception"] = flow_id in exception_flow_ids
        else:
            flow_id = data["flow_id"]
            alert_flows = {}
            if data["status"] == FlowInfo.STATUS.RUNNING:
                alert_flows[flow_id] = data
            data["has_exception"] = FlowHandler(flow_id).has_exception

        return data

    @staticmethod
    def wrap_node_count(data, many=False):
        """
        封装 Flow 内容，添加节点数量字段
        @param {Dict | [Dict]} data 字典数据或者列表数据
        @param {Boolean} many 是否为列表数据
        """
        if many:
            m_flows = {f["flow_id"]: f for f in data}
            flow_node_counts = FlowNodeInfo.objects.values("flow_id").annotate(node_count=Count("flow_id"))
            flow_id_node_count_map = {_f["flow_id"]: _f["node_count"] for _f in flow_node_counts}
            for flow_id, m_flow in list(m_flows.items()):
                m_flow["node_count"] = flow_id_node_count_map.get(flow_id, 0)
        else:
            flow_id = data["flow_id"]
            data["node_count"] = FlowHandler(flow_id).node_count
        return data

    @staticmethod
    def wrap_process_status(data, many=False):
        """
        封装 Flow 内容，添加节点数量字段
        @param {Dict | [Dict]} data 字典数据或者列表数据
        @param {Boolean} many 是否为列表数据
        """
        if many:
            m_flows = {f["flow_id"]: f for f in data}
            latest_task_ids = FlowExecuteLog.objects.values("flow_id").annotate(latest_task_id=Max("id"))
            ids = [_id["latest_task_id"] for _id in latest_task_ids]
            flow_process_status = FlowExecuteLog.objects.filter(id__in=ids).values("flow_id", "status")

            flow_id_process_status_map = {_f["flow_id"]: _f["status"] for _f in flow_process_status}
            for flow_id, m_flow in list(m_flows.items()):
                m_flow["process_status"] = flow_id_process_status_map.get(flow_id, "no-status")
        else:
            flow_id = data["flow_id"]
            data["process_status"] = FlowHandler(flow_id).process_status
        return data

    @staticmethod
    def wrap_custom_calculate(data, many=False):
        if many:
            # 暂不支持获取多个 flow 的补算状态
            pass
        else:
            flow_id = data["flow_id"]
            data["custom_calculate"] = FlowHandler(flow_id).get_custom_calculate_info()

    @staticmethod
    def wrap_display(data, many=False):
        if many:
            # project_ids = list(set([f['project_id'] for f in data]))
            # 这里暂不抽象出ProjectHandler，因为只有这里用到project
            project_config_list = ProjectHelper.get_multi_project()
            project_config_dict = {}
            for project_config in project_config_list:
                project_config_dict[project_config["project_id"]] = project_config["project_name"]
            # 删项目不会删flow，因此返回时需过滤无效项目对应的flow
            active_project_flow_data = []
            for m_flow in data:
                project_id = m_flow["project_id"]
                if project_id not in project_config_dict:
                    logger.warning(_("相应项目元数据信息不存在，项目已被删除，project_id=%s") % project_id)
                    continue
                m_flow["project_name"] = project_config_dict[project_id]
                active_project_flow_data.append(m_flow)
            data[:] = active_project_flow_data
        else:
            flow_id = data["flow_id"]
            data["project_name"] = FlowHandler(flow_id).project_name
        return data

    @staticmethod
    def wrap_extra_info(data, wrap_extra_info, many=False):
        """
        封装 Flow 内容，添加节点数量字段
        @param {Dict | [Dict]} data 字典数据或者列表数据
        @param {Dict} wrap_extra_info 添加额外的数据，如exception，node_count等
        @param {Boolean} many 是否为列表数据
        """
        if wrap_extra_info.get("add_exception_info"):
            FlowHandler.wrap_exception(data, many=many)
        if wrap_extra_info.get("add_node_count_info"):
            FlowHandler.wrap_node_count(data, many=many)
        if wrap_extra_info.get("add_process_status_info"):
            FlowHandler.wrap_process_status(data, many=many)
        if wrap_extra_info.get("show_display"):
            FlowHandler.wrap_display(data, many=many)
        if wrap_extra_info.get("add_custom_calculate_info"):
            FlowHandler.wrap_custom_calculate(data, many=many)
        return data

    @staticmethod
    def md5_for_file(f):
        md5 = hashlib.md5()
        for chunk in f.chunks():
            md5.update(chunk)
        return md5.hexdigest()

    @transaction.atomic()
    def upload_file(self, username, flow_id, file, suffix, description, path=None):
        """
        @param username:
        @param flow_id:
        @param file:
        @param suffix:
        @param description:
        @param path:
        @return:
        """
        if path is None:
            tmp_id = str(uuid.uuid4())
            path = "{}{}".format(HDFS_UPLOADED_FILE_TMP_DIR, tmp_id)
        hdfs_ns_id = StorekitHelper.get_default_storage("hdfs", self.geog_area_codes[0])["cluster_group"]
        res_data = ComponentHelper.hdfs_upload(hdfs_ns_id, path, file)
        md5_value = res_data["md5"]
        # 插入文件信息
        kwargs = {
            "name": file.name,
            "suffix": suffix,
            "flow_id": flow_id,
            "path": path,
            "md5": md5_value,
            "created_by": username,
            "updated_by": username,
            "description": description,
        }
        file_info_obj = FlowFileUpload.objects.create(**kwargs)
        return model_to_dict(file_info_obj)

    def check_tdw_conf(self, tdw_conf, is_create=False):
        if not is_create:
            tdw_nodes = list(
                filter(
                    lambda _n: _n.node_type in NodeTypes.TDW_CATEGORY,
                    self.nodes,
                )
            )
            if not tdw_conf and tdw_nodes:
                # 页面有任意tdw节点时，更新时不可以关闭 TDW 支持
                raise FlowError(_("当前任务已配置TDW节点，不可关闭TDW支持"))
            # TDW 任务启动成功过(避免是因为配置错误导致了启动失败)不可修改应用组、源服务器、集群版本、计算集群等信息
            # ==> 等价于若 TDW 任务中 存在 TDW 节点 running_version 不为空，则不可修改
            origin_tdw_conf = self.flow.get_tdw_conf()
            if origin_tdw_conf and [_n for _n in tdw_nodes if _n.running_version]:
                if not tdw_conf:
                    raise FlowError(_("当前 TDW 任务启动过，不可关闭 TDW 支持"))
                if tdw_conf["tdwAppGroup"] != origin_tdw_conf["tdwAppGroup"]:
                    raise FlowError(_("当前 TDW 任务启动过，不可更改应用组"))

    def create_default_tdw_storage(self, result_table_id, cluster_id):
        storage_type = "tdw"
        # 组装创建存储所需参数
        rt_storages_relation = {
            "result_table_id": result_table_id,
            "cluster_name": cluster_id,
            "cluster_type": storage_type,
            "expires": "7d",
            "storage_config": "{}",
            "generate_type": "system",
        }
        StorekitHelper.create_physical_table(**rt_storages_relation)

    def update_tdw_source(self, params):
        """
        @param params:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
                "output_name": "xxx",       # 输出中文名
                "description": "xxx",
                "associated_lz_id": "xxx"
            }
        @return:
        """
        result_table_id = "{}_{}".format(params["bk_biz_id"], params["table_name"])
        self.perm_check_data(result_table_id)
        rt = ResultTable(result_table_id)
        rt.perm_check_tdw_table()
        rt_params = {"result_table_id": result_table_id}
        # patch 接口，允许只更新部分信息
        if "output_name" in params:
            rt_params.update({"result_table_name_alias": params["output_name"]})
        if "description" in params:
            rt_params.update({"description": params["description"]})
        if "associated_lz_id" in params:
            associated_lz_id = params["associated_lz_id"]
            rt_params.update({"extra": {"tdw": {"associated_lz_id": {"import": associated_lz_id}}}})
            rt_tdw_conf = rt.get_extra("tdw")
            if rt_tdw_conf["associated_lz_id"]:
                origin_lz_id = rt_tdw_conf["associated_lz_id"]["import"]
                # 判断是否修改了 lz_id, 若是则需要触发同步
                if origin_lz_id != associated_lz_id:
                    rt_params["extra"]["tdw"]["usability"] = "Prepared"
        ResultTableHelper.update_result_table(rt_params)
        return {"result_table_id": result_table_id}

    def remove_tdw_source(self, params):
        """
        @param params:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
            }
        @return:
        """
        result_table_id = "{}_{}".format(params["bk_biz_id"], params["table_name"])
        # 删除 RT 是个高危操作
        rt = ResultTable(result_table_id)
        if not rt.is_exist():
            raise FlowError(_("结果表(%s)不存在") % result_table_id)
        if rt.processing_type != "batch":
            raise FlowError(
                _("结果表(%(result_table_id)s)的 processing_type 为(%(processing_type)s), 不允许删除")
                % {
                    "processing_type": rt.processing_type,
                    "result_table_id": result_table_id,
                }
            )
        FlowHandler.validate_related_tdw_source(result_table_id)
        self.perm_check_data(result_table_id)
        rt.perm_check_tdw_table()
        # 1. 删除 RT 与存储的关联关系
        cluster_id = rt.get_extra("tdw")["cluster_id"]
        StorekitHelper.delete_physical_table(result_table_id, "tdw")
        # 2. 删除 RT，删除失败则回滚
        call_func_with_rollback(
            ResultTableHelper.delete_result_table,
            {"result_table_id": result_table_id},
            [
                {
                    "func": self.create_default_tdw_storage,
                    "params": {
                        "result_table_id": result_table_id,
                        "cluster_id": cluster_id,
                    },
                }
            ],
        )

    def create_tdw_source(self, params):
        """
        创建 tdw 存量表对应的RT，目前暂时将逻辑放在 flowAPI
        @param params:
            {
                "bk_biz_id": 1,             # 业务号
                "table_name": "xxx",        # 表名
                "cluster_id": "xx",         # 集群ID
                "db_name": "xxx",           # 数据库名称
                "tdw_table_name": "xxx"     # 存量表名
                "output_name": "xxx",       # 输出中文名
                "description": "xxx",
                "associated_lz_id": "xxx"   # 可有可没有
            }
        @return:
        """
        cluster_id = params["cluster_id"]
        db_name = params["db_name"]
        table_name = params["tdw_table_name"]
        AuthHelper.check_tdw_table_perm(cluster_id, db_name, table_name)
        bk_biz_id = params["bk_biz_id"]
        result_table_id = "{}_{}".format(bk_biz_id, params["table_name"])
        # 1. 创建 TDW 存量表对应 RT
        rt_params = {
            "bk_biz_id": bk_biz_id,
            "result_table_id": result_table_id,
            "result_table_type": None,
            "result_table_name": params["table_name"],
            "result_table_name_alias": params["output_name"],
            "generate_type": "user",
            "processing_type": "batch",
            "sensitivity": "public",
            "platform": "tdw",
            "is_managed": 0,
            "description": params["description"],
            "fields": [],
            "project_id": self.project_id,
            "extra": {
                "tdw": {
                    "usability": "Prepared",
                    "cols_info": [],
                    "cluster_id": cluster_id,
                    "db_name": db_name,
                    "table_name": table_name,
                }
            },
            "tags": self.geog_area_codes,
        }
        if "associated_lz_id" in params:
            rt_params["extra"]["tdw"]["associated_lz_id"] = {"import": params["associated_lz_id"]}
        try:
            ResultTableHelper.set_result_table(result_table=rt_params)
        except BaseAPIError as e:
            if e.code == TDW_RESULT_TABLE_BEEN_USED_ERR:
                raise FlowError(_("当前TDW表及其关联结果表已存在，不可重复标准化"), code=e.code)
            else:
                raise e
        # 2. 创建 rt 与存储的关联关系，创建失败需要回滚
        call_func_with_rollback(
            self.create_default_tdw_storage,
            {"result_table_id": result_table_id, "cluster_id": params["cluster_id"]},
            [
                {
                    "func": ResultTableHelper.delete_result_table,
                    "params": {"result_table_id": result_table_id},
                }
            ],
        )
        return {"result_table_id": result_table_id}

    def related_running_nodes(self, result_table_id, node_types):
        related_nodes = set(
            FlowNodeRelation.objects.filter(node_type__in=node_types, result_table_id=result_table_id).values_list(
                "node_id", flat=True
            )
        )
        # 过滤条件若加上 FlowInfo.STATUS.STARTING|STOPPING，若刚好同时调用可能会有问题，因此不 exclude 该状态
        related_running_nodes = (
            FlowNodeInfo.objects.filter(node_id__in=related_nodes)
            .exclude(status__in=[FlowInfo.STATUS.NO_START])
            .values_list("node_id", flat=True)
        )

        return related_running_nodes

    def latest_execute_log(self, action_types=None, is_done=False):
        latest_field = "created_at"
        if is_done:
            latest_field = "end_time"
        if action_types:
            filter_records = FlowExecuteLog.objects.filter(flow_id=self.flow_id, action__in=action_types)
        else:
            filter_records = FlowExecuteLog.objects.filter(flow_id=self.flow_id)
        latest_record = None
        if filter_records.exists():
            latest_record = filter_records.latest(latest_field)
        return latest_record

    def fetch_all_to_nodes(self, node, all_to_nodes=None):
        """
        获取 node 的所有下游节点
        @param node:
        @param all_to_nodes:
        @return:
            {
                node_id: node_object
            }
        """
        to_nodes = node.get_to_nodes_handler()
        for to_node in to_nodes:
            if to_node.node_id not in all_to_nodes:
                all_to_nodes[to_node.node_id] = to_node
                self.fetch_all_to_nodes(to_node, all_to_nodes)
        return all_to_nodes

    @classmethod
    def check_context_change(cls, current_context, last_context, key):
        """
        校验指定上下文对象相比与当前，是否修改了数据消费模式，计算集群等信息
        @param current_context: 当前启动上下文信息
        @param last_context: 上次启动的所有上下文信息
        @param key: 上次启动的上下文信息 key 值
        @return:
        """
        has_change_info_by_key = False
        if key in last_context:
            has_change_info_by_key = True
            pre_info_by_key = last_context[key]
            if key in current_context:
                current_info_by_key = current_context[key]
                if pre_info_by_key == current_info_by_key:
                    has_change_info_by_key = False
        elif key in current_context:
            has_change_info_by_key = True
        return has_change_info_by_key

    def check_latest_context(self, current_context, node):
        """
        查看上次启动(正常或异常启动)的上下文信息是否发生了变动，若发生变动，判断当前节点所在的任务是否需要重启
        @param node:
        @return:
            若对于当前节点而言，已修改了上下文信息，返回 true，否则返回 false
        """
        context = self.get_latest_deploy_context()
        if not context:
            return True
        # 当前启停操作上下文中包含两个参数(consuming_mode/cluster_group)
        should_restart = False
        # 更换消费模式仅对实时计算有影响
        if node.node_type in [
            NodeTypes.STREAM,
            NodeTypes.DATA_MODEL_APP,
            NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            NodeTypes.FLINK_STREAMING,
            NodeTypes.SPARK_STRUCTURED_STREAMING,
        ]:
            should_restart = should_restart | (current_context.get("consuming_mode") != "continue")
        # 更换集群仅对实时计算和离线计算(不包括TDW)有影响
        if node.node_type in [
            NodeTypes.STREAM,
            NodeTypes.DATA_MODEL_APP,
            NodeTypes.DATA_MODEL_STREAM_INDICATOR,
            NodeTypes.DATA_MODEL_BATCH_INDICATOR,
            NodeTypes.FLINK_STREAMING,
            NodeTypes.SPARK_STRUCTURED_STREAMING,
            NodeTypes.BATCH,
            NodeTypes.BATCHV2,
            NodeTypes.MODEL_APP,
        ]:
            should_restart = should_restart | FlowHandler.check_context_change(
                current_context, context, "cluster_group"
            )
        return should_restart

    def build_nodes_to_restart(self, current_context):
        """
        将所有需要重启的节点检查出来，规则为:
            1. 与运行中的版本相比(不能存在运行中节点删除的情况)为新版本的节点
                1. 新增的节点/连线的所有下游节点(除非其一级下游是支持轻量级重启的节点，并且不向下追溯更下游节点)
                2. 支持轻量级重启的节点及其自身除外，当前仅做了修改的节点/连线及其所有下游节点
            2. 状态不正常/未启动的节点(需要启动，不一定需要停止)
            3. 修改了数据消费模式、计算集群等
            * 除此之外，若非 processing 节点符合上述条件，其所有上游节点中也需要重启(上游合流节点若不符合上述条件，就不需要重启)
        """
        # 如果是重启过程，记录需要重启的节点
        nodes_to_restart = {}

        # 如果是重启过程，记录需要"轻量级重启"的节点
        nodes_to_restart_slightly = {}

        nodes = self.get_ordered_nodes(reverse=True)
        # 正序查找减少下游节点的版本校验
        for _n in nodes[::-1]:
            if _n.node_id not in nodes_to_restart:
                # strong_restart_confirm 为 true 标明当前节点或状态异常，或为新节点，一定需要重启
                strong_restart_confirm = _n.status not in [FlowInfo.STATUS.RUNNING] or self.check_latest_context(
                    current_context, _n
                )
                if _n.is_new_version() or _n.is_modify_version() or strong_restart_confirm:
                    if (
                        _n.node_type in NodeTypes.SUPPORT_RESTART_SLIGHTLY_CATEGORY
                        and not strong_restart_confirm
                        and not _n.is_new_version()
                    ):
                        # 1.2 的例外条件，当前节点为支持轻量级重启的节点，且仅进行了修改，不需要重启
                        nodes_to_restart_slightly[_n.node_id] = _n
                        continue
                    # 否则当前节点一定需要重启
                    _nodes = [_n]
                    # 判断当前节点上游节点是否需要重启
                    # 1. 上游属于 DYNAMIC_ADD_CHANNEL_CATEGORY 且当前类型与上游节点类型不一致，才需要重启上游节点
                    # PS: 不可能出现 flink_streaming(启动，没有 channel) -> flink_streaming(增加)
                    for _f_n in _n.get_from_nodes_handler():
                        if (
                            _f_n.node_id not in nodes_to_restart
                            and _f_n.node_type not in NodeTypes.SUPPORT_RESTART_SLIGHTLY_CATEGORY
                            and _f_n.node_type in NodeTypes.DYNAMIC_ADD_CHANNEL_CATEGORY
                            and _f_n.node_type != _n.node_type
                        ):
                            _nodes.append(_f_n)
                    # _nodes 中的节点为所有需要重启的节点
                    # 若当前节点是新节点，并且不存在需要重启的上游节点(上游没有 processing 节点)
                    # 对于支持轻量级重启的节点类型，不需要重启下游的节点
                    if _n.is_new_version() and len(_nodes) == 1:
                        # 如果符合 1.1 并且当前节点上游不存在待重启的节点，检查一级下游节点是否有支持轻量级重启的节点，有则跳过
                        nodes_to_restart[_n.node_id] = _n
                        for _n_n in _n.get_to_nodes_handler():
                            if _n_n.node_type in NodeTypes.SUPPORT_RESTART_SLIGHTLY_CATEGORY:
                                nodes_to_restart_slightly[_n_n.node_id] = _n_n
                                continue
                            self.fetch_all_to_nodes(_n_n, nodes_to_restart)
                    else:
                        # 否则当前节点及其所有下游都需要重启
                        for _n_n in _nodes:
                            nodes_to_restart[_n_n.node_id] = _n_n
                            self.fetch_all_to_nodes(_n_n, nodes_to_restart)
        # 为避免出现 nodes_to_restart 和 nodes_to_restart_slightly 出现交叉，这里再做一次筛选，将原本可能支持轻量级重启的节点移除
        for node_id in nodes_to_restart:
            if node_id in nodes_to_restart_slightly:
                del nodes_to_restart_slightly[node_id]
        return nodes_to_restart, nodes_to_restart_slightly

    def create_data_makeup(
        self,
        processing_id,
        rerun_processings,
        source_schedule_time,
        target_schedule_time,
        with_storage,
    ):
        return BatchHelper.create_data_makeup(
            processing_id,
            rerun_processings,
            source_schedule_time,
            target_schedule_time,
            with_storage,
            self.geog_area_codes[0],
        )


class FlowVersionHandler(object):
    """
    DataFlow 版本控制对象
    """

    def __init__(self, flow_id, version):
        self.flow_id = flow_id
        self.version = version

        self._data = None

    def get_node(self, node_id):
        """
        通过节点ID，获取节点对象
        """
        for _n in self.nodes:
            if _n.node_id == node_id:
                return _n

        return None

    def get_from_nodes(self, node_id):
        """
        通过节点ID，获取父节点对象列表
        """
        _from_nodes = []
        for _l in self.links:
            if _l.to_node_id == node_id:
                _from_nodes.append(self.get_node(_l.from_node_id))
        return _from_nodes

    @property
    def nodes(self):
        return self.data["nodes"]

    @property
    def links(self):
        return self.data["links"]

    @property
    def data(self):
        if self._data is None:
            o_flow = FlowInfo.objects.get(flow_id=self.flow_id)
            objects = o_flow.get_components_by_version(self.version)

            _nodes = []
            _links = []
            for _object in objects:
                if isinstance(_object, FlowNodeInfo):
                    _nodes.append(NODE_FACTORY.get_node_handler_by_node(_object))
                if isinstance(_object, FlowLinkInfo):
                    _links.append(_object)

            self._data = {"nodes": _nodes, "links": _links}

        return self._data
