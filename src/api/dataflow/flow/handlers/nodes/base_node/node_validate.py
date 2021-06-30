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

from common.auth import PermissionDeniedError
from common.exceptions import ApiResultError
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.settings import FLOW_MAX_NODE_COUNT
from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.log import flow_logger as logger


class NodeValidate(object):
    """
    节点校验相关
    """

    @staticmethod
    def node_count_validate(flow_id):
        o_flow = FlowHandler(flow_id)
        if FLOW_MAX_NODE_COUNT is not None and len(o_flow.nodes) + 1 > FLOW_MAX_NODE_COUNT:
            raise FlowError(_("当前任务节点数量超出限制，最大允许数量为%s") % FLOW_MAX_NODE_COUNT)

    @staticmethod
    def tdw_auth_validate(flow_id, node_type):
        o_flow = FlowHandler(flow_id)
        if node_type in NodeTypes.TDW_CATEGORY and not o_flow.flow.get_tdw_conf():
            raise FlowError(_("当前任务未启用TDW支持，请启用"))

    @staticmethod
    def from_links_validate(flow_id, from_links):
        for link in from_links:
            _node_id = int(link["source"]["node_id"])
            if NODE_FACTORY.get_node_handler(_node_id).flow_id != int(flow_id):
                raise Errors.NodeValidError(_("上游节点与当前节点不在同一画布，不可连接"))
            link["source"]["node_id"] = _node_id

    @staticmethod
    def form_param_validate(params, form_class, is_create):
        """
        验证参数
        @param {dict} params  参数
        @param {object} form_class 校验参数的 Form 表单
        """
        form = form_class(params, is_create)
        if not form.is_valid():
            err_msg = _("参数不对")
            if hasattr(form, "format_errmsg"):
                err_msg = form.format_errmsg()
            raise Errors.NodeValidError(err_msg)
        return form.cleaned_data

    @staticmethod
    def node_auth_check(node_handler, flow_id, form_data, is_create=True):
        # tdw 权限检查
        if not is_create and node_handler.node_type in NodeTypes.TDW_CATEGORY:
            # 用户没有应用组权限不可修改节点
            tdw_conf = node_handler.flow.get_tdw_conf()
            # 没权限的提示：没有TDW应用组的权限
            AuthHelper.check_tdw_app_group_perm(tdw_conf["tdwAppGroup"])

        if node_handler.node_type in NodeTypes.SOURCE_CATEGORY:
            return FlowHandler(flow_id).perm_check_data(form_data["result_table_id"])
        elif node_handler.node_type in NodeTypes.STORAGE_CATEGORY:
            # 校验存储节点所选集群集群权限
            cluster_name = form_data["cluster"]
            # 如果是更新操作，若未修改集群，无需鉴权
            if not is_create:
                origin_cluster_name = ResultTable(form_data["result_table_id"]).cluster_name(node_handler.storage_type)
                if cluster_name == origin_cluster_name:
                    return True
            cluster_groups = NodeUtils.get_storage_cluster_groups(node_handler.storage_type, cluster_name)
            if not cluster_groups:
                raise ApiResultError(
                    _("集群%(cluster_name)s，存储类型%(storage_type)s不属于任一集群组.")
                    % {
                        "cluster_name": cluster_name,
                        "storage_type": node_handler.storage_type,
                    }
                )
            for cluster_group in cluster_groups:
                # 集群可能属于多个集群组
                # 核对结果：一个项目能否使用一个集群，只需要项目对集群所属任意一个集群组有权限即可
                try:
                    return FlowHandler(flow_id).perm_check_cluster_group(cluster_group)
                except PermissionDeniedError as e:
                    logger.warning(e)
                    pass
            raise Errors.NodeValidError(_("存储节点集群权限不足"))
