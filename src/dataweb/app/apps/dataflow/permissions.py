# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.core.cache import cache
from django.http import Http404
from django.utils.translation import ugettext as _
from rest_framework.permissions import BasePermission

from apps.api import AuthApi
from apps.dataflow.handlers.dataid import DataId
from apps.dataflow.handlers.flow import Flow

# from apps.dataflow.models import RoleUserAccess


class Permission(object):
    @staticmethod
    def has_project_perm(username, project_id, action="project.retrieve"):
        """
        对象级的权限控制，用户是否具有项目的权限
        """
        return AuthApi.check_user_perm({"user_id": username, "action_id": action, "object_id": project_id})

    @staticmethod
    def batch_judge_flow_project_perm(username, project_ids, action="project.retrieve"):
        """
        批量判断用户是否具有flow对应项目的权限
        """
        permissions_list = [
            {"user_id": username, "action_id": action, "object_id": project_id} for project_id in project_ids
        ]
        has_permission_list = AuthApi.batch_check_user_perm(
            {
                "permissions": permissions_list,
            }
        )
        return has_permission_list

    @staticmethod
    def has_biz_perm(username, biz_id, action="biz.manage"):
        """
        对象级的权限控制，用户是否具有业务的权限
        """
        return AuthApi.check_user_perm({"user_id": username, "action_id": action, "object_id": biz_id})

    @staticmethod
    def has_biz_list_perm(username, biz_ids, action="biz.manage"):
        """
        用户是否有业务列表的权限（批量）
        """
        biz_ids = [int(biz_id) for biz_id in biz_ids]
        return all([Permission.has_biz_perm(username, biz_id, action) for biz_id in biz_ids])

    @staticmethod
    def has_dataid_perm(username, data_id, action="raw_data.retrieve"):
        """
        对象级的权限控制，用户是否具有 dataid 的权限
        """
        return AuthApi.check_user_perm({"user_id": username, "action_id": action, "object_id": data_id})

    @staticmethod
    def has_flow_perm(username, flow_id, action="flow.retrieve"):
        """
        对象级的权限控制，用户是否具有 flow 的权限
        """
        return AuthApi.check_user_perm({"user_id": username, "action_id": action, "object_id": flow_id})

    @staticmethod
    def has_result_table_perm(username, result_table_id, action="result_table.retrieve"):
        """
        对象级的权限控制，用户是否具有RT的权限
        @param action {String} 操作方式，目前仅支持 query_info、query_data、others
        @todo 需要关联 project_biz_data 确定是否有二级授权
        """
        return AuthApi.check_user_perm({"user_id": username, "action_id": action, "object_id": result_table_id})


def perm_cache(func):
    """
    根据用户+请求参数，把权限验证结果结果进行缓存
    """

    def _deco(self, request, view):
        # 只对查询（GET方法）进行权限缓存
        if request.method != "GET":
            return func(self, request, view)

        user = request.user.username
        kwargs = "_".join("{}:{}".format(_k, _w) for _k, _w in list(view.kwargs.items()))
        cache_name = "{}__{}__{}".format(user, view.action, kwargs)
        perm = cache.get(cache_name)
        if perm is None:
            perm = func(self, request, view)
            cache.set(cache_name, perm, 60)

        return perm

    return _deco


class ProjectPermissions(BasePermission):
    """
    通过 DRF 在对象层面进行权限控制
    """

    message = _("您没有项目权限")
    DELETE_CHECK_EXEMPT_ACTION = ["restore"]

    M_VIEW_ACTION = {
        "update_project_user": "project.manage",
        "destroy": "project.delete",
        "restore": "project.manage",
        "partial_update": "project.update",
    }

    @perm_cache
    def has_permission(self, request, view):
        project_id = view.kwargs.get("project_id", None)

        if project_id is None:
            return True

        username = request.user.username
        action = self.M_VIEW_ACTION.get(view.action, "project.retrieve")

        return Permission.has_project_perm(username, project_id, action=action)


class DataIdPermissions(BasePermission):
    """
    通过 DRF 在对象层面进行权限控制
    """

    M_VIEW_ACTION = {
        "retrieve": "raw_data.retrieve",
        "list_data_flow": "raw_data.retrieve",
        "get_data_id": "raw_data.retrieve",
        "get_latest_msg": "raw_data.query_data",
        "list_latest_msg": "raw_data.query_data",
        "list_data_count_by_time": "raw_data.query_data",
    }

    message = _("您没有 DataId 权限")

    @perm_cache
    def has_permission(self, request, view):
        data_id = view.kwargs.get("raw_data_id", None)
        if data_id is None:
            return True

        if not DataId(data_id=data_id).is_exist():
            raise Http404(_("DataID不存在"))

        username = request.user.username
        action = self.M_VIEW_ACTION[view.action] if view.action in self.M_VIEW_ACTION else "raw_data.retrieve"
        return Permission.has_dataid_perm(username, data_id, action=action)


class FlowPermissions(BasePermission):
    """
    Flow权限
    """

    @perm_cache
    def has_permission(self, request, view):
        username = request.user.username
        # 创建权限特殊处理
        if view.action == "create":
            project_id = request.data.get("project_id")

            return Permission.has_project_perm(username, project_id)

        # 批量权限特殊处理
        multi_action = ["multi_destroy", "multi_start", "multi_stop"]
        if view.action in multi_action:
            flow_ids = request.data.get("flow_ids")
            # @todo 有效率问题，待优化成批量鉴权
            return all([Permission.has_flow_perm(username, flow_id) for flow_id in flow_ids])

        flow_id = view.kwargs.get("flow_id", None)
        # 非特殊处理的list_route接口
        if flow_id is None:
            return True

        if not Flow(flow_id=flow_id).is_exist():
            raise Http404(_("任务不存在"))

        return Permission.has_flow_perm(username, flow_id)


class ResultTablePermissions(BasePermission):
    """
    对结果表操作，统一进行权限控制
    """

    M_VIEW_ACTION = {
        "retrieve": "result_table.retrieve",
        "list_query_history": "result_table.retrieve",
        "get_storage_info": "result_table.retrieve",
        "get_rt_schema": "result_table.retrieve",
        "list_field": "result_table.retrieve",
        "set_selected_history": "result_table.retrieve",
        "delete_selected_history": "result_table.retrieve",
        "list_data_count_by_time_rt": "result_table.query_data",
        "list_latest_msg": "result_table.query_data",
        # 由后台API保证权限，此处豁免校验
        "query_es": None,
        "get_es_chart": None,
        "query_rt": None,
        "get_latest_msg": "result_table.query_data",
        "list_kafka_latest_msg": "result_table.query_data",
    }

    @perm_cache
    def has_permission(self, request, view):
        username = request.user.username
        result_table_id = view.kwargs.get("result_table_id", None)

        if result_table_id is None:
            return True

        action = self.M_VIEW_ACTION[view.action] if view.action in self.M_VIEW_ACTION else "result_table.retrieve"

        # action是None时豁免校验
        if action is None:
            return True
        return Permission.has_result_table_perm(username, result_table_id, action=action)


class BizPermissions(BasePermission):
    """
    通过 DRF 在对象层面进行权限控制
    """

    message = _("您没有业务权限")

    def has_permission(self, request, view):
        biz_id = view.kwargs.get("biz_id", None)
        if biz_id is None:
            return True

        username = request.user.username

        return Permission.has_biz_perm(username, biz_id)
