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

from auth.api import TofApi
from auth.config.ticket import TICKET_FLOW_CONFIG
from auth.constants import SYSTEM_USER, Sensitivity
from auth.core.permission import RolePermission, UserPermission
from auth.exceptions import (
    NoFunctionErr,
    NoProcessorErr,
    PermissionObjectDoseNotExistError,
    ScopeCheckErr,
)
from auth.handlers.object_classes import AuthResultTable, ObjectFactory
from auth.models import UserRole
from auth.models.base_models import ActionConfig
from auth.utils import generate_md5
from common.local import get_local_param, get_request_username
from django.utils.translation import ugettext_lazy as _


class DefaultProcessMixin:
    def __init__(self):
        pass

    @classmethod
    def get_process(cls, permission):
        """
        获取该权限项的审批流程

        @param {Dict[]} permission 包含action, scope, subject_id, subject_name, subject_class, object_class
        """
        action_id = permission.get("action")
        scope = permission.get("scope")

        if action_id in ["result_table.query_data", "result_table.query_queue"]:
            return cls.get_result_table_query_data_process(action_id, scope)
        else:
            return cls.get_default_process(action_id, scope)

    @classmethod
    def get_default_process(cls, action_id, scope):
        """
        通用功能授权流程

        目前仅支持在object_config中设定的object_class对应的scope_id_key作为资源标识申请，如result_table对应result_table_id
        @param action_id:
        @param scope:
        @return:
        """
        action = ActionConfig.objects.get(pk=action_id)
        authotization_action_id = f"{action.object_class_id}.manage_auth"

        object_class_cls = ObjectFactory.init_object_by_class(action.object_class_id)
        object_id = scope[object_class_cls.input_val_key]

        role_users = UserPermission.list_authorized_role_users(authotization_action_id, object_id, no_all_scope=True)
        role_ids = sorted(role_users.keys())
        user_ids = set()
        for ids in list(role_users.values()):
            user_ids.update(ids)

        user_ids = list(user_ids)
        if len(user_ids) == 0:
            raise NoProcessorErr()

        return {
            "flag": "{action_id}:{role_ids}".format(action_id=action_id, role_ids=":".join(role_ids)),
            "process_length": 1,
            "states": [{"process_step": 0, "processors": user_ids}],
        }

    @classmethod
    def get_result_table_query_data_process(cls, action_id, scope):
        """
        目前获取 result_table.query_data 审批流程功能，且流程是定制化
        """
        if "result_table_id" not in scope:
            raise ScopeCheckErr(_("目前仅支持 RT 级别的权限申请"))

        result_table_id = scope["result_table_id"]
        o_rt = AuthResultTable(object_id=result_table_id)

        # 检查 RT 是否存在
        if o_rt.obj is None:
            raise PermissionObjectDoseNotExistError()

        sensitivity_processor = {
            Sensitivity.PUBLIC: cls.get_public_process,
            Sensitivity.PRIVATE: cls.get_private_process,
            Sensitivity.CONFIDENTIAL: cls.get_confidential_process,
        }
        rt_sensitivity = Sensitivity(o_rt.obj__sensitivity)
        if rt_sensitivity not in sensitivity_processor:
            raise NoFunctionErr(_(f"暂不支持敏感级为 {rt_sensitivity} 的数据申请"))

        return sensitivity_processor[rt_sensitivity](action_id, o_rt)

    @classmethod
    def get_public_process(cls, action_id, result_table):
        """
        公共数据审批流程

        @param {string} action_id
        @param {auth.handler.object_classes.AuthResulTable} result_table
        """
        return {
            "flag": "{action_id}:public",
            "process_length": 1,
            "states": [{"process_step": 0, "process_name": _("直接通过"), "processors": [get_request_username()]}],
        }

    @classmethod
    def get_private_process(cls, action_id, result_table):
        """
        业务私有数据审批流程

        @param {string} action_id
        @param {auth.handler.object_classes.AuthResulTable} result_table
        """
        result_table_id = result_table.obj__result_table_id
        bk_biz_id = result_table.obj__bk_biz_id

        data_managers, data_managers_md5 = get_data_managers(result_table_id, bk_biz_id, "biz.manager")

        if len(data_managers) == 0:
            raise NoProcessorErr(_(f"结果表 {result_table_id} 不存在数据管理员（业务负责人）"))

        flag = "{action_id}:private:{bk_biz_id}:{md5}".format(
            action_id=action_id, bk_biz_id=bk_biz_id, md5=data_managers_md5
        )
        states = [{"process_step": 0, "process_name": _("数据管理员审批"), "processors": data_managers}]
        return {"flag": flag, "process_length": 1, "states": states}

    @classmethod
    def get_confidential_process(cls, action_id, result_table):
        """
        业务机密数据审批流程

        @param {string} action_id
        @param {auth.handler.object_classes.AuthResulTable} result_table
        """
        result_table_id = result_table.obj__result_table_id
        bk_biz_id = result_table.obj__bk_biz_id

        data_managers, data_managers_md5 = get_data_managers(result_table_id, bk_biz_id, "biz.leader")

        if len(data_managers) == 0:
            raise NoProcessorErr(_(f"结果表 {result_table_id} 不存在数据管理员（业务总监）"))

        flag = "{action_id}:confidential:{bk_biz_id}:{md5}".format(
            action_id=action_id, bk_biz_id=bk_biz_id, md5=data_managers_md5
        )

        states = []
        process_step = 0

        user_id = get_request_username()
        auth_info = get_local_param("auth_info")
        # STEP1 直属 Leader 审批，若申请人为数据管理员则无需 Leader 流程
        if user_id not in data_managers:
            states.append(
                {
                    "process_step": process_step,
                    "process_name": _("直属 Leader 审批"),
                    "processors": [get_direct_leader(user_id, auth_info)],
                }
            )
            process_step += 1

        # STEP2 数据管理员审批
        states.append({"process_step": process_step, "process_name": _("数据管理员审批"), "processors": data_managers})
        process_step += 1

        return {"flag": flag, "process_length": process_step, "states": states}


def get_data_managers(result_table_id, bk_biz_id, biz_role_id):
    """
    获取结果表的数据管理员
    """
    # 暂时按照业务负责人和数据管理员合并为审批人员进行处理，后续需要区分
    managers = list(
        UserRole.objects.filter(scope_id=result_table_id, role_id="result_table.manager").values_list(
            "user_id", flat=True
        )
    )
    biz_managers = list(
        UserRole.objects.filter(scope_id=bk_biz_id, role_id=biz_role_id).values_list("user_id", flat=True)
    )
    data_managers = sorted(list(set(biz_managers + managers)))
    return data_managers, generate_md5(str(data_managers))


def get_direct_leader(user_id, auth_info):
    """
    获取用户直属 leader
    """
    # 员工信息中的 OfficialId 表示领导级别
    # 7、8      普通员工
    # 6         TeamLeader（副组长、组长、总监、副总监）
    # 5、4、3    助理总经理/总经理
    # 2         主管副总（副总裁）
    # 1         总裁
    params = {"login_name": user_id}
    params.update(auth_info)

    staff_info = TofApi.get_staff_info(params, raise_exception=True).data
    if int(staff_info["OfficialId"]) > 6:
        return TofApi.get_staff_direct_leader(params, raise_exception=True).data["LoginName"]

    return user_id


class RoleProcessMixin:
    def __init__(self):
        pass

    @classmethod
    def get_process(cls, permission):
        """
        获取角色的相关审批项，暂时需要保证角色审批仅包含一个步骤

        @param permission: action, user_id, role_id, scope_id
        """
        action_id = permission.get("action")
        scope_id = permission.get("scope_id")
        role_id = permission.get("role_id")
        return cls._get_default_process(action_id, scope_id, role_id)

    @classmethod
    def get_core_processers(cls, role_id, scope_id):
        """
        获取角色审批核心处理人
        """
        return RolePermission.get_authorizers_users(role_id, scope_id)

    @classmethod
    def _get_default_process(cls, action_id, scope_id, role_id):
        if scope_id is None or role_id is None:
            raise ScopeCheckErr()

        user_ids = cls.get_core_processers(role_id, scope_id)

        if len(user_ids) == 0:
            raise NoProcessorErr()

        flag = f"{action_id}:{role_id}:{scope_id}"
        return {"flag": flag, "process_length": 1, "states": [{"process_step": 0, "processors": user_ids}]}


class CommonTicketProcessMixin:
    def __init__(self):
        pass

    @classmethod
    def get_process(cls, ticket_type, ticket_step_params=None):
        """
        获取该权限项的审批流程
        @return:
        """
        ticket_step_params = ticket_step_params if ticket_step_params else dict()
        flow = TICKET_FLOW_CONFIG[ticket_type]

        flags = []
        states = []
        for index, step in enumerate(flow.steps):
            role_id = step.process_role_id
            scope_id = step.process_scope_id
            paased_by_system = step.paased_by_system

            _key = f"{index}&process_role_id"
            if _key in ticket_step_params:
                role_id = ticket_step_params[_key]

            _key = f"{index}&process_scope_id"
            if _key in ticket_step_params:
                scope_id = ticket_step_params[_key]

            _key = f"{index}&paased_by_system"
            if _key in ticket_step_params:
                paased_by_system = ticket_step_params[_key]

            user_ids = list(
                UserRole.objects.filter(role_id=role_id, scope_id=scope_id).values_list("user_id", flat=True)
            )
            if paased_by_system:
                user_ids.append(SYSTEM_USER)

            _flag = f"{role_id}:{scope_id}"
            if len(user_ids) == 0:
                raise NoProcessorErr(_(f"审批人为空，源于角色（{_flag}）不存在成员"))

            flags.append(_flag)
            states.append({"process_step": index, "processors": user_ids})

        return {"flag": "&".join(flags), "process_length": len(states), "states": states}
