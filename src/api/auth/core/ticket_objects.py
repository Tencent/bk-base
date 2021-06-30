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
import datetime
import json
from abc import abstractmethod

import requests
from auth.api import DataflowApi
from auth.constants import (
    FAILED,
    PROCESSING,
    PROJECT,
    STOPPED,
    SUCCEEDED,
    SubjectTypeChoices,
    TokenPermissionStatus,
)
from auth.core.attrs_defination import NoticeContent
from auth.core.process import (
    CommonTicketProcessMixin,
    DefaultProcessMixin,
    RoleProcessMixin,
)
from auth.core.ticket_serializer import (
    BaseTicketSerializer,
    CommonTicketSerializer,
    DataPermissionItemSerializer,
    DataTicketSerializer,
    RolePermissionItemSerializer,
    RoleTicketSerializer,
    TokenPermissionItemSerializer,
    TokenTicketSerializer,
)
from auth.exceptions import (
    AuthAPIError,
    HasAlreadyExistsErr,
    RepeatPermissionErr,
    TicketCallbackErr,
    TokenNotExistErr,
    UpdateRoleErr,
)
from auth.handlers.object_classes import oFactory
from auth.handlers.project import ProjectHandler
from auth.handlers.raw_data import RawDataHandler
from auth.handlers.resource_group import ResourceGroupHandler, ResourceGroupSubject
from auth.handlers.role import RoleHandler
from auth.models.auth_models import AuthDataToken
from auth.models.ticket_models import (
    ApprovalController,
    DataTicketPermission,
    RoleTicketPermission,
    Ticket,
    TicketState,
    TokenTicketPermission,
)
from auth.tasks.sync_queue_auth import add_queue_auth
from auth.utils.base import parse_in_dict
from common.log import logger
from django.db import transaction
from django.db.models import Q
from django.utils.translation import ugettext as _
from six.moves import range

from common import local

ApprovalController = ApprovalController


class BaseTicket:
    BASE_MODEL = Ticket
    BASE_SERIALIZER = BaseTicketSerializer

    MODEL = None
    SERIALIZER = None
    CHILD_SERIALIZER = None

    def __init__(self, ticket_type):
        self.ticket_type = ticket_type

    def check_base_params(self, base_params):
        """
        获取校验后的基础信息
        @param base_params:
        @return:
        """
        base_params["created_at"] = datetime.datetime.now()
        base_params["status"] = PROCESSING
        base_params["ticket_type"] = self.ticket_type
        base_params["process_step"] = 0
        serializer = self.BASE_SERIALIZER(data=base_params)
        serializer.is_valid(raise_exception=True)
        base_params = serializer.validated_data
        return base_params

    def check_permissions(self, permissions):
        """
        获取校验后的权限信息
        @param permissions:
        @return:
        """
        child_serializer = self.CHILD_SERIALIZER(data=permissions, many=True)
        child_serializer.is_valid(raise_exception=True)
        return child_serializer.validated_data

    def check_ticket_data(self, data):
        """
        检出清洗的数据
        @param data: {"permissions": [], "ticket_type": xx, "created_by": yy, "reason": zz}
        @return:
        """
        base_params = parse_in_dict(data, ["ticket_type", "created_by", "reason"])

        # Compatible for no extra data-ticket
        if "extra" in data:
            base_params["extra"] = data["extra"]

            # 单据 ticket.process_id 是后续变更加上的，目前默认 extra 中 存有 process_id 字段
            base_params["process_id"] = data["extra"]["process_id"]

        base_params = self.check_base_params(base_params)
        permissions = self.generate_permissions(data.pop("permissions", []))
        if len(permissions):
            permissions = self.check_permissions(permissions)
        self.check_repeat(base_params, permissions)
        return base_params, permissions

    def check_repeat(self, base_params, permissions):
        """
        检查
        @param base_params:
        @param permissions:
        @return:
        """
        for i1 in range(len(permissions)):
            for i2 in range(i1 + 1, len(permissions)):
                len_permission_item = 0
                for key, value in list(permissions[i1].items()):
                    if value != permissions[i2].get(key):
                        break
                    len_permission_item += 1
                if len_permission_item == len(list(permissions[i1].items())):
                    raise RepeatPermissionErr()

        exist_tickets = Ticket.objects.filter(
            Q(status=PROCESSING), **parse_in_dict(base_params, ["ticket_type", "created_by", "reason", "extra"])
        ).all()
        for ticket in exist_tickets:
            for item in permissions:
                if self.MODEL.objects.filter(ticket_id=ticket.pk, **item).count() > 0:
                    raise HasAlreadyExistsErr()
        return True

    def generate_permissions(self, permissions):
        """
        构造permission数据结构，补充user_id
        @param permissions:
        @return:
        """
        for perm in permissions:
            if "user_id" not in perm:
                perm["user_id"] = local.get_request_username()
        return permissions

    def create_ticket(self, ticket_list, base_params, auto_approve):
        """
        创建订单
        @param ticket_list:
        @param base_params:
        @param auto_approve:Determine whether to execute 'auto_approve' when creating ticket
        @return:
        [{
            'status': 'processing',
            'process_step': 0,
            'processors': ['processor666'],
            'extra': '{}',
            'ticket_type': 'project_biz',
            'created_at': '2019-06-21 10:13:26',
            'ticket_type_display': '项目申请结果数据',
            'created_by': 'admin666',
            'reason': 'hello world',
            'end_time': None,
            'process_length': 1,
            'state_id': 271,
            'status_display': '处理中',
            'id': 330,
            'permissions': [{
                'id': 103,
                'ticket': 'Ticket object',
                'key': 'result_table_id',
                'value': '591_test_rt',
                'scope': {
                    'object_name': '591_test_rt',
                    'scope_id': '591_test_rt',
                    'description': '591_test_rt_alias'
                },
                'subject_id': '1',
                'subject_name': '数据平台对内版',
                'subject_class': 'project',
                'action': 'result_table.query_data',
                'object_class': 'result_table',
                'subject_class_name': '项目',
                'object_class_name': '结果数据',
                'action_name': '数据查询',
                'scope_object': {
                    'scope_object_class_name': '结果数据',
                    'scope_name': '591_test_rt'
                }
            }]
        }]
        """
        tickets = []
        with transaction.atomic(using="basic"):
            for ticket_data in ticket_list:
                base_params["process_length"] = ticket_data.get("process_length")
                ticket = self.BASE_MODEL.objects.create(**base_params)
                for item in ticket_data.get("permissions", []):
                    item["ticket"] = ticket
                    self.MODEL.objects.create(**item)
                for state in ticket_data.get("states", []):
                    create_params = {
                        "ticket": ticket,
                        "processors": state.get("processors"),
                        "process_step": state.get("process_step"),
                    }
                    TicketState.objects.create(**create_params)

                ticket.start_process(auto_approve)
                ticket.refresh_from_db()
                tickets.append({"ticket": self.SERIALIZER(ticket).data})
            tickets = self.SERIALIZER.wrap_permissions_display(tickets)
        return [_ticket.get("ticket") for _ticket in tickets]

    def generate_tickets(self, permissions, base_params):
        """
        拆单，根据flag判断是否需要进行拆单
        @param permissions: 根据auth.models.ticket_models中定义的xxxTicketPermission模型传递对应的参数
        @return: 传出根据auth.core.ticket_serializer定义的xxxTicketSerializer相关模型对应的列表
        [{
            'process_length': 1,
            'states': [{
                'process_step': 0,
                'processors': ['processor666']
            }],
            'flag': 'result_table.query_data:bk_biz_id:591',
            'permissions': [{
                'scope'： {
                    'result_table_id': u '591_test_rt'
                },
                'subject_id': '1',
                'subject_name': '数据平台对内版',
                'subject_class': 'project',
                'action': 'result_table.query_data',
                'object_class': 'result_table'
            }]
        }]
        """
        ticket_list = []
        for item in permissions:
            process = self.get_process(item)
            flag = process.get("flag")
            has_permission = False
            for ticket in ticket_list:
                if ticket.get("flag") == flag:
                    ticket.get("permissions").append(item)
                    has_permission = True
                    break
            if not has_permission:
                process["permissions"] = [item]
                ticket_list.append(process)
        return ticket_list

    def generate_ticket_data(self, data):
        """
        生成ticket数据
        @param data:
        @return:
        """
        base_params, permissions = self.check_ticket_data(data)
        ticket_list = self.generate_tickets(permissions, base_params)

        auto_approve = True
        if data.get("auto_approve") is not None:
            auto_approve = data.get("auto_approve")

        tickets = self.create_ticket(ticket_list, base_params, auto_approve)
        return tickets

    @abstractmethod
    def get_content_for_notice(self, ticket):
        """
        获取消息发送内容（申请项目、角色、数据..）
        @param [Ticket] ticket:
        @return:
        """
        pass

    @abstractmethod
    def add_permission(self, ticket):
        """
        添加权限的函数，继承的子类必须要实现
        @param ticket:
        @return:
        """
        pass


class DataTicketObj(BaseTicket, DefaultProcessMixin):
    MODEL = DataTicketPermission
    SERIALIZER = DataTicketSerializer
    CHILD_SERIALIZER = DataPermissionItemSerializer

    def get_content_for_notice(self, ticket):
        """
        获取消息发送内容（申请项目、角色、数据..）
        @param [Ticket] ticket:
        @return:
        """
        tickets = []
        tickets.append({"ticket": self.SERIALIZER(ticket).data})
        wrap_ticket = self.SERIALIZER.wrap_permissions_display(tickets)
        ticket_display = wrap_ticket[0]["ticket"]
        gainer = "[{}] {}".format(
            ticket_display["permissions"][0]["subject_class_name"], ticket_display["permissions"][0]["subject_name"]
        )
        scopes = [
            "[{}] {}".format(item["scope_object"]["scope_object_class_name"], item["scope_object"]["scope_name"])
            for item in ticket_display["permissions"]
        ]
        content = NoticeContent(
            created_by=ticket_display["created_by"],
            ticket_type=ticket_display["ticket_type_display"],
            gainer=gainer,
            scope=scopes,
            reason=ticket_display["reason"],
        )
        return content


class ResourceGroupTicketObj(DataTicketObj):
    def add_permission(self, ticket):
        """
        添加资源组权限
        """
        group_handler = ResourceGroupHandler()

        permissions = self.CHILD_SERIALIZER(self.MODEL.objects.filter(ticket=ticket).all(), many=True).data
        for item in permissions:
            action = item.get("action")
            subject_class = item["subject_class"]
            subject_id = item["subject_id"]
            if action in ["resource_group.use"] and subject_class in [
                SubjectTypeChoices.PROJECT,
                SubjectTypeChoices.RAWDATA,
            ]:

                scope = oFactory.BKDATA_OBJECTS.get(item["object_class"]).clean_scopes(item["scope"])
                group_handler.add_authorization(
                    subject_class, subject_id, scope["resource_group_id"], ticket.created_by
                )
            else:
                # 暂不处理
                pass

    @classmethod
    def list_authoriazed_tickets(cls, subject_class, subject_id):
        """
        列举已授权和正在授权的单据列表
        """
        permissioms = DataTicketPermission.objects.select_related("ticket").filter(
            subject_class=subject_class, subject_id=subject_id, action="resource_group.use"
        )

        result_dict = dict()

        # note：可能存在针对统一资源组申请多次的情况，采取默认排序中，后置替换前置的方式来保证
        for perm in permissioms:
            if perm.key != "resource_group_id":
                continue

            resource_group_id = perm.value

            # 循环中有数据库查询，这里需要关注下性能，要评估是否存在一个对象有很多申请单据
            _ticket = perm.ticket
            _current_state_id = _ticket.current_state().id

            if (
                resource_group_id not in result_dict
                or result_dict[resource_group_id]["created_at"] < _ticket.created_at
            ):
                result_dict[resource_group_id] = {
                    "ticket_id": _ticket.id,
                    "state_id": _current_state_id,
                    "created_at": _ticket.created_at,
                    "status": _ticket.status,
                }

        # 已经有权限的资源组列表，没有单据也应该有记录
        rg_handler = ResourceGroupHandler()
        authorized_ids = rg_handler.list_authorized_resource_group_ids(
            [ResourceGroupSubject(subject_class, subject_id)]
        )

        for _id in authorized_ids:
            if _id not in result_dict or result_dict[_id]["status"] != SUCCEEDED:
                result_dict[_id] = {"ticket_id": None, "state_id": None, "created_at": None, "status": SUCCEEDED}

        return result_dict


class ProjectDataTicketObj(DataTicketObj):
    def add_permission(self, ticket):
        """
        添加申请数据的相关权限
        """
        permissions = self.CHILD_SERIALIZER(self.MODEL.objects.filter(ticket=ticket).all(), many=True).data
        for item in permissions:
            action = item.get("action")
            subject_class = item["subject_class"]
            subject_id = item["subject_id"]

            if action in ["result_table.query_data", "raw_data.query_data"] and subject_class == "project":
                scope = oFactory.BKDATA_OBJECTS.get(item["object_class"]).clean_scopes(item["scope"])
                ProjectHandler(subject_id).add_data(scope, action_id=action)
            else:
                # 暂不处理
                pass

    @classmethod
    def project_tickets(cls, project_id=None, action_id="result_table.query_data"):
        """
        项目申请的结果数据
        @param project_id: 项目di
        @return:
        {
            "591_test_rt": {
              "status": "processing",
              "created_at": "2019-04-03 16:22:27",
              "ticket_id": 44
            },
            "593_test_rt": {
              "status": "failed",
              "created_at": "2019-04-03 16:42:31",
              "reason": "驳回理由",
              "ticket_id": 46
            },
            "592_test_rt": {
              "status": "succeeded",
              "created_at": "2019-04-03 16:22:50",
              "ticket_id": 45
            }
        }
        """
        key = oFactory.init_object(action_id).input_val_key

        result_dict = {}
        tickets = cls.SERIALIZER(
            Ticket.objects.filter(
                dataticketpermission__subject_class=PROJECT,
                dataticketpermission__subject_id=project_id,
                dataticketpermission__key=key,
            ).distinct(),
            many=True,
        ).data
        for ticket in tickets:
            for permission in ticket.get("permissions"):
                scope = permission.get("scope")
                if scope is None:
                    continue

                scope_id = scope.get(key)
                if not scope_id:
                    continue

                if result_dict.get(scope_id) is None:
                    result_dict[scope_id] = {
                        "object_id": scope_id,
                        "ticket_id": ticket.get("id"),
                        "state_id": ticket.get("state_id"),
                        "created_at": ticket.get("created_at"),
                        "status": ticket.get("status"),
                        "bk_biz_id": None,
                    }
                else:
                    if result_dict.get(scope_id).get("created_at") < ticket.get("created_at"):
                        result_dict[scope_id] = {
                            "object_id": scope_id,
                            "ticket_id": ticket.get("id"),
                            "created_at": ticket.get("created_at"),
                            "status": ticket.get("status"),
                            "bk_biz_id": None,
                        }
        return result_dict

    @classmethod
    def distinct_data(cls, project_id=None, bk_biz_id=None, action_id="result_table.query_data"):
        """
        从单据提取所有项目申请过的数据，同时还要补齐未通过单据授权的数据
        """
        object_class_cls = oFactory.init_object(action_id)
        key_name = object_class_cls.input_val_key
        object_class = object_class_cls.auth_object_class

        # 存在申请单据的数据
        project_tickets = cls.project_tickets(project_id=project_id, action_id=action_id)

        # 不存在申请单据但项目有权限的数据
        data = ProjectHandler(project_id=project_id).get_data(action_id=action_id)
        for _data in data:
            # 这里认定所有通过单据整理出来的 object_id 都为字符类型
            key = str(_data.get(key_name))
            if key:
                if key not in project_tickets:
                    project_tickets[key] = {
                        "object_id": key,
                        "created_at": None,
                        "state_id": None,
                        "ticket_id": None,
                        "status": SUCCEEDED,
                        "bk_biz_id": _data["bk_biz_id"],
                    }
                else:
                    project_tickets[key]["bk_biz_id"] = _data["bk_biz_id"]

        # 需要支持业务过滤，大部分数据通过已授权数据，可以补全业务字段，部分单据且未授权记录需要做些特殊处理，补全业务字段
        none_bk_biz_tickets = [v for v in list(project_tickets.values()) if v["bk_biz_id"] is None]

        if len(none_bk_biz_tickets) > 0:
            if object_class == "result_table":
                for content in none_bk_biz_tickets:
                    _object_id = content["object_id"]
                    if "_" in _object_id:
                        content["bk_biz_id"] = int(_object_id.split("_")[0])
            else:
                RawDataHandler.wrap_biz_id(none_bk_biz_tickets, "object_id", is_str=True)

        if bk_biz_id is not None:
            return {k: v for k, v in list(project_tickets.items()) if v["bk_biz_id"] == bk_biz_id}

        return project_tickets


class RoleTicketObj(BaseTicket, RoleProcessMixin):
    MODEL = RoleTicketPermission
    SERIALIZER = RoleTicketSerializer
    CHILD_SERIALIZER = RolePermissionItemSerializer

    def add_permission(self, ticket):
        """
        添加角色相关的权限
        @param ticket:
        @return:
        """
        permissions = self.CHILD_SERIALIZER(self.MODEL.objects.filter(ticket=ticket).all(), many=True).data

        role_permissions = []
        for item in permissions:

            if item["action"] not in ["add_role", "delete_role"]:
                logger.error("Invalid action in RolePermission authorization: {}".format(item["action"]))
                continue

            perm = {
                "operate": RoleHandler.GRANT if item["action"] == "add_role" else RoleHandler.REVOKE,
                "scope_id": item["scope_id"],
                "role_id": item["role_id"],
                "user_id": item["user_id"],
            }
            role_permissions.append(perm)

        results = RoleHandler.update_role_batch(ticket.created_by, role_permissions)
        fail_results = [r for r in results if not r[1]]
        fail_userids = [r[0]["user_id"] for r in fail_results]

        if len(fail_results) > 0:
            logger.error(f"[RoleService AddRole] Fail to update role memebers, errors={fail_results}")
            raise UpdateRoleErr(_("更新部分角色成员失败（{}），请联系管理员").format(", ".join(fail_userids)))

        return []

    def get_content_for_notice(self, ticket):
        """
        获取消息发送内容（申请项目、角色、数据..）
        @param [Ticket] ticket:
        @return:
        """
        tickets = []
        tickets.append({"ticket": self.SERIALIZER(ticket).data})
        wrap_ticket = self.SERIALIZER.wrap_permissions_display(tickets)
        ticket_display = wrap_ticket[0]["ticket"]
        apply_content = "[{}] {}".format(
            ticket_display["permissions"][0]["object_class_name"], ticket_display["permissions"][0]["action_name"]
        )
        scopes = [
            "[{}] {}".format(item["scope_object"]["scope_object_class_name"], item["scope_object"]["scope_name"])
            for item in ticket_display["permissions"]
        ]
        content = NoticeContent(
            created_by=ticket_display["created_by"],
            ticket_type=ticket_display["ticket_type_display"],
            gainer=ticket_display["created_by"],
            apply_content=apply_content,
            scope=scopes,
            reason=ticket_display["reason"],
        )
        return content


class DataTokenTicketObj(BaseTicket, DefaultProcessMixin):
    MODEL = TokenTicketPermission
    SERIALIZER = TokenTicketSerializer
    CHILD_SERIALIZER = TokenPermissionItemSerializer

    def update_permission(self, ticket, status):
        """
        更新权限状态
        @param ticket:
        @param status:
        @return:
        """
        permissions = self.CHILD_SERIALIZER(self.MODEL.objects.filter(ticket=ticket).all(), many=True).data

        # 针对队列服务进行额外授权的列表
        data_token_result_table_list = []

        for item in permissions:
            # 针对队列服务进行额外授权
            if item["action"] in ["result_table.query_queue"] and status == TokenPermissionStatus.ACTIVE:
                data_token_id = item["subject_id"]
                scope = item["scope"]
                if "result_table_id" in scope:
                    data_token_result_table_list.append([data_token_id, scope["result_table_id"]])

            data_scope = {
                "all": False,
                "permissions": [
                    {"action_id": item["action"], "object_class": item["object_class"], "scope": item["scope"]}
                ],
            }
            try:
                o_token = AuthDataToken.objects.get(pk=item["subject_id"])
            except AuthDataToken.DoesNotExist:
                raise TokenNotExistErr()
            o_token.update_permission_status(data_scope, status=status)

        if len(data_token_result_table_list) > 0:
            add_queue_auth.delay(data_token_result_table_list)

    def add_permission(self, ticket):
        """
        完成审批后，向AuthDataTokenPermission更新token权限
        @param ticket:
        @return:
        """
        self.update_permission(ticket, TokenPermissionStatus.ACTIVE)

    def after_terminate(self, ticket, status):
        """
        单据终止后，把申请中的权限置为inactive
        @param ticket:
        @param status:
        @return:
        """
        self.update_permission(ticket, TokenPermissionStatus.INACTIVE)

    def get_content_for_notice(self, ticket):
        """
        获取消息发送内容（申请项目、角色、数据..）
        @param [Ticket] ticket:
        @return:
        """
        tickets = []
        tickets.append({"ticket": self.SERIALIZER(ticket).data})
        wrap_ticket = self.SERIALIZER.wrap_permissions_display(tickets)
        ticket_display = wrap_ticket[0]["ticket"]
        gainer = "[{}] {}".format(
            ticket_display["permissions"][0]["subject_class_name"], ticket_display["permissions"][0]["subject_name"]
        )
        apply_content = "[{}] {}".format(
            ticket_display["permissions"][0]["object_class_name"], ticket_display["permissions"][0]["action_name"]
        )
        scopes = [
            "[{}] {}".format(item["scope_object"]["scope_object_class_name"], item["scope_object"]["scope_name"])
            for item in ticket_display["permissions"]
        ]
        content = NoticeContent(
            created_by=ticket_display["created_by"],
            ticket_type=ticket_display["ticket_type_display"],
            gainer=gainer,
            apply_content=apply_content,
            scope=scopes,
            reason=ticket_display["reason"],
        )
        return content


class CommonTicketObj(BaseTicket, CommonTicketProcessMixin):
    """
    通用单据的区别都在于 ticket.extra 的区别

    extra 通用结构
        {
            process_id: "111_xx",
            content: "对《项目1》->《任务1》的离线结果表 1_xx, 2_xx 进行重算",
            callback_url: "http://www.aa.com/ss/callback/"
        }
    """

    MODEL = None
    SERIALIZER = CommonTicketSerializer
    CHILD_SERIALIZER = None

    def check_repeat(self, base_params, permissions):
        """
        检查
        @param base_params:
        @param permissions: ==[]
        @return:
        """
        exist_tickets = Ticket.objects.filter(
            Q(status=PROCESSING), **parse_in_dict(base_params, ["ticket_type", "extra"])
        ).all()

        if exist_tickets.count() > 0:
            raise HasAlreadyExistsErr()
        return True

    def generate_tickets(self, permissions, base_params):
        """
        @param permissions:[]
        @return:
        """
        ticket_list = []
        ticket_step_params = base_params["extra"]["ticket_step_params"]
        process = self.get_process(self.ticket_type, ticket_step_params)
        process["permissions"] = []
        ticket_list.append(process)
        return ticket_list

    def create_ticket(self, ticket_list, base_params, auto_approve):
        """
        @param ticket_list:
        @param base_params:
        @param auto_approve:Determine whether to execute 'auto_approve' when creating ticket
        @return:
        """
        tickets = []
        with transaction.atomic(using="basic"):
            for ticket_data in ticket_list:
                base_params["process_length"] = ticket_data.get("process_length")
                ticket = self.BASE_MODEL.objects.create(**base_params)
                for state in ticket_data.get("states"):
                    create_params = {
                        "ticket": ticket,
                        "processors": state.get("processors"),
                        "process_step": state.get("process_step"),
                    }
                    TicketState.objects.create(**create_params)

                ticket.start_process(auto_approve)
                ticket.refresh_from_db()
                tickets.append({"ticket": self.SERIALIZER(ticket).data})
            tickets = self.SERIALIZER.wrap_permissions_display(tickets)
        return [_ticket.get("ticket") for _ticket in tickets]

    def add_permission(self, ticket):
        """
        审批通过后，添加权限
        """
        self.trigger_finally(ticket, SUCCEEDED)

    def after_terminate(self, ticket, status):
        """
        非审批通过，其他中止操作触发
        """
        self.trigger_finally(ticket, status)

    def trigger_finally(self, ticket, status):
        """
        单据最后终结事件
        """
        ticket_state = TicketState.objects.filter(ticket_id=ticket.id)
        operator = ticket_state[0].processed_by
        message = ticket_state[0].process_message
        process_id = ticket.extra["process_id"]
        callback_url = ticket.extra["callback_url"]

        if callback_url is None:
            logger.warning(f"[TICKET CALLBACK] Common ticket has no callback url for ticket(id={ticket.id})")
            return

        self.callback_request(
            callback_url,
            ticket.id,
            {
                "bk_username": operator,
                "operator": operator,
                "message": message,
                "process_id": process_id,
                "status": status,
            },
        )

    def callback_request(self, callback_url, ticket_id, data):
        try:
            session = requests.Session()
            # retries = 3
            # retry = Retry(
            #     total=retries,
            #     read=retries,
            #     connect=retries,
            #     backoff_factor=0.3,
            #     status_forcelist=(500, 502, 504),
            # )
            # adapter = HTTPAdapter(max_retries=retry)
            # session.mount('http://', adapter)
            # session.mount('https://', adapter)

            session.headers.update({"Content-Type": "application/json; chartset=utf-8"})
            raw_response = session.post(callback_url, data=json.dumps(data), timeout=10)

            logger.info(
                "[TICKET CALLBACK] Common ticket callback origin system for "
                "ticket(id={}), response={}".format(ticket_id, raw_response.text)
            )

            if raw_response.status_code != 200:
                raise AuthAPIError(f"The third system return not 200 response, {raw_response}")

            response_result = raw_response.json()
            if not response_result["result"]:
                raise AuthAPIError(f"The third system return not-ok response, {response_result}")

            logger.info(
                "[TICKET CALLBACK] Common ticket callback origin system succeed for " "ticket(id={})".format(ticket_id)
            )
        except Exception:
            logger.exception(
                ("[TICKET CALLBACK] Common ticket callback origin system failed for " "ticket(id={})").format(ticket_id)
            )
            raise TicketCallbackErr()

    def get_content_for_notice(self, ticket):
        """
        获取消息发送内容（common）
        @param [Ticket] ticket:
        @return:
        """
        tickets = []
        tickets.append({"ticket": self.SERIALIZER(ticket).data})
        wrap_ticket = self.SERIALIZER.wrap_permissions_display(tickets)
        ticket_display = wrap_ticket[0]["ticket"]
        apply_content = "{}".format(ticket_display["extra"]["content"])
        content = NoticeContent(
            created_by=ticket_display["created_by"],
            ticket_type=ticket_display["ticket_type_display"],
            apply_content=apply_content,
            reason=ticket_display["reason"],
        )
        return content


class BatchReCalcObj(CommonTicketObj):
    def trigger_finally(self, ticket, status):
        comfirm_type_map = {SUCCEEDED: "approved", FAILED: "rejected", STOPPED: "cancelled"}

        ticket_state = TicketState.objects.filter(ticket_id=ticket.id)
        recall_json = {
            "operator": ticket_state[0].processed_by,
            "message": ticket_state[0].process_message,
            "id": ticket.extra["process_id"],
            "confirm_type": comfirm_type_map[status],
        }
        DataflowApi.batch_recalc_callback(recall_json, raise_exception=True)
