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

from auth import exceptions
from auth.constants import SUBJECTS, USER
from auth.handlers.object_classes import oFactory
from auth.handlers.project import ProjectHandler
from auth.handlers.raw_data import RawDataHandler
from auth.handlers.result_table import ResultTableHandler
from auth.models import (
    DataTicketPermission,
    RoleTicketPermission,
    Ticket,
    TicketState,
    TokenTicketPermission,
)
from auth.models.base_models import ActionConfig, ObjectConfig, RoleConfig
from auth.models.outer_models import ProjectInfo, ResultTable
from auth.utils.serializer import SelfDRFDateTimeField, SelfDRFRawCharField
from common.log import logger
from django.utils.translation import ugettext as _
from rest_framework import serializers


class BaseTicketSerializer(serializers.ModelSerializer):
    """
    单据基类序列化，单据序列化器有以下两个用处

    1. 主要校验前端传入参数
    2. 将DB存储数据，返回值前端展示时，可以增添，调整显示内容
    """

    extra = serializers.DictField(required=False)
    ticket_type_display = serializers.CharField(source="get_ticket_type_display", read_only=True)
    status_display = serializers.CharField(source="get_status_display", read_only=True)
    process_length = serializers.IntegerField(required=False)

    class Meta:
        model = Ticket
        fields = "__all__"

    @staticmethod
    def wrap_permissions_display(data):
        """
        将单据核心内容 permissions 进行补充，平整化展示，需要子类实现，针对 data.permissions 进行转化

        @resultExample permissions {Dict}
            [
                {
                    subject_class: "user",
                    subject_class_name: "用户",
                    subject_id: "user01",
                    subject_name: "user01",

                    action: "add_role",
                    action_name: "数据开发员",

                    object_class: "project",
                    object_class_name: "项目",
                    scope_object: {scope_name: "[3945]测试项目", scope_object_class_name: "项目"}
                }
            ]
        """
        return data

    def to_representation(self, instance):
        """
        除了返回单据基本信息以后，还需要补充必须字段

        @returnExample {Dict}
            {
                # BaseInfo
                id: 471
                created_at: "2019-06-10 17:23:44"
                created_by: "user01"
                end_time: "2019-06-10 17:23:44"
                extra: "{}"
                reason: ""
                status: "succeeded"
                status_display: "已同意"
                ticket_type: "apply_role"
                ticket_type_display: "申请角色",

                # StateInfo
                state_id: 431,
                processors: ["user1", "user2", "user3"],
                process_length: 1,
                process_step: 1,

                # Permission Info
                permissions: [],

                # Extra Info
                extra_info : {
                    'content': ''
                }
            }
        """
        ret = super().to_representation(instance)
        current_step = ret["process_step"]
        step = current_step - 1 if current_step == ret["process_length"] else current_step
        # 同一step可能存在多个state，比如一个RT需要两个人同时审批通过时
        # @todo 性能可优化
        current_state = instance.states.filter(process_step=step).order_by("processed_at").first()
        if current_state is None:
            ret["processors"] = []
            ret["state_id"] = -1
        else:
            ret["processors"] = current_state.processors
            ret["state_id"] = current_state.id
        return ret


class TicketStateSerializer(serializers.ModelSerializer):
    """
    单据状态节点序列化
    """

    status_display = serializers.CharField(source="get_status_display", read_only=True)
    processed_at = SelfDRFDateTimeField(required=False)
    processors = SelfDRFRawCharField(allow_blank=True)

    class Meta:
        model = TicketState
        fields = "__all__"

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        return ret


class DataPermissionItemSerializer(serializers.ModelSerializer):
    """
    申请数据相关权限子单的序列化
    """

    ticket = serializers.CharField(required=False)
    key = serializers.CharField(required=False)
    value = serializers.CharField(required=False)
    scope = serializers.DictField()

    class Meta:
        fields = "__all__"
        model = DataTicketPermission

    def validate(self, attrs):
        """
        校验数据，保证项目申请数据的合法性
        """
        if attrs.get("object_class") in ["result_table", "raw_data"] and attrs.get("subject_class") == "project":
            # 校验项目的区域标签与所申请数据的区域标签是否一致
            project_id = attrs.get("subject_id")
            project_area_tags = ProjectHandler(project_id).geo_tags

            if "result_table_id" in attrs["scope"]:
                object_id = attrs["scope"].get("result_table_id")
                object_area_tags = ResultTableHandler(object_id).geo_tags
            elif "raw_data_id" in attrs["scope"]:
                object_id = attrs["scope"].get("raw_data_id")
                object_area_tags = RawDataHandler(object_id).geo_tags
            else:
                object_area_tags = []

            # 约定了业务数据、项目一定会有区域标签，若没有则视为不合法
            if not project_area_tags or not object_area_tags:
                raise exceptions.ProjectDataTagValidErr()

            # 业务数据的标签必须是项目标签的子集，否则视为不合法
            if set(object_area_tags) - set(project_area_tags):
                raise exceptions.ProjectDataTagValidErr()

        return super().validate(attrs)

    def to_representation(self, instance):
        """
        对返回结果进行封装
        @param instance:
        @return:
        """
        return super().to_representation(instance)

    def _get_name_from_id(self, object_id, object_type):
        """
        通过类型和id获取name
        @param object_id:
        @param object_type:
        @return:
        """
        unknown = f"unknown {object_type}"
        if not object_id:
            return unknown
        if object_type == "biz":
            biz_list = None
            if self.context:
                biz_list = self.context.get("business")
            return biz_list.get(int(object_id), unknown) if biz_list else unknown
        elif object_type == "project":
            project = ProjectInfo.objects.filter(project_id=object_id).first()
            return project.project_name if project else unknown
        elif object_type == "result_table":
            result_table = ResultTable.objects.filter(result_table_id=object_id).first()
            return result_table.result_table_name if result_table else unknown
        else:
            return unknown

    def _get_wrapper_from_id(self, object_id, object_type):
        """
        包装id和name
        @param object_id:
        @param object_type:
        @return:
        """
        return {"id": object_id, "name": self._get_name_from_id(object_id, object_type)}


class RolePermissionItemSerializer(serializers.ModelSerializer):
    """
    申请角色相关权限子单的序列化
    """

    ticket = serializers.CharField(required=False)

    class Meta:
        fields = "__all__"
        model = RoleTicketPermission


class TokenPermissionItemSerializer(DataPermissionItemSerializer):
    """
    申请token相关权限子单的序列化
    """

    def to_representation(self, instance):
        """
        对返回结果进行封装
        @param instance:
        @return:
        """
        return super().to_representation(instance)

    class Meta:
        model = TokenTicketPermission
        fields = "__all__"


class DataTicketSerializer(BaseTicketSerializer):
    permissions_serializer = DataPermissionItemSerializer

    @staticmethod
    def wrap_permissions_display(data):
        """
        @param data:
        many 为False 时，data = []
        @return:
        """
        # 添加前端需显示的字段
        object_name_map = ObjectConfig.get_object_config_map()
        action_name_map = ActionConfig.get_action_name_map()

        data = oFactory.wrap_display_all(
            data,
            output_val_key="scope_id",
            key_path=["ticket", "permissions", "scope"],
            display_key_maps={"display_name_key": "object_name"},
        )

        for _data in data:
            for perm in _data["ticket"]["permissions"]:
                perm["subject_class"] = perm.get("subject_class")
                perm["subject_class_name"] = [item for item in SUBJECTS if item[0] == perm["subject_class"]][0][1]
                object_class = perm["object_class"]
                perm["object_class"] = object_class
                perm["object_class_name"] = object_name_map[object_class]["object_name"]
                action = perm["action"]
                perm["action_name"] = action_name_map.get(action, action)

                # 特殊处理，若没有显示名的，以scope_id作为显示名
                if not perm["scope"].get("object_name"):
                    perm["scope"]["object_name"] = perm.get("scope_id")

                perm["scope_object"] = {
                    "scope_name": perm["scope"]["object_name"],
                    "scope_object_class_name": perm["object_class_name"],
                }
        return data

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        ret["permissions"] = self.permissions_serializer(
            DataTicketPermission.objects.filter(ticket_id=instance.pk).all(), many=True, context=self.context
        ).data
        return ret


class RoleTicketSerializer(BaseTicketSerializer):
    permissions_serializer = RolePermissionItemSerializer

    @staticmethod
    def wrap_permissions_display(data):

        if not len(data):
            return []

        # 添加前端需显示的字段
        role_name_map = RoleConfig.get_role_config_map()
        role_object_map = RoleConfig.get_role_object_map()

        # 这里认为同一张申请单中申请的的角色是一样的
        try:
            role_id = data[0]["ticket"]["permissions"][0]["role_id"]
        except (IndexError, KeyError):
            logger.warning(_("单据内容缺失 %s" % json.dumps(data)))
            return data

        d_object_class = role_object_map.get(role_id)

        if d_object_class is None:
            logger.error(_("【AUTH】角色%s非法或未定义" % role_id))
            return data

        data = oFactory.init_object_by_class(d_object_class["object_class"]).wrap_display(
            data,
            input_val_key="scope_id",
            output_val_key="scope_id",
            display_key_maps={"display_name_key": "object_name"},
            key_path=["ticket", "permissions"],
        )

        for _data in data:

            for perm in _data["ticket"]["permissions"]:
                perm["subject_id"] = perm["user_id"]
                perm["subject_class"] = USER
                perm["subject_name"] = perm["user_id"]
                perm["subject_class_name"] = [item for item in SUBJECTS if item[0] == USER][0][1]
                role_id = perm["role_id"]
                perm["object_class"] = role_object_map.get(role_id, {}).get("object_class", role_id)
                perm["object_class_name"] = role_object_map.get(role_id, {}).get("object_name", role_id)
                perm["action_name"] = role_name_map.get(role_id, {}).get("role_name", role_id)

                # # 特殊处理，若没有显示名的，以scope_id作为显示名
                if not perm.get("object_name"):
                    perm["object_name"] = perm.get("scope_id")

                perm["scope_object"] = {
                    "scope_name": perm["object_name"],
                    "scope_object_class_name": d_object_class["object_name"],
                }
        return data

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        ret["permissions"] = self.permissions_serializer(
            RoleTicketPermission.objects.filter(ticket_id=instance.pk).all(), many=True, context=self.context
        ).data
        return ret


class TokenTicketSerializer(DataTicketSerializer):
    permissions_serializer = TokenPermissionItemSerializer

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        ret["permissions"] = self.permissions_serializer(
            TokenTicketPermission.objects.filter(ticket_id=instance.pk).all(), many=True, context=self.context
        ).data
        return ret


class CommonTicketSerializer(BaseTicketSerializer):
    """
    通用单据序列化器，关注 BaseTicketSerializer 实现 wrap_permissions_display + to_representation 必要逻辑
    """

    @staticmethod
    def wrap_permissions_display(data):

        if not len(data):
            return []
        # permission如果没有任何值的话，create_by只能体现用户，不能确定用户是哪个角色,permission没有意义
        new_data = []
        for item in data:
            item["ticket"]["permission"] = []
            new_data.append(item)
        return new_data

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        extra_dict = instance.extra
        ret["extra_info"] = {"process_id": extra_dict["process_id"], "content": extra_dict["content"]}
        return ret
