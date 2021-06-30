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


from auth.constants import SensitivityMapping
from auth.handlers.resource_group import ResourceGroupHandler
from auth.handlers.result_table import ResultTableHandler
from auth.models.auth_models import AuthDataToken
from auth.models.base_models import RoleConfig
from auth.models.outer_models import ClusterGroupConfig
from common.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers
from rest_framework.fields import (
    BooleanField,
    CharField,
    ChoiceField,
    DictField,
    IntegerField,
    ListField,
)


class RoleSerializer(serializers.Serializer):
    role_id = serializers.CharField(max_length=64)
    role_name = serializers.CharField(max_length=64)
    object_class = serializers.CharField(max_length=64, source="object_class_id")
    allow_empty_member = serializers.BooleanField(default=True)
    order = serializers.IntegerField(default=1)
    user_mode = serializers.BooleanField(default=True)
    description = serializers.CharField(max_length=64, default="")


class RoleUserAddSerializer(serializers.Serializer):
    role_id = CharField(label=_("角色ID"))
    scope_id = CharField(label=_("域ID"))
    user_id = CharField(label=_("用户ID"))


class RoleUserUpdateSerializer(serializers.Serializer):
    role_id = CharField(label=_("角色ID"))
    user_ids = ListField(label=_("用户列表"), child=CharField(label=_("用户ID")), allow_null=True, allow_empty=True)
    scope_id = CharField(label=_("域ID"))

    def validate(self, attrs):
        if not attrs.get("user_ids"):
            attrs["user_ids"] = []
        try:
            role = RoleConfig.objects.get(pk=attrs.get("role_id"))
        except RoleConfig.DoesNotExist:
            raise ValidationError(_("参数错误，角色不存在"))

        if role.allow_empty_member is False and not attrs.get("user_ids"):
            raise ValidationError(_("%s该角色成员不能为空") % attrs.get("role_id"))

        return attrs


class MultiRoleUserUpdateSerializer(serializers.Serializer):
    role_users = ListField(label=_("角色用户列表"), child=RoleUserUpdateSerializer())


class RoleUserListParamSerializer(serializers.Serializer):
    scope_id = CharField(label=_("角色范围"), required=False, default=None)


class MultiRoleUserListParamSerializer(serializers.Serializer):
    scope_id = CharField(label=_("角色范围"), required=False, default=None)
    object_class = CharField(label=_("权限对象"))


class RoleUserUpdateParamSerializer(serializers.Serializer):
    scope_id = CharField(label=_("角色范围"), required=False, default=None)
    user_ids = ListField(label=_("用户列表"), child=CharField(label=_("用户ID")))


class ProjectUsersUpdateSerializer(serializers.Serializer):
    class _ChildSerializer(serializers.Serializer):
        role_id = CharField(label=_("角色ID"))
        user_ids = ListField(label=_("用户列表"), child=CharField(label=_("用户ID")))

    role_users = ListField(label=_("角色用户列表"), child=_ChildSerializer())

    def validate(self, attrs):
        valid_roles = RoleConfig.objects.filter(object_class="project")
        valid_role_ids = []
        for role in valid_roles:
            valid_role_ids.append(role.role_id)
        for role_user in attrs.get("role_users", []):
            role_id = role_user.get("role_id")
            if role_id not in valid_role_ids:
                raise ValidationError(_("提交的角色不合法%s") % role_id)
        return attrs


class RawDataUsersUpdateSerializer(ProjectUsersUpdateSerializer):
    def validate(self, attrs):
        valid_roles = RoleConfig.objects.filter(object_class="raw_data")
        valid_role_ids = []
        for role in valid_roles:
            valid_role_ids.append(role.role_id)
        for role_user in attrs.get("role_users", []):
            role_id = role_user.get("role_id")
            if role_id not in valid_role_ids:
                raise ValidationError(_("提交的角色不合法%s") % role_id)
        return attrs


class UserRoleSerializer(serializers.Serializer):
    object_class = CharField(label=_("对象类型"))
    scope_id = CharField(label=_("对象id"), required=False)
    object_name__contains = CharField(label=_("对象名搜索"), required=False)
    show_display = BooleanField(label=_("是否返回显示名"), required=False)
    page = IntegerField(label=_("页码"), required=False)
    page_size = IntegerField(label=_("分页数量"), required=False)
    is_user_mode = BooleanField(label=_("是否用户模式"), required=False, default=True)


class ScopeSerializer(serializers.Serializer):
    object_class = CharField(label=_("对象类型"))
    bk_biz_id = IntegerField(label=_("业务id"), required=False)
    project_id = IntegerField(label=_("项目id"), required=False)


class ObjectsScopeActionSerializer(serializers.Serializer):
    is_user_mode = BooleanField(label=_("是否用户模式"), required=False, default=True)


class PermissionScopesSerializer(serializers.Serializer):
    action_id = CharField(label=_("操作方式"))
    show_display = BooleanField(label=_("是否返回显示名"), required=False)
    show_admin_scopes = BooleanField(label=_("是否管理员范围"), required=False)
    add_tdw = BooleanField(label=_("是否考虑默认标准的TDW表"), required=False)
    tdw_filter = CharField(label=_("TDW表过滤参数"), required=False)
    bk_biz_id = IntegerField(label=_("支持业务参数过滤"), required=False)
    page = IntegerField(label=_("分页参数"), required=False)
    page_size = IntegerField(label=_("分页参数"), required=False, default=10)


class PermissionScopesDimensionSerializer(serializers.Serializer):
    action_id = CharField(label=_("操作方式"))
    dimension = CharField(label=_("维度字段"))


class PermissionCheckSerializer(serializers.Serializer):
    action_id = CharField(label=_("操作方式"))
    object_id = CharField(label=_("对象ID"), required=False, allow_null=True)
    display_detail = BooleanField(label=_("是否展示鉴权详情"), required=False, default=False)


class ProjectDataCheckSerializer(serializers.Serializer):
    result_table_id = CharField(label=_("结果表ID"), required=False, default=None)  # 不仅仅有 result_table_id，该字段在历史进程中逐渐废弃
    object_id = CharField(label=_("对象ID"), required=False, default=None)
    action_id = CharField(label=_("操作方式"), required=False, default="result_table.query_data")

    def validate(self, data):
        if data["result_table_id"] is None and data["object_id"] is None:
            raise ValidationError(_("鉴权对象必须要要传，请传递 object_id 参数"))

        if data["object_id"] is None:
            data["object_id"] = data["result_table_id"]

        return data


class ClusterGroupConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ClusterGroupConfig
        fields = "__all__"


class ProjectClusterGroupCheckSerializer(serializers.Serializer):
    cluster_group_id = CharField(label=_("集群组ID"))


class AuthSyncSerializer(serializers.Serializer):
    data_set_deletions = ListField(label=_("被删除的数据集"), child=DictField(label=_("被删除的数据集")))
    data_set_influences = ListField(label=_("受影响的数据集"), child=DictField(label=_("受影响的数据集")))


class AuthDataTokenSerializer(serializers.ModelSerializer):
    """
    授权码序列化器
    """

    status = serializers.CharField()
    status_display = serializers.CharField()
    expired_nearly = serializers.BooleanField()

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        token = ret["data_token"]
        digit = 4
        # 取前四位和最后四位，中间掩码
        ret["data_token"] = "".join([token[:digit], "*" * (digit * 2), token[len(token) - digit :]])
        return ret

    class Meta:
        model = AuthDataToken
        fields = "__all__"


class DataTokenUpdateSerializer(serializers.Serializer):
    data_scope = DictField(label=_("对象范围"))
    reason = CharField(label=_("申请理由"))
    expire = IntegerField(label=_("过期时间"), min_value=0)

    def validate_data_scope(self, data_scope):
        """
        针对不同功能申请，需要先限定下申请范围
        """
        permissions = data_scope["permissions"]

        # 检查订阅结果表是否有队列存储
        result_table_ids_for_query_queue = [
            perm["scope"]["result_table_id"] for perm in permissions if perm["action_id"] == "result_table.query_queue"
        ]

        if len(result_table_ids_for_query_queue) > 0:
            rts = ResultTableHandler.list_by_rt_ids_with_storages(result_table_ids_for_query_queue)

            not_exist_queue_rt_ids = [rt.result_table_id for rt in rts if not rt.has_queue()]
            if len(not_exist_queue_rt_ids) > 0:
                raise ValidationError(f"没有队列存储的结果表（{not_exist_queue_rt_ids}）不支持申请《数据订阅》权限")

            not_exist_rt_ids = [
                rt_id for rt_id in result_table_ids_for_query_queue if rt_id not in [rt.result_table_id for rt in rts]
            ]
            if len(not_exist_rt_ids) > 0:
                raise ValidationError(f"结果表不存在（{not_exist_rt_ids}）")

        # 暂时授权码仅支持申请有查询集群的资源组，其他集群的资源组申请没有用处
        resource_group_ids_for_use = [
            perm["scope"]["resource_group_id"] for perm in permissions if perm["action_id"] == "resource_group.use"
        ]

        if len(resource_group_ids_for_use) > 0:
            presto_resource_group_ids = ResourceGroupHandler().list_res_grp_id_by_cluster_type("presto")
            not_presto_resource_group_ids = [
                _id for _id in resource_group_ids_for_use if _id not in presto_resource_group_ids
            ]
            if len(not_presto_resource_group_ids) > 0:
                raise ValidationError(
                    "暂时仅支持申请 presto 集群所在的资源组，请删除移除不支持的资源组（{}）" "".format(not_presto_resource_group_ids)
                )

        return data_scope


class DataTokenRetrieveSerializer(serializers.Serializer):
    permission_status = CharField(label=_("权限状态"), required=False, default="active")
    show_display = BooleanField(label=_("展示权限对象属性"), required=False, default=True)
    show_scope_structure = BooleanField(label=_("显示授权码组织结构"), required=False, default=True)


class DataTokenCreateSerializer(DataTokenUpdateSerializer):
    data_token_bk_app_code = CharField(label=_("蓝鲸app"))


class DataTokenCheckSerializer(serializers.Serializer):
    check_data_token = CharField(label=_("Data Token"))
    check_app_code = CharField(label=_("APP CDDE"))
    action_id = CharField(label=_("操作方式"))
    object_id = CharField(label=_("对象ID"), required=False, allow_null=True)


class TdwUserSerializer(serializers.Serializer):
    tdw_username = CharField(label=_("tdw用户名"))
    tdw_password = CharField(label=_("tdw密码"))


class DbListSerializer(serializers.Serializer):
    cluster_id = CharField(label=_("集群id"), required=False)


class TableListSerializer(DbListSerializer):
    db_name = CharField(label=_("库名"))


class TablePermCheckSerializer(TableListSerializer):
    table_name = CharField(label=_("表名"))


class TdwAppGroupCheckSerializer(serializers.Serializer):
    app_group = CharField(label=_("TDW应用组"))


class TdwAppGroupListByProjectSerializer(serializers.Serializer):
    project_id = IntegerField(label=_("项目id"))


class TdwAppGroupGetByFlowSerializer(serializers.Serializer):
    flow_id = IntegerField(label=_("任务id"))


class ProjectDataSerializer(serializers.Serializer):
    bk_biz_id = IntegerField(label=_("业务id"), required=False)
    page = IntegerField(label=_("是否返回分页结构"), required=False)
    with_queryset = BooleanField(label=_("是否返回 QuerySet 类型结果表"), required=False, default=False)
    action_id = CharField(label=_("操作方式"), required=False, default="result_table.query_data")
    extra_fields = BooleanField(label=_("是否返回额外字段"), required=False, default=False)


class ProjectDataTicketSerializer(serializers.Serializer):
    bk_biz_id = IntegerField(label=_("业务id"), required=False, default=None)
    action_ids = CharField(label=_("操作列表"), required=False, default="result_table.query_data")


class ProjectAddDataSerializer(serializers.Serializer):
    bk_biz_id = IntegerField(label=_("业务id"))
    result_table_id = CharField(label=_("结果表ID"), required=False, default=None)  # 兼容参数，逐渐不支持了
    object_id = CharField(label=_("对象ID"), required=False, default=None)
    action_id = CharField(label=_("操作方式"), required=False, default="result_table.query_data")

    def validate(self, data):
        if data["result_table_id"] is None and data["object_id"] is None:
            raise ValidationError(_("授权对象必须要传，请传递 object_id 参数"))

        if data["object_id"] is None:
            data["object_id"] = data["result_table_id"]

        return data


class PermissionBatchCheckItemSerializer(serializers.Serializer):
    user_id = CharField(label=_("用户ID"))
    action_id = CharField(label=_("操作方式"))
    object_id = CharField(label=_("对象ID"), required=False, allow_null=True)


class PermissionBatchCheckSerializer(serializers.Serializer):
    permissions = ListField(label=_("鉴权列表"), child=PermissionBatchCheckItemSerializer())
    display_detail = BooleanField(label=_("是否展示鉴权详情"), required=False, default=False)


class DgraphScopesBatchCheckSerializer(serializers.Serializer):
    class DgraphScopesBatchCheckItemSerializer(serializers.Serializer):
        user_id = CharField(label=_("用户ID"))
        action_id = CharField(label=_("操作方式"))
        variable_name = CharField(label=_("Dgraph语句变量名称"))
        metadata_type = CharField(label=_("元数据对象类型"))

    permissions = ListField(label=_("鉴权列表"), child=DgraphScopesBatchCheckItemSerializer())


class UserHandoverSerializer(serializers.Serializer):
    receiver = CharField(label=_("被交接人"))


class SensitivityListSerializer(serializers.Serializer):
    has_biz_role = BooleanField(required=False, default=False)
    bk_biz_id = IntegerField(required=False)


class DataSetSensitivityRetrieveSerializer(serializers.Serializer):
    data_set_type = ChoiceField(choices=["raw_data", "result_table"])
    data_set_id = CharField()


class DataSetSensitivityUpdateSerializer(serializers.Serializer):
    data_set_type = ChoiceField(label=_("数据集类型"), choices=["raw_data", "result_table"])
    data_set_id = CharField(label=_("数据集ID"))
    tag_method = ChoiceField(label=_("标记方式"), choices=["user", "default"])
    sensitivity = ChoiceField(label=_("敏感度"), choices=list(SensitivityMapping.keys()), required=False)

    def validate(self, data):
        if data["tag_method"] in ["user"]:
            sensitivity = data.get("sensitivity", None)
            if not sensitivity:
                raise ValidationError(_("当标记为 User 模式，敏感度字段必须传"))

            if not SensitivityMapping[sensitivity]["active"]:
                raise ValidationError(_("暂不支持当前敏感度，请设置为其他敏感度"))

        return data


class IamResrouseSerializer(serializers.Serializer):
    print()
