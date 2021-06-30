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


import operator
from datetime import timedelta
from functools import reduce

from auth.api import DatabusApi
from auth.constants import TokenPermissionStatus
from auth.exceptions import DataScopeFormatErr, TokenExpiredErr
from auth.models.base_models import ObjectConfig
from auth.models.manages import BaseManager
from auth.utils import generate_md5, generate_random_string
from common.base_utils import model_to_dict
from common.local import get_request_username
from common.log import logger
from common.transaction import meta_sync_register
from django.db import models
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _


class AUTH_STATUS:
    NORMAL = "normal"
    EXPIRED = "expired"
    INVALID = "invalid"

    CHOICES = ((NORMAL, _("正常")), (EXPIRED, _("过期")), (INVALID, _("作废")))


class UserRoleManager(BaseManager):
    def create(self, *args, **kwargs):
        if "created_by" not in kwargs:
            kwargs["created_by"] = get_request_username()
        if "updated_by" not in kwargs:
            kwargs["updated_by"] = get_request_username()

        # 由于正常查询 UserRole 不会返回非法的关系，则新增时，需要避免冲突
        user_id = kwargs["user_id"]
        scope_id = kwargs.get("scope_id", None)
        role_id = kwargs["role_id"]

        match_objects = self.match_objects(user_id, role_id, scope_id)

        if len(match_objects) > 0:
            match_objects.delete()

        return super(BaseManager, self).create(*args, **kwargs)

    def bulk_create(self, *args, **kwargs):
        for _obj in args[0]:
            if not _obj.created_by:
                _obj.created_by = get_request_username()
            if not _obj.updated_by:
                _obj.updated_by = get_request_username()

            match_objects = self.match_objects(_obj.user_id, _obj.role_id, getattr(_obj, "scope_id", None))
            if len(match_objects) > 0:
                match_objects.delete()

        return super(BaseManager, self).bulk_create(*args, **kwargs)

    def match_objects(self, user_id, role_id, scope_id):
        """
        查询是否存在其他非法状态的对象列表

        @return {queryset}
        """
        return UserRole.origin_objects.filter(
            user_id=user_id,
            role_id=role_id,
            scope_id=scope_id,
            auth_status__in=[AUTH_STATUS.INVALID, AUTH_STATUS.EXPIRED],
        )

    def get_queryset(self):
        return super().get_queryset().filter(auth_status=AUTH_STATUS.NORMAL)


class UserRole(models.Model):
    """
    权限个体与角色关系信息
    """

    user_id = models.CharField(max_length=64)
    # role_id = models.ForeignKey(RoleConfig, models.DO_NOTHING, db_column='role_id')
    role_id = models.CharField(max_length=64)
    scope_id = models.CharField(max_length=128, blank=True, null=True)
    auth_status = models.CharField(max_length=32, choices=AUTH_STATUS.CHOICES, default=AUTH_STATUS.NORMAL)
    expired_date = models.DateTimeField(blank=True, null=True)
    created_by = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=64)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)
    objects = UserRoleManager()
    origin_objects = BaseManager()

    @classmethod
    def user_roles(cls, user_id, scope_id=None, object_class=None):
        """
        用户所拥有的角色
        @return:
        """
        from auth.models.base_models import RoleConfig

        role_ids = RoleConfig.get_object_roles_map().get(object_class, [])
        query_condition = Q(user_id=user_id)
        if object_class is not None:
            query_condition = Q(user_id=user_id, role_id__in=role_ids)
            if scope_id is not None:
                query_condition = Q(user_id=user_id, role_id__in=role_ids, scope_id=scope_id)

        data = [model_to_dict(user_role) for user_role in cls.objects.filter(query_condition)]
        if not data and len(role_ids) > 0 and scope_id is not None:
            data = [
                {
                    "scope_id": scope_id,
                    "role_id": role_ids[0],
                    "user_id": user_id,
                }
            ]

        return data

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_user_role"


class ProjectDataManager(models.Manager):
    """
    主要处理内部操作时，自动补全必要字段，比如 created_by updated_by
    """

    def create(self, *args, **kwargs):
        if "created_by" not in kwargs:
            kwargs["created_by"] = get_request_username()
        return super().create(*args, **kwargs)

    def bulk_create(self, *args, **kwargs):
        for _obj in args[0]:
            if not _obj.created_by:
                _obj.created_by = get_request_username()

        return super().bulk_create(*args, **kwargs)


class ProjectData(models.Model):
    """
    项目与数据的关系
    """

    project_id = models.IntegerField(_("项目ID"))
    bk_biz_id = models.IntegerField(_("业务ID"))
    result_table_id = models.CharField(_("结果表标识"), max_length=255, null=True)

    active = models.BooleanField(default=True)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    description = models.TextField(_("作业描述"), blank=True)

    objects = ProjectDataManager()

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "project_data"


class ProjectRawData(models.Model):
    """
    项目与原始数据的关系
    """

    project_id = models.IntegerField(_("项目ID"))
    bk_biz_id = models.IntegerField(_("业务ID"))
    raw_data_id = models.CharField(_("结果表标识"), max_length=255, null=True)

    active = models.BooleanField(default=True)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    description = models.TextField(_("作业描述"), blank=True)

    objects = ProjectDataManager()

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "project_rawdata"


class ProjectClusterGroupConfig(models.Model):
    """
    项目和集群组的关系
    """

    project_id = models.IntegerField(_("项目ID"))
    cluster_group_id = models.CharField(_("集群组ID"), primary_key=True, max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    created_by = models.CharField("创建者", max_length=128)
    description = models.TextField("描述信息", null=True, blank=True)

    class Meta:
        db_table = "project_cluster_group_config"
        app_label = "basic"
        managed = False


class AuthDataTokenManager(BaseManager):
    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .filter(
                _status__in=[AuthDataToken.STATUS.ENABLED, AuthDataToken.STATUS.EXPIRED, AuthDataToken.STATUS.DISABLED]
            )
        )


class AuthDataToken(models.Model):
    """
    授权码
    """

    class STATUS:

        # 过期的状态不直接写入DB, 通过过期时间expired_at得出
        EXPIRED = "expired"
        DISABLED = "disabled"
        ENABLED = "enabled"
        REMOVED = "removed"

        CHOICE = (
            (EXPIRED, _("已过期")),
            (DISABLED, _("已禁用")),
            (ENABLED, _("已启用")),
            (REMOVED, _("已移除")),
        )

    data_token = models.CharField(max_length=64)
    data_token_bk_app_code = models.CharField(max_length=64)
    _status = models.CharField(max_length=64, choices=STATUS.CHOICE, db_column="status", default=STATUS.ENABLED)
    expired_at = models.DateTimeField(blank=True, null=True)
    created_by = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=64)
    updated_at = models.DateTimeField(blank=True, null=True, auto_now=True)
    description = models.TextField(blank=True, null=True)

    objects = AuthDataTokenManager()
    origin_objects = BaseManager()

    @classmethod
    def create(cls, bk_username, data_token_bk_app_code, expired_at):
        """
        随机生成token
        """
        length = AuthDataToken._meta.get_field("data_token").max_length
        data_token = generate_random_string(length=length)
        params = {"created_by": bk_username, "data_token_bk_app_code": data_token_bk_app_code, "expired_at": expired_at}
        o_token, created = AuthDataToken.objects.get_or_create(defaults=params, **{"data_token": data_token})
        if not created:
            # 假设token重复了的情况，重新再生成，触发概率极低
            return cls.create(*params)

        return o_token.data_token

    @classmethod
    def get_status_queryset(cls, l_status=None):
        """
        状态查询集
        @param l_status: 状态列表
        @return:
        """
        queryset = cls.objects.all()

        # l_status为None或者空列表
        if not l_status:
            return queryset

        status__in_query = Q(_status__in=l_status)
        if cls.STATUS.EXPIRED in l_status:
            # 过期的或者指定查询状态
            return queryset.filter(Q(expired_at__lte=timezone.now()) | status__in_query)
        else:
            return queryset.filter(Q(expired_at__gt=timezone.now()) & status__in_query)

    def is_expired(self, raise_exception=False):
        """
        是否过期
        @return:
        """
        # 未配置过期时间时，认为永不过期
        if self.expired_at is None:
            return False

        if self.expired_at > timezone.now():
            return False

        if raise_exception:
            raise TokenExpiredErr()
        else:
            return True

    def update_permission_status(self, data_scope, status):
        """
        激活权限
        @param data_scope:
        @param status: 权限状态
        @return:
        """
        query_condition = []
        for _perm in data_scope.get("permissions", []):
            scope_id_key, scope_id = self.clean_scope(_perm)
            query_condition.append(
                Q(
                    action_id=_perm["action_id"],
                    object_class=_perm["object_class"],
                    scope_id_key=scope_id_key,
                    scope_id=scope_id,
                )
            )
        if query_condition:
            AuthDataTokenPermission.objects.filter(data_token=self).filter(
                reduce(operator.or_, query_condition)
            ).update(status=status)

    def apply_permission(self, data_scope):
        """
        设置token对应的权限
        @param data_scope: 对象范围
        @paramExample data_scope:
        {
          'is_all': False,
          'permissions': [
            {
              'scope_id_key': 'bk_biz_id',
              'scope_object_class': 'biz',
              'scope_name_key': 'bk_biz_name',
              'scope_display': {
                'bk_biz_name': '蓝鲸基础计算平台'
              },
              'scope': {
                'bk_biz_id': '591'
              },
              'object_class': 'biz',
              'action_id': 'biz.access_raw_data'
            }
          ]
        }
        @return:
        """

        # 已存在的授权码权限
        existed_permissions = AuthDataTokenPermission.objects.filter(data_token=self).values(
            "action_id", "object_class", "scope_id_key", "scope_id"
        )
        tuple_existed_permissions = [
            (
                existed_perm["action_id"],
                existed_perm["object_class"],
                existed_perm["scope_id_key"],
                existed_perm["scope_id"],
            )
            for existed_perm in existed_permissions
        ]
        bulk_permission = []
        for _perm in data_scope.get("permissions", []):
            action_id = _perm["action_id"]
            object_class = _perm["object_class"]
            scope_id_key, scope_id = self.clean_scope(_perm)
            if (action_id, object_class, scope_id_key, scope_id) not in tuple_existed_permissions:
                bulk_permission.append(
                    AuthDataTokenPermission(
                        data_token=self,
                        action_id=action_id,
                        object_class=object_class,
                        status=TokenPermissionStatus.APPLYING,
                        # scope内只可能有一个对象范围
                        scope_id_key=scope_id_key,
                        scope_id=scope_id,
                    )
                )
        AuthDataTokenPermission.objects.bulk_create(bulk_permission)

    @staticmethod
    def clean_scope(permission):
        """
        清洗数据范围
        @param permission:
        @paramExample permission:
        {
            "action_id":"result_table.query",
            "object_class":"result_table",
            "scope_id_key":"result_table_id",
            "scope_name_key":"result_table_name",
            "scope_object_class":"result_table",
            "scope":{
                "result_table_id":"591_result_table",
                "result_table_name":"结果表1"
            }
        }
        创建类的对象范围的scope为None
        {
            "action_id": "project.create",
            "object_class": "project",
            "scope": None
        },
        @return: tuple ('project_id', '1')
        """
        if permission["scope"] is None:
            scope_id_key = None
            scope_id = None
        else:
            if len(list(permission["scope"].keys())) != 1 or len(list(permission["scope"].values())) != 1:
                logger.error("【DATA_TOKEN】数据格式错误 %s" % permission)
                raise DataScopeFormatErr()

            # scope内只可能有一个对象范围
            try:
                scope_id_key = list(permission["scope"].keys())[0]
                scope_id = list(permission["scope"].values())[0]
            except (KeyError, IndexError):
                logger.error("【DATA_TOKEN】数据格式错误 %s" % permission)
                raise DataScopeFormatErr()
        return scope_id_key, scope_id

    @property
    def permissions(self):
        """
        token已申请的权限
        """
        return self.list_permissions([TokenPermissionStatus.ACTIVE])

    def list_permissions(self, status_arr):
        """
        @return:
            [
                {
                    'scope_id_key': 'bk_biz_id',
                    'scope_object_class': 'biz',
                    'scope_name_key': 'bk_biz_name',
                    'scope': {
                        'bk_biz_id': '591'
                    },
                    'object_class': 'biz',
                    'action_id': 'biz.access_raw_data'
                }
            ]
        """
        o_permissions = AuthDataTokenPermission.objects.filter(data_token=self, status__in=status_arr)
        permissions = []
        for _perm in o_permissions:
            data = model_to_dict(_perm)
            data["scope_object_class"] = data["object_class"]
            data["scope"] = {data["scope_id_key"]: data.pop("scope_id")}
            permissions.append(data)
        return permissions

    @property
    def scopes(self):
        """
        token已申请的权限对应的对象范围
        @return:
        [
            {
                scope_object_classes: [
                    {
                        scope_id_key: "bk_biz_id",
                        scope_name_key: "bk_biz_name",
                        scope_object_class: "biz",
                        scope_object_name: "业务"
                    }
                ],
                object_class_name: "业务",
                action_name: "数据接入",
                has_instance: true,
                object_class: "biz",
                action_id: "biz.access_raw_data"
            }
        ]
        """
        included_action_ids = [perm["action_id"] for perm in self.permissions]
        object_scope_actions = []
        for object_scope_action in ObjectConfig.get_object_scope_action():
            for _action in object_scope_action["actions"]:
                if _action["action_id"] in included_action_ids:
                    object_scope_actions.append(
                        {
                            "object_class_name": object_scope_action["object_class_name"],
                            "object_class": object_scope_action["object_class"],
                            "scope_object_classes": _action["scope_object_classes"],
                            "action_name": _action["action_name"],
                            "has_instance": _action["has_instance"],
                            "action_id": _action["action_id"],
                        }
                    )
        return object_scope_actions

    def renewal(self, days):
        """
        续期，如果未进行续期操作则返回 False
        """
        if self.expired_nearly:
            self.expired_at = self.expired_at + timedelta(days=days)
            self._status = self.STATUS.ENABLED
            self.save(update_fields=["expired_at", "_status"])
            return True

        return False

    @property
    def status(self):
        """
        token状态，过期状态通过判断是否过期进行更新
        @return:
        """
        if self.is_expired():
            self._status = self.STATUS.EXPIRED
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def status_display(self):
        return self.get__status_display()

    @property
    def expired_nearly(self):
        """
        是不是在过期区间内，即在 (expired_at - 14days ~ expired_ar + 7days)
        """
        if self.expired_at is None:
            return False

        start_point = timezone.now() - timedelta(days=14)
        end_point = timezone.now() + timedelta(days=14)
        if start_point <= self.expired_at <= end_point:
            return True

        return False

    class Meta:
        db_table = "auth_data_token"
        app_label = "auth"
        ordering = ["-created_at", "expired_at"]
        managed = False


class AuthDataTokenPermission(models.Model):
    """
    授权码权限
    """

    status = models.CharField(max_length=64, choices=TokenPermissionStatus.CHOICE)
    data_token = models.ForeignKey(AuthDataToken, models.DO_NOTHING)
    action_id = models.CharField(max_length=64)
    object_class = models.CharField(max_length=32)
    scope_id_key = models.CharField(max_length=64)
    scope_id = models.CharField(max_length=128, blank=True, null=True)
    created_by = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=64)
    updated_at = models.DateTimeField(blank=True, null=True, auto_now=True)
    description = models.TextField(blank=True, null=True)

    objects = BaseManager()

    class Meta:
        db_table = "auth_data_token_permission"
        app_label = "auth"
        managed = False


class DataTokenQueueUser(models.Model):
    """
    队列服务用户表
    """

    queue_user = models.CharField(max_length=128, primary_key=True)
    queue_password = models.CharField(max_length=255)
    data_token = models.CharField(max_length=64)

    @classmethod
    def get_or_init_by_id(cls, data_token_id):
        o_token = AuthDataToken.objects.get(pk=data_token_id)
        return cls.get_or_init(o_token.data_token)

    @classmethod
    def get_or_init(cls, data_token):
        o_token = AuthDataToken.objects.get(data_token=data_token)

        if cls.objects.filter(data_token=data_token).exists():
            return cls.objects.get(data_token=data_token)

        # 队列服务账号名，最好跟 APP_CODE 有直接体现，较容易辨认
        queue_user = "{}&{}".format(o_token.data_token_bk_app_code, generate_md5(data_token))
        queue_password = generate_random_string(50)

        api_params = {"user": queue_user, "password": queue_password}
        DatabusApi.add_queue_user(api_params, raise_exception=True)

        # 判断是否存在，再创建，中间停留过长时间，需要再次判断是否存在
        obj, created = cls.objects.update_or_create(
            data_token=data_token, defaults={"queue_user": queue_user, "queue_password": queue_password}
        )
        return obj

    def register_result_tables(self, result_table_ids):
        """
        添加用户对结果数据的访问权限
        """
        api_params = {"user": self.queue_user, "result_table_ids": result_table_ids}
        DatabusApi.add_queue_auth(api_params, raise_exception=True)

    class Meta:
        db_table = "auth_data_token_queue_user"
        app_label = "auth"
        managed = False


class RawDataClusterGroupConfig(models.Model):
    """
    项目和集群组的关系
    """

    raw_data_id = models.IntegerField(_("原始数据ID"))
    cluster_group_id = models.CharField(_("集群组ID"), primary_key=True, max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    created_by = models.CharField("创建者", max_length=128)
    description = models.TextField("描述信息", null=True, blank=True)

    class Meta:
        db_table = "raw_data_cluster_group_config"
        app_label = "basic"
        managed = False


class AuthResourceGroupRecord(models.Model):
    """
    资源组授权记录
    """

    subject_type = models.CharField(_("授权主体类型"), max_length=256)
    subject_id = models.CharField(_("授权主体"), max_length=256)
    resource_group_id = models.CharField(_("资源组ID"), max_length=256)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    created_by = models.CharField("创建者", max_length=128)
    description = models.TextField("描述信息", null=True, blank=True)

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_resource_group_record"


class AuthDatasetSensitivity(models.Model):
    data_set_type = models.CharField(_("数据集类型"), max_length=64)
    data_set_id = models.CharField(_("数据集ID"), max_length=256)
    sensitivity = models.CharField(_("敏感度"), max_length=64)
    tag_method = models.CharField(_("标记方式"), max_length=64, default="user")

    created_at = models.DateTimeField("创建时间", auto_now=True)
    created_by = models.CharField("创建者", max_length=128)

    @classmethod
    def get_dataset_tag_method(cls, data_set_type, data_set_id):
        """
        获取数据集标记方式
        """
        if data_set_type == "raw_data":
            return "user"

        try:
            obj = cls.objects.get(data_set_type=data_set_type, data_set_id=data_set_id)
            return obj.tag_method
        except AuthDatasetSensitivity.DoesNotExist:
            return "default"

    @classmethod
    def tagged(cls, data_set_type, data_set_id, sensitivity, bk_username, tag_method="user"):
        """
        用户添加敏感度标记
        """
        cls.objects.update_or_create(
            data_set_type=data_set_type,
            data_set_id=data_set_id,
            tag_method=tag_method,
            defaults={"sensitivity": sensitivity, "created_by": bk_username},
        )

    @classmethod
    def untagged(cls, data_set_type, data_set_id):
        """
        用户移除敏感度标记
        """
        cls.objects.filter(data_set_type=data_set_type, data_set_id=data_set_id).delete()

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_dataset_sensitivity"


meta_sync_register(ProjectData)
meta_sync_register(ProjectRawData)
