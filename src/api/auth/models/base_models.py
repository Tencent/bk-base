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
import os

import attr
import yaml
from cached_property import cached_property
from django.conf import settings

from auth.exceptions import CoreBaseModelInstanceNotExist, CoreBaseModelNoPK
from common.bklanguage import BkLanguage
from common.log import logger


class LocalStorage:
    def __init__(self):
        self._storage = {"objects": [], "actions": [], "action_relations": [], "roles": [], "policies": []}

    def get(self, key):
        return self._storage[key]

    def load(self):
        run_version = settings.RUN_VERSION
        run_mode = settings.RUN_MODE.lower()

        files = [
            "auth/config/core/action.yaml",
            "auth/config/core/object.yaml",
            "auth/config/core/role.yaml",
            "auth/config/core/env_{}_{}.yaml".format(run_version, run_mode),
        ]

        for f in files:
            try:
                path = os.path.join(settings.AUHT_PROJECT_DIR, f)
                if not os.path.exists(path):
                    logger.info(f"[SystemInit]CoreConfigure file({path}) not exists.")
                    continue

                with open(path, "r", encoding="utf8") as f:
                    content = yaml.load(f.read(), Loader=yaml.FullLoader)
                    if content is None:
                        logger.info(f"[SystemInit] CoreConfigure file({path}) is empty")
                        continue

                    for k, v in list(content.items()):
                        if k not in self._storage:
                            self._storage[k] = []

                        self._storage[k].extend(v)

                logger.info(f"[SystemInit] Succeed to load CoreConfigure file({path})")
            except Exception as err:
                logger.exception(f"[SystemInit] Fail to load core yaml file {f}, {err}")


core_storage = LocalStorage()
core_storage.load()


class Manager:
    """
    管理器，从 LocalStorage 获取配置信息
    """

    def __init__(self, storage, storage_key):
        self.storage_key = storage_key
        self.storage = storage

        self.model = None

    def get(self, **kwargs):
        """
        获取指定实例
        """
        instances = self.filter(**kwargs)
        if len(instances) == 0:
            raise CoreBaseModelInstanceNotExist(f"{self.model}({kwargs}) instacne not exist")

        return instances[0]

    def filter(self, **kwargs):
        """
        支持实例过滤查询

        @paramExample kwargs 基本属性匹配
            {
                'object_class_id': 'project'
            }
        @paramExample 模型属性匹配
            {
                'object_class': 'project'
            }
        """
        kwargs = self.clean_kwargs(kwargs)
        return [inst for inst in self.instances if all([getattr(inst, k) == v for k, v in list(kwargs.items())])]

    def create(self, **kwargs):
        """
        对现有在内存中的模型配置进行新增，正式环境不允许使用这个方法，目前仅在单元测试中使用
        """
        instance = self.build_instance(kwargs)
        self.instances.append(instance)

    def clean_kwargs(self, kwargs):
        """
        将支持的过滤参数进行装换，装换模型属性

        @paramExample
            {
                'pk': 'project.manage',
                'object_class': 'project',
                'has_instance': True
            }

        @returnExample
            {
                'action_id': 'project.manage',
                'object_class_id': 'project',
                'has_instance': True
            }
        """
        fields_mapping = {f.name: f for f in attr.fields(self.model)}

        for k, v in list(kwargs.items()):

            # 将形如 pk 装换为具体主键字段
            if k == "pk":
                primary_key = self.model.Meta.primary_key
                if primary_key is None:
                    raise CoreBaseModelNoPK(f"{self.model} model has no PK configugre")

                kwargs.pop(k)
                kwargs[primary_key] = v
                continue

            f = fields_mapping[k]

            # 将形如 object_class=project 转换为 object_class_id=project
            if issubclass(f.type, BaseModel) and not isinstance(v, f.type):
                kwargs.pop(k)
                kwargs[f"{k}_id"] = v

        return kwargs

    def all(self):
        """
        获取全部实例
        """
        return self.instances

    def values(self):
        """
        返回字典列表
        """
        return [attr.asdict(inst) for inst in self.instances]

    @cached_property
    def instances(self):
        """
        获取所有实例
        """
        instances = self.storage.get(self.storage_key)
        return [self.build_instance(inst) for inst in instances]

    def build_instance(self, instance):
        """
        根据模型定义，建立实例
        """
        cleaned_instance = dict()

        for f in attr.fields(self.model):
            if f.name in instance:
                if issubclass(f.type, BaseModel):
                    try:
                        cleaned_instance[f.name] = f.type.objects.get(pk=instance[f.name])
                    except CoreBaseModelInstanceNotExist:
                        # 不存在部分外键不存在的取值，比如 *
                        cleaned_instance[f.name] = instance[f.name]
                else:
                    cleaned_instance[f.name] = instance[f.name]

        target = self.model(**cleaned_instance)
        setattr(target, "raw_content", instance)
        return target

    def contribute_to_class(self, model):
        self.model = model


class MetaModel(type):
    """
    Metaclass for all models.
    """

    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        if hasattr(new_class, "objects"):
            new_class.objects.contribute_to_class(new_class)
        return new_class


class BaseModel(metaclass=MetaModel):
    @property
    def pk(self):
        return getattr(self, self.Meta.primary_key)

    def __attrs_post_init__(self):
        for f in attr.fields(type(self)):
            if issubclass(f.type, BaseModel):
                model_id_key = f"{f.name}_id"
                model_value = getattr(self, f.name)

                if isinstance(model_value, BaseModel):
                    setattr(self, model_id_key, model_value.pk)
                else:
                    setattr(self, model_id_key, model_value)

    def __getattribute__(self, name):
        """
        拦截 BaseModel 的属性，进行国际化翻译
        """
        if name in ["object_name", "role_name", "description", "action_name"]:
            try:
                raw_content = object.__getattribute__(self, "raw_content")
            except AttributeError:
                raw_content = dict()

            lang = BkLanguage.current_language()
            lang_name = f"{name}_{lang}"
            if lang_name in raw_content:
                return raw_content[lang_name]

        return object.__getattribute__(self, name)


@attr.s
class ObjectConfig(BaseModel):
    object_class = attr.ib(type=str)
    object_name = attr.ib(type=str)
    has_object = attr.ib(type=bool)
    scope_id_key = attr.ib(type=str)
    scope_name_key = attr.ib(type=str)
    user_mode = attr.ib(type=bool)
    to_iam = attr.ib(type=bool)
    description = attr.ib(type=str, default="")

    objects = Manager(core_storage, "objects")

    @classmethod
    def get_object_scope_key_map(cls):
        """
        权限资源对象——键 映射
        @return:
        {
            'object_class': 'scope_id_key',
            'biz': 'bk_biz_id',
            'bkdata': '*',
        }
        """
        _all = cls.objects.all()
        return {
            _object.object_class: {"scope_id_key": _object.scope_id_key, "scope_name_key": _object.scope_name_key}
            for _object in _all
        }

    @classmethod
    def get_object_config_map(cls):
        return {
            _object.object_class: {"object_name": _object.object_name, "description": None}
            for _object in cls.objects.all()
        }

    @classmethod
    def get_objects_by_user_mode(cls, is_user_mode=None):
        """
        返回指定用户模式的查询集
        @param: {Boolean} is_user_mode 是否用户模式
        @return: queryset
        """
        if is_user_mode is None:
            return cls.objects.all()

        return cls.objects.filter(user_mode=is_user_mode)

    @classmethod
    def get_object_scope_action(cls, is_user_mode=None):
        """
        获取对象资源范围动作
        @param: is_user_mode 是否用户模式
        @param: bk_biz_id 使用业务id过滤数据
        @return:
        [
            {
                "object_class": "biz",
                "object_name": "业务",
                "has_object": False,
                "scope_object_classes": [
                    {
                        "scope_id_key": "bk_biz_id",
                        "scope_name_key": "bk_biz_name",
                        "scope_object_name": "业务",
                        "scope_object_class": "biz",

                    }
                ],
                "actions": [
                    {
                        "action_id": "raw_data.create",
                        "action_name": "数据接入",
                        "has_object": False,

                    }
                ]
            },
        ]
        """
        objects = cls.get_objects_by_user_mode(is_user_mode=is_user_mode)
        actions = []
        for object in objects:
            action = ActionConfig.objects.filter(object_class_id=object.object_class)
            for a in action:
                actions.append(a)
        data = []
        for obj_index, _object in enumerate(objects):
            data.append(
                {
                    "object_class": _object.object_class,
                    "object_class_name": _object.object_name,
                    "scope_id_key": _object.scope_id_key,
                    "scope_name_key": _object.scope_name_key,
                    "has_object": _object.has_object,
                    "scope_object_classes": [
                        {
                            "scope_id_key": _object.scope_id_key,
                            "scope_name_key": _object.scope_name_key,
                            "scope_object_name": _object.object_name,
                            "scope_object_class": _object.object_class,
                        }
                    ],
                    "actions": [
                        {
                            "action_id": _action.action_id,
                            "action_name": _action.action_name,
                            "has_instance": _action.has_instance,
                            "can_be_applied": _action.can_be_applied,
                            "scope_object_classes": [
                                {
                                    "scope_id_key": _object.scope_id_key,
                                    "scope_name_key": _object.scope_name_key,
                                    "scope_object_name": _object.object_name,
                                    "scope_object_class": _object.object_class,
                                }
                            ],
                            "order": _action.order,
                        }
                        for _action in actions
                        if _action.object_class.object_class == _object.object_class
                        and (
                            is_user_mode is False
                            or is_user_mode is None
                            or _action.user_mode is True
                            and is_user_mode is True
                        )
                    ],
                    "order": obj_index,
                }
            )
        return data

    class Meta:
        primary_key = "object_class"


@attr.s
class ActionConfig(BaseModel):
    action_id = attr.ib(type=str)
    action_name = attr.ib(type=str)
    action_name_en = attr.ib(type=str)
    object_class = attr.ib(type=ObjectConfig)
    has_instance = attr.ib(type=bool)
    user_mode = attr.ib(type=bool)
    can_be_applied = attr.ib(type=bool)
    order = attr.ib(type=int)
    to_iam = attr.ib(type=bool)
    description = attr.ib(type=str, default="")

    object_class_id = attr.ib(type=str, init=False)

    objects = Manager(core_storage, "actions")

    @classmethod
    def get_action_name_map(cls):
        return {_action.action_id: _action.action_name for _action in cls.objects.all()}

    @classmethod
    def action_relations(cls):
        return ActionRelationConfig.objects.all()

    @classmethod
    def get_parent_action_ids(cls, action_id, parent_action_ids):
        """
        获取父级 Action IDS
        @param {String} action_id 功能
        @param {List<String>} parent_action_ids 父级功能列表
        """

        for relation in cls.action_relations():
            _child = relation.child_id
            _parent = relation.parent_id
            if _child == action_id:
                if _parent not in parent_action_ids:
                    parent_action_ids.append(_parent)
                    cls.get_parent_action_ids(_parent, parent_action_ids)

    class Meta:
        primary_key = "action_id"


@attr.s
class ActionRelationConfig(BaseModel):
    parent = attr.ib(type=ActionConfig)
    child = attr.ib(type=ActionConfig)

    parent_id = attr.ib(type=str, init=False)
    child_id = attr.ib(type=str, init=False)

    objects = Manager(core_storage, "action_relations")

    class Meta:
        primary_key = None


@attr.s
class RoleConfig(BaseModel):
    """
    权限角色配置表
    """

    role_id = attr.ib(type=str)
    role_name = attr.ib(type=str)
    object_class = attr.ib(type=ObjectConfig)
    description = attr.ib(type=str, default="")
    allow_empty_member = attr.ib(type=bool, default=True)
    user_mode = attr.ib(type=bool, default=False)
    order = attr.ib(type=int, default=1)
    to_iam = attr.ib(type=bool, default=False)

    object_class_id = attr.ib(type=str, init=False)

    objects = Manager(core_storage, "roles")

    @classmethod
    def get_role_config_map(cls):
        """角色id及角色名称映射关系"""
        return {
            _role.role_id: {
                "role_name": _role.role_name,
                "description": _role.description,
                "user_mode": _role.user_mode,
            }
            for _role in cls.objects.all()
        }

    @classmethod
    def get_role_object_map(cls):
        """
        角色对应资源对象映射表
        @return:
        """
        object_name_map = ObjectConfig.get_object_config_map()
        return {
            _role.role_id: {
                "object_class": _role.object_class_id,
                "object_name": object_name_map[_role.object_class_id]["object_name"],
            }
            for _role in cls.objects.all()
        }

    @classmethod
    def get_object_roles_map(cls):
        """
        资源对象角色映射表
        @return: [project.manager, project.flow_member]
        """
        all_role = cls.objects.all()
        object_role_map = {}
        for _role in all_role:
            object_class = _role.object_class_id
            role_id = _role.role_id
            if object_class not in object_role_map:
                object_role_map[object_class] = [role_id]
            else:
                object_role_map[object_class].append(role_id)
        return object_role_map

    @staticmethod
    def list_related_roles(role_id, role_object_map, object_role_map):
        """
        查询关联的角色，为避免在循环中使用此方法时重复查询映射关系,
        请提前将role_object_map和object_role_map查出
        @param role_id: project.manager
        @param role_object_map: 见get_role_object_map
        @param object_role_map: 见get_object_roles_map
        @return: [project.manager, project.flow_member]
        """
        object_class = role_object_map[role_id]["object_class"]
        return object_role_map.get(object_class, [])

    class Meta:
        primary_key = "role_id"


@attr.s
class RolePolicyInfo(BaseModel):
    """
    角色权限策略信息
    """

    role_id = attr.ib(type=RoleConfig)
    action_id = attr.ib(type=ActionConfig)
    object_class = attr.ib(type=ObjectConfig)

    scope_attr_key = attr.ib(type=str, default="")
    scope_attr_value = attr.ib(type=str, default="")

    role_id_id = attr.ib(type=str, init=False)
    action_id_id = attr.ib(type=str, init=False)
    object_class_id = attr.ib(type=str, init=False)

    objects = Manager(core_storage, "policies")

    @classmethod
    def get_role_action_map(cls):
        """
        角色 动作 映射表
        @return:
        """
        data = {}
        action_configs = ActionConfig.objects.all()
        for info in cls.objects.all():
            role_id = info.role_id_id
            action_id = info.action_id_id
            if info.object_class_id == "*":
                action_ids = [action.action_id for action in action_configs]
            elif action_id == "*":
                action_ids = [
                    action.action_id for action in action_configs if info.object_class_id == action.object_class_id
                ]
            else:
                action_ids = [action_id]

            if role_id not in data:
                data[role_id] = []

            data[role_id].extend(action_ids)
        return data

    class Meta:
        primary_key = None
