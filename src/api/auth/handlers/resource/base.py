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


from abc import ABCMeta

import attr


@attr.s
class AuthResource(metaclass=ABCMeta):
    """
    权限控制模块中的基础资源类型
    """

    resource_type = None
    storage_type = None
    storage_model = None


@attr.s
class AuthMetaAttr:
    """
    权限控制模块中的基础资源属性
    """

    # 主键字段
    identifier = attr.ib(type=bool, default=False)
    # 在元数据 Dgraph 存储中的名称
    dgraph_name = attr.ib(type=str, default="")
    # 在元数据 MySQL 存储中的名称
    mysql_name = attr.ib(type=str, default="")
    # 是否为展示字段
    is_display = attr.ib(type=bool, default=False)
    # 关联 parent 资源，可用于拓扑层级选择
    is_parent = attr.ib(type=bool, default=False)
    # 是否可作为范围限定的属性
    scope_attribute = attr.ib(type=object, default=None)
    # 直接关联属性，一般情况下 is_parent=True，该字段都是有值的
    direct_attr = attr.ib(type=str, default=None)
    # 非直接关联，即不可在从数据表属性中直接获取
    is_indirect = attr.ib(type=bool, default=False)


default_auth_resources_registry = {}


def as_resource(cls):
    if not issubclass(cls, AuthResource):
        raise Exception("AsResource decorator only used for AuthResource class")

    default_auth_resources_registry[cls.resource_type] = cls

    parent_resources_registry = {}
    scope_attrs_registry = {}
    value_attrs_registry = {}

    for attr_inst in attr.fields(cls):
        if attr_inst.metadata["identifier"]:
            identifier_attr = attr_inst
        if attr_inst.metadata["is_display"]:
            display_attr = attr_inst
        if attr_inst.metadata["is_parent"]:
            parent_resources_registry[attr_inst.type.resource_type] = attr_inst
        if not attr_inst.metadata["is_parent"]:
            value_attrs_registry[attr_inst.name] = attr_inst
        if attr_inst.metadata["scope_attribute"] is not None:
            scope_attrs_registry[attr_inst.name] = attr_inst

    # 预处理，快速检索到期望属性
    # 主键属性对象 + 主键属性名称
    setattr(cls, "identifier_attr", identifier_attr)
    setattr(cls, "identifier_attr_name", identifier_attr.name)

    # 展示属性对象
    setattr(cls, "display_attr", display_attr)

    # 父级资源属性注册表
    # 以 RawDataResource 为例，则注册表为 {"biz": RawDataResource.bk_biz}
    setattr(cls, "parent_resources_registry", parent_resources_registry)

    # 普通属性列表
    setattr(cls, "value_attrs_registry", value_attrs_registry)

    # 范围属性注册表
    # 以 RawDataResource 为例，则注册表为 {"sensitivity": RawDataResource.sensitivity,
    #                                   "bk_biz_id": RawDataResource.bk_biz_id}
    setattr(cls, "scope_attrs_registry", scope_attrs_registry)

    return cls


def as_attr(**kwargs):
    if "metadata" in kwargs:
        # attr.ib 属性申明中的 metadata 必须为字典
        kwargs["metadata"] = attr.asdict(kwargs["metadata"], recurse=False)

    attr_inst = attr.ib(**kwargs)
    return attr_inst
