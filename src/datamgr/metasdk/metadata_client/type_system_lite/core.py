# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有


from __future__ import absolute_import, print_function, unicode_literals

# 元数据基础数据结构
import inspect

import attr

from metadata_client.exc import LocalMetaDataTypeError
from metadata_client.util.common import Empty, StrictABCMeta


@attr.s
class LocalMetaData(object):
    """
    元数据基类
    """

    __metaclass__ = StrictABCMeta
    __abstract__ = True

    metadata = {}
    referred_cls = None


default_metadata_type_registry = {}


def as_local_metadata(maybe_cls, registry=None, **kwargs):
    if registry is None:
        registry = default_metadata_type_registry

    def wrap(cls):
        if not issubclass(cls, LocalMetaData):
            raise LocalMetaDataTypeError(message_kv={"cls_name": cls.__name__})
        # 注册元数据类型
        registry[cls.__name__] = cls
        # 执行通用初始化工作
        cls = attr.s(cls, **kwargs)
        if str("metadata") not in vars(cls):
            cls.metadata = {}
        for attr_def in attr.fields(cls):
            if attr_def.metadata.get("identifier", False):
                identifier_attr_name = attr_def.name
                break
        else:
            raise LocalMetaDataTypeError("No identifier attr in this metadata type.")

        local_cls = type(cls.__name__, (LocalMetaData,), {str("metadata"): {}})
        rebuilt_local_attr_ibs = {}
        for attr_def in attr.fields(cls):
            ib_kwargs = _get_local_attr_ib_rebuild_kwargs(attr_def)
            rebuilt_local_attr_ibs[attr_def.name] = attr.ib(**ib_kwargs)
        for name, ib in rebuilt_local_attr_ibs.items():
            setattr(local_cls, name, ib)
        attr.s(local_cls, **kwargs)
        local_cls.metadata["identifier"] = attr.fields_dict(local_cls)[identifier_attr_name]

        return local_cls

    if maybe_cls:
        return wrap(maybe_cls)
    else:
        return wrap


def _get_local_attr_ib_rebuild_kwargs(attribute_instance):
    ib_arg_names = set(dir(attribute_instance)) & set(inspect.getargspec(attr.ib).args)
    if str("convert") in ib_arg_names:
        ib_arg_names.remove(str("convert"))
    ib_kwargs = {k: getattr(attribute_instance, k) for k in ib_arg_names}
    ib_kwargs["default"] = Empty
    ib_kwargs.pop("validator", None)
    ib_kwargs.pop("cmp", None)
    return ib_kwargs
