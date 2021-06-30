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

import inspect
from collections import OrderedDict
from copy import deepcopy
from typing import List

import attr
from attrdict import AttrDict

from metadata.exc import MetaDataTypeError
from metadata.util.common import Empty, StrictABCMeta
from metadata.util.i18n import lazy_selfish as _

# 元数据基础数据结构


class MetaPrototype(object, metaclass=StrictABCMeta):
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        print('init MetaPrototype')
        super(MetaPrototype, self).__init__()


@attr.s
class MetaData(MetaPrototype, metaclass=StrictABCMeta):
    """
    元数据基类
    """

    __abstract__ = True

    metadata = {}
    referred_cls = None

    @classmethod
    def convert(
        cls,
        changed_info,
        converter=None,
        partial=False,
    ):
        if not converter:
            from metadata.type_system.converter import CommonInstanceConverter

            converter = CommonInstanceConverter
        converter(AttrDict(**changed_info), cls, partial)
        return converter.output


@attr.s
class ReferredMetaData(MetaData, metaclass=StrictABCMeta):
    """
    每个元数据类型对应的Ref类
    """

    __abstract__ = True
    agent = None


@attr.s
class Facets(object):
    """
    仅支持基础类型数值，功能较弱，目前未使用。
    """

    pass


# 仅用于类型系统的数据结构


class StringDeclaredType(object, metaclass=StrictABCMeta):
    """
    使用字符串声明类型。适用于循环引用等情况，元数据声明时，会自动从已注册的存储里搜索类型。
    """

    __abstract__ = True
    type_name = None

    @classmethod
    def typed(cls, type_name):
        return type(
            str(cls.__name__ + type_name),
            (cls,),
            {str('type_name'): type_name},
        )


default_metadata_type_registry = {}


def as_metadata(maybe_cls, registry=None, **kwargs):
    """
    注册至元数据系统，并初始化。
    :param maybe_cls: 用于支持装饰器传参数
    :param registry: 元数据存储，用于存储注册的类型
    :param kwargs: 其他attr.ib参数
    :return:
    :Todo: 提前搜集注册元数据事件监听的模型和字段列表
    """
    if registry is None:
        registry = default_metadata_type_registry

    def wrap(cls):
        if not issubclass(cls, MetaData):
            raise MetaDataTypeError(cls_name=cls)
        # 注册元数据类型
        registry[cls.__name__] = cls
        # 执行通用初始化工作
        if not hasattr(cls, str('__attrs_attrs__')):
            cls = attr.s(cls, **kwargs)
        if str('metadata') not in vars(cls):
            cls.metadata = {'reverse_referenced': set()}
        else:
            cls.metadata['reverse_referenced'] = set()
        # 非抽象类型进行元数据初始化工作
        if not getattr(cls, '__abstract__', False):
            cls.__extend_attrs__ = OrderedDict()
            for attr_def in attr.fields(cls):
                if attr_def.metadata.get('identifier', False):
                    identifier_attr_name = attr_def.name
                    break
            else:
                raise MetaDataTypeError(_('No identifier attr in this metadata type.'))
            # 创建Ref类型
            referred_cls_name = str('Referred') + cls.__name__
            referred_cls = type(referred_cls_name, (ReferredMetaData,), {str('metadata'): {}})
            referred_cls.agent = cls
            cls.referred_cls = referred_cls

            # 执行注册回调
            register_callbacks = getattr(cls, '__metadata__', {}).get('register_callbacks', [])
            if register_callbacks:
                for func in register_callbacks:
                    func(cls, registry)

            # 刷新元数据类型：重建attr.ib定义，然后重新声明数据结构
            rebuilt_attr_ibs = {
                attr_def.name: attr.ib(**_get_attr_ib_rebuild_kwargs(cls, attr_def, registry))
                for attr_def in attr.fields(cls)
            }
            extend_attrs = cls.metadata.get('extend_attrs', {})
            if extend_attrs:
                add_extend_attr_ibs(extend_attrs, cls, rebuilt_attr_ibs, registry)
            delattr(cls, '__attrs_attrs__')
            for name, ib in list(rebuilt_attr_ibs.items()):
                setattr(cls, name, ib)
            cls.typed = attr.attrib(type=str, default='')
            attr.s(cls, **kwargs)
            cls.metadata['identifier'] = attr.fields_dict(cls)[identifier_attr_name]
            maintain_reverse_attr_defs(cls)
            cls.metadata['converter'] = getattr(cls, '__converter__', None)
            # 刷新ref类型，在ref类型上拷贝原对象attr定义，但均有Empty默认值。去除validate。
            rebuilt_reffed_attr_ibs = {}
            for attr_def in attr.fields(cls):
                ib_kwargs = _get_attr_ib_rebuild_kwargs(cls, attr_def, registry)
                ib_kwargs['default'] = Empty
                ib_kwargs.pop('validator', None)
                rebuilt_reffed_attr_ibs[attr_def.name] = attr.ib(**ib_kwargs)
            for name, ib in list(rebuilt_reffed_attr_ibs.items()):
                setattr(referred_cls, name, ib)
            attr.s(referred_cls, **kwargs)
            referred_cls.metadata['identifier'] = attr.fields_dict(referred_cls)[identifier_attr_name]

        # 可用于定义的抽象类型进行元数据初始化工作
        if vars(cls).get('__definable__', False):
            referred_cls_name = str('Referred') + cls.__name__
            referred_cls = type(referred_cls_name, (ReferredMetaData,), {})
            referred_cls.agent = cls
            cls.referred_cls = referred_cls

        return cls

    if maybe_cls:
        return wrap(maybe_cls)
    else:
        return wrap


def add_extend_attr_ibs(extend_dct, cls, rebuilt_dct, registry):
    for k, v in extend_dct.items():
        v = deepcopy(v)
        v['type'] = registry[v.pop('type_name')].referred_cls if 'type_name' in v else v['type']
        if 'metadata' not in v:
            v['metadata'] = {}
        v = dgraph_common_predicate_attr_kwargs(cls, k, v)
        rebuilt_dct[k] = attr.ib(**v)


def maintain_reverse_attr_defs(cls):
    """
    在被引用的类型上，添加反向映射信息。
    :param cls:
    :return:
    """
    for attr_def in attr.fields(cls):
        is_list, ib_primitive_type = parse_list_type(attr_def.type)
        if issubclass(ib_primitive_type, MetaData):
            ib_primitive_type.agent.metadata['reverse_referenced'].add((cls, attr_def))


def _get_attr_ib_rebuild_kwargs(cls, attribute_instance, registry):
    """
    生成重建元数据attr_def的关键词参数, 替换字符指代类型，等等。
    :param attribute_instance: attr_def实例
    :param registry: 元数据注册区
    :return: 变更后的关键词参数
    """
    ib_arg_names = set(dir(attribute_instance)) & set(inspect.getargspec(attr.ib).args)
    if str('convert') in ib_arg_names:
        ib_arg_names.remove(str('convert'))
    ib_kwargs = {k: getattr(attribute_instance, k) for k in ib_arg_names}
    ib_type = ib_kwargs['type']
    is_list, ib_primitive_type = parse_list_type(ib_type)
    if issubclass(ib_primitive_type, StringDeclaredType):
        attr_actual_type = registry.get(ib_primitive_type.type_name)
        if not attr_actual_type:
            raise MetaDataTypeError(_('No actual type for the {}.'.format(ib_primitive_type)))
        ib_primitive_type = attr_actual_type
    if issubclass(ib_primitive_type, MetaData) and not issubclass(ib_primitive_type, ReferredMetaData):
        ib_primitive_type = ib_primitive_type.referred_cls
    if is_list and not issubclass(ib_primitive_type, List):
        ib_kwargs['type'] = List[ib_primitive_type]
    else:
        ib_kwargs['type'] = ib_primitive_type
    ib_kwargs = dgraph_common_predicate_attr_kwargs(cls, attribute_instance.name, ib_kwargs)
    return ib_kwargs


def dgraph_common_predicate_attr_kwargs(cls, attr_name, ib_kwargs):
    if getattr(cls, '__metadata__', {}):
        if attr_name in cls.__metadata__.get('dgraph', {}).get('common_predicates', {}):
            dgraph_info = cls.__metadata__['dgraph']['common_predicates'][attr_name]
            ib_metadata = dict(ib_kwargs['metadata'])
            ib_metadata['dgraph'] = dgraph_info['dgraph']
            ib_kwargs['metadata'] = ib_metadata
    return ib_kwargs


def parse_list_type(ib_type):
    """
    处理列表型参数类型
    :param ib_type: 参数类型
    :return: 是否为列表型，原子类型
    """
    if issubclass(ib_type, List):
        is_list = True
        ib_primitive_type = ib_type.__args__[0]
    else:
        is_list = False
        ib_primitive_type = ib_type
    return is_list, ib_primitive_type


def gen_instance_type(instance):
    """
    获取ReferredMetaData和普通MetaData的真实类型
    :param instance: metadata实例
    :return:
    """
    return instance.agent if isinstance(instance, ReferredMetaData) else instance.__class__


def get_all_primitive_attr_defs(md_type):
    return {
        attr_def.name: attr_def
        for attr_def in attr.fields(md_type)
        if not issubclass(parse_list_type(attr_def.type)[1], MetaData)
    }
