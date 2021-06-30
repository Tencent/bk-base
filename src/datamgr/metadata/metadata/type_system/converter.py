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
"""
各类至metadata实例转换器
"""


from abc import abstractmethod
from copy import deepcopy
from datetime import datetime

import arrow
import attr
import dateutil
from cached_property import cached_property

from metadata.exc import MetaDataTypeConvertError
from metadata.runtime import rt_context
from metadata.type_system.basic_type import Internal
from metadata.type_system.core import ReferredMetaData
from metadata.util.common import Empty, StrictABCMeta
from metadata.util.i18n import lazy_selfish as _


class InstanceConverter(object, metaclass=StrictABCMeta):
    """
    元数据变更信息->元数据实例 转换器基类。输入input,输出output。
    """

    __abstract__ = True
    attr_converters = []

    def __init__(self, input_, type_, partial=False):
        """
        :param input_: 输入
        :param type_: 元数据类型/建模
        :param partial: 是否只转换部分属性
        """
        self.attr_converters = deepcopy(self.attr_converters)
        self.input = input_
        self.type = type_
        self.partial = partial

    @abstractmethod
    def output(self):
        """
        转换后的元数据实例
        :return:
        """
        pass

    def convert_attr(self, value, attr_def):
        """
        选择合适的属性转换器，转换实例上的属性值。
        :param value: 属性值
        :param attr_def: 属性定义
        :return: 转换后的属性值
        """
        for attr_converter_cls in self.attr_converters:
            conv = attr_converter_cls(value, attr_def, self.input, self.type)
            if conv.in_scope:
                return conv.output
        else:
            raise MetaDataTypeConvertError(_('No available attr converter.'))


class AttrConverter(object, metaclass=StrictABCMeta):
    """
    元数据变更属性值->元数据实例属性值 转换器基类。输入input,输出output。
    """

    __abstract__ = True

    def __init__(self, input_, attr_def, instance, output_instance_type):
        """
        :param input_: 输入属性值
        :param attr_def: 元数据类型的属性定义
        :param instance: 元数据变更信息实例
        :param output_instance_type: 转换后的元数据类型
        """
        self.input = input_
        self.attr_def = attr_def
        self.instance = instance
        self.output_instance_type = output_instance_type
        self.normal_conf = rt_context.config_collection.normal_config

    @abstractmethod
    def output(self):
        """
        转换后的属性值
        :return:
        """
        pass

    @abstractmethod
    def in_scope(self):
        """
        指示当前属性是否适合此转换器转换
        :return:
        """
        pass


class CommonAttrConverter(AttrConverter):
    @cached_property
    def output(self):
        if self.input:
            if not isinstance(self.input, self.attr_def.type):
                if self.attr_def.type is datetime:
                    return (
                        arrow.get(self.input).replace(tzinfo=dateutil.tz.gettz(self.normal_conf.TIME_ZONE)).isoformat()
                    )
                return self.attr_def.type(self.input)
            else:
                return self.input
        else:
            return self.input

    @cached_property
    def in_scope(self):
        return True


class CustomCommonAttrConverter(CommonAttrConverter):
    rules = {}
    __abstract__ = True

    @cached_property
    def output(self):
        if self.input:
            return self.rules[self.attr_def.name]()
        else:
            return self.input

    @cached_property
    def in_scope(self):
        if self.attr_def.name in self.rules:
            return True
        else:
            return False


class ReferredAttrConverter(AttrConverter):
    def __init__(self, *args, **kwargs):
        super(ReferredAttrConverter, self).__init__(*args, **kwargs)

    @cached_property
    def output(self):
        if self.input:
            actual_type = self.attr_def.type
            actual_value = self.input
            if actual_value is Empty:
                return Empty
            if (not actual_value) and (actual_value != 0):
                return Internal(role='as_none')
            identifier_attr = actual_type.metadata['identifier']
            xid_type = identifier_attr.type
            if not isinstance(actual_value, xid_type):
                actual_value = xid_type(actual_value)
            return actual_type(**{identifier_attr.name: actual_value})
        else:
            return self.input

    @cached_property
    def in_scope(self):
        if issubclass(self.attr_def.type, ReferredMetaData):
            return True
        else:
            return False


class CustomReferredAttrConverter(ReferredAttrConverter):
    rules = {}
    __abstract__ = True

    @cached_property
    def output(self):
        actual_type, actual_value = self.rules[self.attr_def.name]()
        if actual_value is Empty:
            return Empty
        if (not actual_value) and (actual_value != 0):
            return Internal(role='as_none')
        identifier_attr = actual_type.metadata['identifier']
        xid_type = identifier_attr.type
        if not isinstance(actual_value, xid_type):
            actual_value = xid_type(actual_value)
        return actual_type(**{identifier_attr.name: actual_value})

    @cached_property
    def in_scope(self):
        if self.attr_def.name in self.rules and issubclass(self.attr_def.type, ReferredMetaData):
            return True
        else:
            return False


class ReferredAttrFromIdAttrConverter(CustomReferredAttrConverter):
    def __init__(self, *args, **kwargs):
        super(ReferredAttrFromIdAttrConverter, self).__init__(*args, **kwargs)
        self.rules = deepcopy(self.rules)
        for post_fix in ('_obj', '_person'):
            if self.attr_def.name.endswith(post_fix):
                if hasattr(self.instance, self.attr_def.name.split(post_fix)[0]):
                    self.rules[self.attr_def.name] = self.from_id
        else:
            if (
                hasattr(self.instance, self.attr_def.name + '_id')
                or hasattr(self.instance, self.attr_def.name + '_code')
                or hasattr(self.instance, self.attr_def.name + '_config_id')
            ):
                self.rules[self.attr_def.name] = self.from_id

    def from_id(self):
        obj_id = Empty
        for postfix in (
            '_obj',
            '_person',
        ):
            if self.attr_def.name.endswith(postfix):
                obj_id = getattr(self.instance, self.attr_def.name.split(postfix)[0], Empty)
        else:
            for postfix in ('_code', '_id', '_config_id'):
                if hasattr(self.instance, self.attr_def.name + postfix):
                    obj_id = getattr(self.instance, self.attr_def.name + postfix, Empty)
        if obj_id is not Empty:
            return self.attr_def.type, obj_id
        else:
            raise MetaDataTypeConvertError(
                _('Fail to access target info from md type {} with id {}'.format(self.attr_def.type, obj_id))
            )


class CommonInstanceConverter(InstanceConverter):
    attr_converters = [ReferredAttrFromIdAttrConverter, ReferredAttrConverter, CommonAttrConverter]

    def __init__(self, *args, **kwargs):
        super(CommonInstanceConverter, self).__init__(*args, **kwargs)
        if getattr(rt_context, 'skip_auto_gen_reffed_attr', False):
            from metadata_biz.type_system.converters import AutoGenMDReffedAttrConverter

            self.attr_converters = deepcopy(self.attr_converters)
            self.attr_converters.remove(AutoGenMDReffedAttrConverter)

    @cached_property
    def output(self):
        attrs_values = {}
        for attr_def in attr.fields(self.type):
            value = getattr(self.input, attr_def.name, Empty)
            c_v = self.convert_attr(value, attr_def)
            if c_v is not Empty:
                attrs_values[attr_def.name] = c_v
        if not self.partial:
            return self.type(**attrs_values)
        else:
            return self.type.referred_cls(**attrs_values)
