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
事件注册机
"""


import enum
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from uuid import uuid4

import attr
import cattr

from metadata.util.common import StrictABCMeta

module_logger = logging.getLogger(__name__)
python_string_type = str
registered_event_list = defaultdict(dict)


def register_event(cls):
    """
    注册事件装饰器
    :param cls: string 新类
    :return:
    """
    if not getattr(cls, '__abstract__', False):
        registered_event_list[cls._event_type][cls.__name__] = cls
    return cls


def get_registered_target():
    """
    获取注册事件监听的模型名称列表
    :return: list 已注册事件监听的模型名称列表
    :Todo: 暂时写死，后续从default_registry中获取
    """
    return ['AccessRawData', 'ResultTable', 'ResultTableField', 'DataflowNodeInfo', 'DatabusMigrateTask']


class EventType(enum.Enum):
    """
    事件类型
    """

    # 元数据变更事件
    MUTATION = 'mutation'
    # 错误事件
    EXCEPTION = 'exception'


class EventLevel(enum.Enum):
    """
    事件等级
    """

    # 至关重要的，不允许缺失，或可引起环境报错、崩溃等危险的消息
    CRUCIAL = 'crucial'
    # 普通的，通常不允许确实，或可带来有限影响，但不涉及主要流程
    NORMAL = 'normal'
    # 可供参考的，可用于开发自定义附加功能，缺失不会带来明确影响
    REFERENTIAL = 'referential'


class EventMutationListeningType(enum.Enum):
    """
    元数据变更事件监听类型(节点粒度)
    """

    ON_CREATE = 'on_create'
    ON_DELETE = 'on_delete'
    ON_UPDATE = 'on_update'


class EventPrototype(object, metaclass=StrictABCMeta):
    """
    事件基类
    """

    __abstract__ = True

    def __init__(self, event_info):
        self.event_name = None
        self.event_type = None
        self.event_level = None
        self.event_capture_listening_type = None
        self.event_info = event_info
        self.logger = module_logger

    def gen_message(self):
        """
        生成事件消息

        :return: string 消息文本
        """

        msg_id = str(uuid4())
        self.event_info['event_type'] = self.event_type
        self.event_info['event_name'] = self.event_name
        self.event_info['event_capture_listening_type'] = self.event_capture_listening_type
        event_message = EventMessage(
            msg_id=msg_id,
            event_type=self.event_type,
            event_level=self.event_level,
            triggered_by=self.event_info.get('triggered_by', ''),
            triggered_at=self.event_info.get('triggered_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            event_message=json.dumps(self.event_info),
        )
        return json.dumps(cattr.unstructure(event_message))


class EventMutation(EventPrototype):
    """
    <元数据变更>类型事件
    """

    __abstract__ = True
    _namespace = 'meta.event'
    _event_type = EventType.MUTATION.value

    def __init__(self, event_info):
        super(EventMutation, self).__init__(event_info)
        self.namespace = self._namespace
        self.event_type = self._event_type
        self.event_level = EventLevel.NORMAL.value


@register_event
class EventCreateNode(EventMutation):
    """
    create_node 事件
    """

    _listening_type = EventMutationListeningType.ON_CREATE

    def __init__(self, event_info):
        super(EventCreateNode, self).__init__(event_info)
        self.event_name = self.__class__.__name__
        self.event_level = EventLevel.NORMAL.value
        self.event_capture_listening_type = self._listening_type.value

    @classmethod
    def in_scope(cls, scope_dict):
        capture_type_list = scope_dict.get(str('capture_types'), [])
        if capture_type_list and cls._listening_type.value in capture_type_list:
            method = scope_dict.get('method', '')
            is_identifier = scope_dict.get('is_identifier', False)
            if is_identifier and method.endswith('CREATE'):
                return True
        return False


@register_event
class EventDeleteNode(EventMutation):
    """
    delete_node 事件
    """

    _listening_type = EventMutationListeningType.ON_DELETE

    def __init__(self, event_info):
        super(EventDeleteNode, self).__init__(event_info)
        self.event_name = self.__class__.__name__
        self.event_level = EventLevel.CRUCIAL.value
        self.event_capture_listening_type = self._listening_type.value

    @classmethod
    def in_scope(cls, scope_dict):
        capture_type_list = scope_dict.get(str('capture_types'), [])
        if capture_type_list and cls._listening_type.value in capture_type_list:
            method = scope_dict.get('method', '')
            is_identifier = scope_dict.get('is_identifier', False)
            if is_identifier and method.endswith('DELETE'):
                return True
        return False


@register_event
class EventUpdateAttr(EventMutation):
    """
    update_attr 事件
    """

    _listening_type = EventMutationListeningType.ON_UPDATE

    def __init__(self, event_info):
        super(EventUpdateAttr, self).__init__(event_info)
        self.event_name = self.__class__.__name__
        self.event_level = EventLevel.CRUCIAL.value
        self.event_capture_listening_type = self._listening_type.value

    @classmethod
    def in_scope(cls, scope_dict):
        capture_type_list = scope_dict.get(str('capture_types'), [])
        if capture_type_list and cls._listening_type.value in capture_type_list:
            method = scope_dict.get('method', '')
            is_identifier = scope_dict.get('is_identifier', False)
            # is_foreignkey = scope_dict.get('is_foreignkey', False)
            if not is_identifier and (method.endswith('UPDATE') or method.endswith('CREATE')):
                return True
        return False


@attr.s
class EventMessage(object):
    """
    事件消息实例
    """

    msg_id = attr.ib(type=python_string_type)  # 事件唯一id
    event_type = attr.ib(type=python_string_type)  # 事件类型
    event_level = attr.ib(type=python_string_type)  # 事件等级
    triggered_by = attr.ib(type=python_string_type)  # 事件触发者/系统
    triggered_at = attr.ib(type=python_string_type)  # 事件触发时间
    created_at = attr.ib(type=python_string_type, default=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # 事件记录时间
    refer = attr.ib(type=python_string_type, default='metadata')  # 事件来源模块名
    generate_type = attr.ib(type=python_string_type, default='meta_hook')  # 事件来源类型 第三方上报 or 元数据抓取
    expire = attr.ib(type=int, default=-1)  # 事件过期时间(秒数,从triggered_at开始算)
    description = attr.ib(type=python_string_type, default='')  # 事件描述
    event_message = attr.ib(type=python_string_type, default='{}')  # json 事件消息详情
