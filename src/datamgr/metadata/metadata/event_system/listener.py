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
监听事件，生产事件消息到消息队列
"""


import json
import logging
from abc import abstractmethod

import attr
import gevent

from metadata.backend.interface import RawBackendType
from metadata.event_system.register import EventType
from metadata.middleware.rabbitmq import ExchangeType, RabbitMQProducer
from metadata.runtime import rt_context
from metadata.type_system.core import MetaData
from metadata.util.common import StrictABCMeta, camel_to_snake
from metadata_biz.types import default_registry

module_logger = logging.getLogger(__name__)
listener_registry = {}
"""
监听者注册表
"""


def listener_registrar(cls):
    """
    监听者注册装饰器

    :param cls: 监听者类
    :return: boolean True
    """
    listener_name = cls.name if cls.name else camel_to_snake(cls.__name__)
    if listener_name not in listener_registry:
        listener_registry[listener_name] = cls
    return cls


def listener_dispatcher(target_listener, message_dict):
    """
    监听者调度器

    :param target_listener: list 目标监听者列表
    :param message_dict: dict 传输内容
    :return: None
    """
    event_processed = False
    try:
        with gevent.Timeout(0.5, exception=False):
            for listener_name in target_listener:
                listener_cls = listener_registry[listener_name] if listener_name in listener_registry else None
                listener = listener_cls(message_dict)
                if listener and listener.followed_events and listener.check_event():
                    listener.publish_event()
                    break
            event_processed = True
    except Exception as le:
        # Todo: 暂时仅告警，后续补上记录和补录的步骤
        module_logger.warning('[Event_system] event dispatch failed: {}'.format(le))
    finally:
        module_logger.info('[Event_system] event process state: {}'.format(event_processed))


class ListenerPrototype(object, metaclass=StrictABCMeta):
    __abstract__ = True
    followed_events = []

    def __init__(self):
        """
        监听者根据实际情况设置关注事件列表
        """
        self.db_conf = rt_context.config_collection.db_config
        self.follow_registered_event()

    @abstractmethod
    def follow_registered_event(self):
        """
        监听者注册关注的事件列表
        :return:
        """
        return

    @abstractmethod
    def check_event(self, *args, **kwargs):
        """
        检查是否满足触发事件的条件
        :param args:
        :param kwargs:
        :return:
        """
        return

    @abstractmethod
    def publish_event(self):
        """
        触发事件发布消息
        :return:
        """
        return


@listener_registrar
class DgraphMutationListener(ListenerPrototype):
    """
    Dgraph元数据变更事件监听者
    """

    name = 'dgraph_mutation_listener'

    def __init__(self, message_dict):
        """
        对<dgraph主集群>的元数据变更进行监听

        :param message_dict: dict 监听内容
        """

        self.logger = module_logger
        self.captured_event = None
        self.message_dict = message_dict
        backend_type = message_dict.get('target', {}).get('type')
        backend_id = message_dict.get('target', {}).get('id')
        master_id = None
        try:
            system_state = getattr(rt_context, str('system_state'), {})
            master_id = system_state.cache['backends_ha_state'][backend_type]['master']['id']
        except Exception as e:
            self.logger.warning('[backends_ha_state is not existed in zk cache] {}'.format(e))
        if backend_type == RawBackendType.DGRAPH.value:
            if backend_id and backend_id == master_id:
                super(DgraphMutationListener, self).__init__()

    @staticmethod
    def get_md_cls(type_name):
        if type_name:
            return default_registry.get(type_name, None)
        return None

    @staticmethod
    def _get_unicode_value(val):
        """
        获取unicode字符返回

        :param val: 输入
        :return: unicode输出
        """
        if not val:
            return ''
        if isinstance(val, str):
            return val
        try:
            ret_val = str(val)
        except Exception as e:
            module_logger.warning('[get unicode val failed] {}'.format(e))
            ret_val = val
        return ret_val

    def standardize_data(self, mutate_record, mutate_result):
        """
        标准化变更前数据和变更后数据

        :param mutate_record: 变更记录
        :param mutate_result: 变更结果
        :return: tuple (变更前数据dict, 变更后数据dict)
        """
        origin_data_ret = mutate_result.get('data', {}).get('queries', {}).get('get_subject_info', [])
        origin_data_ret = origin_data_ret.pop(0) if origin_data_ret else dict()
        origin_data = {
            key.replace('{}.'.format(mutate_record.type_name), ''): self._get_unicode_value(val)
            for key, val in list(origin_data_ret.items())
            if key.startswith(mutate_record.type_name)
        }
        changed_data = json.loads(getattr(mutate_record, 'changed_data', '{}'))
        changed_data = {
            key: self._get_unicode_value(val)
            for key, val in list(changed_data.items())
            if key not in ('created_at', 'updated_at', 'typed')
        }
        return origin_data, changed_data

    def follow_registered_event(self):
        """
        关注元数据变更类型的已注册事件
        :return:
        """
        self.followed_events = rt_context.event_list.get(EventType.MUTATION.value, None)

    def check_event(self, *args, **kwargs):
        """
        检查事件是否触发

        :return: boolean True/False
        """

        dispatch_id = self.message_dict.get('dispatch_id', None)
        mutate_result = self.message_dict.get('result', {})
        mutate_record = self.message_dict.get('record', {})
        md_cls = self.get_md_cls(mutate_record.type_name)
        md_mapping = {attr_def.name: attr_def for attr_def in attr.fields(md_cls)}
        method = mutate_record.method
        identifier_key = md_cls.metadata['identifier'].name if md_cls else None
        identifier_value = mutate_record.identifier_value
        extra = mutate_record.extra_
        origin_data, changed_data = self.standardize_data(mutate_record, mutate_result)
        if identifier_key not in origin_data:
            origin_data[identifier_key] = identifier_value
        differ = {
            key: {'b': origin_data.get(key, None), 'a': changed_data[key]}
            for key in list(changed_data.keys())
            if changed_data[key] != origin_data.get(key, None)
        }
        if identifier_key not in differ:
            differ[identifier_key] = dict()
        event_info = dict(
            dispatch_id=dispatch_id,
            md_name=md_cls.__name__,
            identifier_key=identifier_key,
            identifier_value=identifier_value,
            triggered_by=extra.get('updated_by', ''),
            triggered_at=extra.get('updated_at', ''),
            origin_data=origin_data,
            changed_data=changed_data,
            differ=differ,
        )
        # 主键需要第一个检查
        check_list = [check_key for check_key in list(differ.keys()) if check_key != identifier_key]
        check_list.insert(0, identifier_key)
        for pred_name in check_list:
            attr_def = md_mapping.get(pred_name, None)
            if not attr_def:
                continue
            is_link = True if issubclass(attr_def.type, MetaData) else False
            is_identifier = True if identifier_key == pred_name else False
            capture_types = getattr(attr_def, 'metadata', {}).get('event', [])
            if not capture_types:
                continue
            scope_dict = dict(
                name=attr_def.name,
                method=method,
                capture_types=capture_types,
                is_identifier=is_identifier,
                is_foreignkey=True if is_link and not is_identifier else False,
            )
            # 检查捕获到的事件, 因各事件之间互斥，检测到一个即可
            for event_name, event_cls in list(self.followed_events.items()):
                if event_cls.in_scope(scope_dict):
                    self.captured_event = event_cls(event_info)
                    return True
        return False

    def publish_event(self):
        """
        发布事件

        :return: None
        """

        event_message = self.captured_event.gen_message()
        producer = RabbitMQProducer(
            conn_addr=self.db_conf.RABBITMQ_ADDR,
            exchange_name='meta_event_system',
            exchange_type=ExchangeType.TOPIC.value,
            routing_key='{}.{}'.format(self.captured_event.namespace, self.captured_event.event_type),
        )
        producer.publish(event_message)
        producer.release()


@listener_registrar
class ExceptionListener(ListenerPrototype):
    """
    错误事件监听者
    """

    name = 'exception_listener'

    def follow_registered_event(self):
        """
        关注错误类型的已注册事件
        :return:
        """
        self.followed_events = rt_context.event_list.get(EventType.EXCEPTION.value, None)

    def check_event(self, *args, **kwargs):
        pass

    def publish_event(self):
        pass
