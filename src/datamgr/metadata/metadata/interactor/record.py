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
import logging
import time
from abc import abstractmethod
from os.path import isfile

import cattr
import jsonext
from cached_property import threaded_cached_property
from kafka import KafkaConsumer, KafkaProducer
from retry import retry

from metadata.backend.interface import BackendType
from metadata.exc import ReplyError
from metadata.interactor.core import TransactionalOperations, interact_lock
from metadata.runtime import rt_context, rt_g
from metadata.state import BackendHAState
from metadata.state.state import StateMode
from metadata.util.common import StrictABCMeta
from metadata.util.i18n import lazy_selfish as _


class RecordingSupport(object):
    def __init__(self, state=None, config_collection=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_collection = rt_context.config_collection if not config_collection else config_collection
        self.db_conf = self.config_collection.db_config
        self.interactor_config = self.config_collection.interactor_config
        self.state = state if state else rt_context.system_state  # type = State

    def get_backend_state(self, name='slave'):
        backends_ha_state = self.state['backends_ha_state']
        return backends_ha_state['dgraph'][name]['state']

    def set_backend_on(self, name='slave'):
        if self.state.mode == StateMode.ONLINE and self.state.wait_to_valid():
            backends_ha_state = self.state['backends_ha_state']
            backends_ha_state['dgraph'][name]['state'] = BackendHAState.ON.value
            with self.state.state_manager_lock:
                self.state['backends_ha_state'] = backends_ha_state

    def set_backend_chaotic(self, name='slave'):
        if self.state.mode == StateMode.ONLINE and self.state.wait_to_valid():
            backends_ha_state = self.state['backends_ha_state']
            backends_ha_state['dgraph'][name]['state'] = BackendHAState.CHAOTIC.value
            with self.state.state_manager_lock:
                self.state['backends_ha_state'] = backends_ha_state


class RecordsPersistence(RecordingSupport, metaclass=StrictABCMeta):
    """
    事务操作记录持久化基类。
    """

    __abstract__ = True

    @abstractmethod
    def save(self, *args, **kwargs):
        """
        保存事务操作记录。
        """
        pass


class LocalRecordsPersistence(RecordsPersistence):
    def save(self, transactional_operations):
        transactional_operations_info = cattr.unstructure(transactional_operations)
        self.logger.info(transactional_operations_info, extra={'recording': True})


class KafkaRecordsPersistence(RecordingSupport):
    def __init__(self, *args, **kwargs):
        """
        Todo:异常及kafka offset异常处理。
        """
        super(KafkaRecordsPersistence, self).__init__(*args, **kwargs)
        self.persist_file_fw = None
        self.max_consume_offset = 0

    # 生产者相关
    @staticmethod
    def _value_serializer(item):
        json_str = jsonext.dumps(item)
        return json_str.encode('utf-8')

    @threaded_cached_property
    def kafka_producer(self):
        with interact_lock:
            if not hasattr(rt_context, 'kafka_producer'):
                with interact_lock:
                    self.logger.info('Init new producer now.')
                    client = KafkaProducer(
                        bootstrap_servers=self.db_conf.KAFKA_ADDR,
                        value_serializer=self._value_serializer,
                        max_request_size=4 * 1024 * 1024,
                    )
                    rt_g.kafka_producer = client
                    rt_g.kafka_producer_fail_count = 0
        return rt_context.kafka_producer

    def save(self, transactional_operations):
        transactional_operations_info = cattr.unstructure(transactional_operations)
        self.produce(transactional_operations_info)

    def produce(self, transactional_operations_info):
        """
        将操作事务记录，发布到kafka。

        :param transactional_operations_info: 操作事务记录
        :return:
        """
        try:
            self.basic_produce(transactional_operations_info)
        except Exception:
            self.logger.exception('Fail to produce.')
            rt_g.kafka_producer_fail_count += 1
            if rt_g.kafka_producer_fail_count > self.interactor_config.KAFKA_FAIL_RESET_CNT:
                del self.kafka_producer
                with interact_lock:
                    delattr(rt_g, 'kafka_producer')
            raise

    @retry(tries=3, delay=0.1)
    def basic_produce(self, transactional_operations_info):
        future = self.kafka_producer.send(
            str(self.interactor_config.KAFKA_TRANSACTIONAL_OPERATION_TOPIC), transactional_operations_info
        )
        future.get(timeout=5)

    # 消费者相关

    @threaded_cached_property
    def persist_consumer(self):
        self.logger.info('Init new persist consumer now.')
        rt_g.persist_consumer_fail_count = 0
        return KafkaConsumer(
            str(
                self.interactor_config.KAFKA_TRANSACTIONAL_OPERATION_TOPIC,
            ),
            group_id='persist_transactional_operations',
            bootstrap_servers=self.db_conf.KAFKA_ADDR,
            consumer_timeout_ms=100,
            value_deserializer=json.loads,
        )

    def consume(self):
        """
        消费事务操作记录。

        :return:
        """
        try:
            for message in self.persist_consumer:
                self.logger.info("Persisting {}/{}.".format(message.offset, message.value['transaction_id']))
                self.logger.info("Persisting content {}.".format(message.value))
                self.persist(message.value)
        except Exception:
            rt_g.persist_consumer_fail_count += 1
            if rt_g.persist_consumer_fail_count > self.interactor_config.KAFKA_FAIL_RESET_CNT:
                del self.persist_consumer
            self.logger.exception('Fail to consume.')

    def __enter__(self):
        if isfile(
            self.interactor_config.persistence_offset,
        ):
            with open(self.interactor_config.persistence_offset, 'r') as fr:
                self.max_consume_offset = json.load(
                    fr,
                )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.persist_file_fw:
            self.persist_file_fw.close()
        with open(self.interactor_config.persistence_offset, 'w') as fw:
            json.dump(
                self.max_consume_offset,
                fw,
            )

    @retry(tries=3, delay=0.1)
    def persist(self, transactional_operations_info):
        """
        持久化从kafka消费的操作事务记录。

        :param transactional_operations_info: 操作事务记录
        :return:
        """
        if not self.persist_file_fw:
            self.persist_file_fw = open(self.interactor_config.persistence_file_path, 'a')
        self.persist_file_fw.write(jsonext.dumps(transactional_operations_info) + "\n")

    def persisting(self):
        """
        持续持久化。

        :return:
        """
        while True:
            self.consume()
            time.sleep(0.05)


class RecordsReplay(RecordingSupport):
    def __init__(self, state=None, config_collection=None, backend_type=BackendType.DGRAPH_BACKUP):
        self.backend_type = backend_type
        super(RecordsReplay, self).__init__(state, config_collection)

    @threaded_cached_property
    def replay_consumer(self):
        self.logger.info('Init new replay consumer now.')
        rt_g.replay_consumer_fail_count = 0
        return KafkaConsumer(
            str(
                self.interactor_config.KAFKA_TRANSACTIONAL_OPERATION_TOPIC,
            ),
            group_id='replay_transactional_operations',
            bootstrap_servers=self.db_conf.KAFKA_ADDR,
            value_deserializer=json.loads,
            consumer_timeout_ms=1000,
            max_poll_interval_ms=500000,
            max_poll_records=300,
        )

    def consume(self):
        try:
            for message in self.replay_consumer:
                self.replay(message.value)
        except ReplyError:
            self.set_backend_chaotic()
            self.logger.exception('[chaotic]replay process occur an exception')
        except Exception:
            rt_g.replay_consumer_fail_count += 1
            if rt_g.replay_consumer_fail_count > self.interactor_config.KAFKA_FAIL_RESET_CNT:
                del self.replay_consumer
            self.logger.exception('Fail to consume.')

    @retry(
        tries=3,
        delay=0.1,
    )
    def replay(self, transactional_operations_info):
        """
        重放从kafka消费的操作事务记录。

        :param transactional_operations_info: 操作事务记录
        :return:
        """
        transactional_operations = None
        try:
            from metadata.interactor.interact import SingleInteractor

            transactional_operations = cattr.structure(transactional_operations_info, TransactionalOperations)
            sd = SingleInteractor(
                backend_type=self.backend_type, interact_transaction_enabled=False, if_recording=False
            )
            sd.dispatch(transactional_operations.operations)
            self.logger.info("Replayed the transaction {}".format(transactional_operations.transaction_id))
        except Exception:
            self.logger.exception(
                'Fail to reply the transaction {}.'.format(
                    transactional_operations.transaction_id
                    if transactional_operations
                    else 'before structure transactional operations'
                )
            )
            raise ReplyError(_('Fail to replay transaction.'))

    def replay_serving(self):
        """
        持续重放。

        :return:
        """
        while True:
            # 仅当State为在线状态时，检测是否有集群切换请求。
            if self.state.wait_to_valid():
                barrier_set = 0
                if (
                    self.state['backends_ha_state'][self.backend_type.raw.value]['slave']['state']
                    == BackendHAState.SWITCH_PREPARE.value
                ):
                    self.logger.info('Start to prepare slave backend HA switch.')
                    barrier_set = 1
                self.consume()
                if barrier_set:
                    self.state.replay_switch_barrier.remove()
                    self.logger.info('Slave backend HA switch prepared.')
            time.sleep(0.05)
