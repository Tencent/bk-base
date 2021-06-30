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

from functools import wraps

from metadata.interactor.record import KafkaRecordsPersistence
from metadata.runtime import rt_context
from metadata.state.state import State, StateMode


class TransactionalDispatchMixIn(object):
    def __init__(self, interact_transaction_enabled=True, *args, **kwargs):
        super(TransactionalDispatchMixIn, self).__init__(*args, **kwargs)
        self.db_conf = self.config_collection.db_config
        self.normal_conf = self.config_collection.normal_config
        self.system_state = kwargs.get(str('state'), rt_context.system_state)  # type:State
        self.interact_transaction_enabled = interact_transaction_enabled
        self.persistence = KafkaRecordsPersistence()

    def transactional_inner_dispatch(self):
        """
        执行基于MetaData Interactor事务系统的调度。

        :return:
        """
        self.invoke()
        if self.interact_transaction_enabled and self.system_state.mode == StateMode.ONLINE:
            self.dispatch_commit(self.apply)()
        else:
            self.apply()

    def dispatch_commit(self, func):
        """
        事务提交调度操作器。

        :param func: 被装饰的事务提交函数
        :return: 装饰后的函数
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            self.system_state.wait_to_valid(self.config_collection.interactor_config.COMMIT_TIMEOUT, raise_err=True)
            with self.system_state.interactor_commit_lock:
                # 增加事务计数
                counter = self.system_state.transaction_counter
                counter += 1
                self.transaction_count = counter.value
                self.logger.info('Transactional ID is {} now'.format(counter.value))
                # 应用
                ret = func(*args, **kwargs)
                # kafka功能开关
                service_switches = getattr(rt_context, 'service_switches', {})
                kafka_switch = service_switches.get('kafka', {}).get('master', True)
                if self.if_recording and kafka_switch:
                    self.persistence.save(self.transactional_operations)
                return ret

        return wrapper
