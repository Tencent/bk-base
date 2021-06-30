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

import functools
import logging
from abc import abstractmethod
from datetime import datetime
from typing import Dict, List
from uuid import uuid4

import attr
import gevent
from attr.validators import in_
from gevent.lock import RLock

from metadata.event_system.listener import listener_dispatcher
from metadata.exc import InteractorError, InteractorOperatorError
from metadata.runtime import rt_context
from metadata.util.common import StrictABCMeta
from metadata.util.i18n import lazy_selfish as _

interact_lock = RLock()


@attr.s
class Operation(object):
    """
    元数据操作。
    """

    method = attr.ib(
        type=str, validator=in_(('CREATE', 'UPDATE', 'DELETE', 'CONDITIONAL_DELETE', 'CONDITIONAL_UPDATE'))
    )
    changed_data = attr.ib(type=str)
    type_name = attr.ib(type=str)
    operate_id = attr.ib(type=str, default='')
    transaction_id = attr.ib(type=int, default=0)
    conditional_filter = attr.ib(type=str, default=None)
    identifier_value = attr.ib(type=str, default=None)
    change_time = attr.ib(type=datetime, factory=datetime.now)
    extra_ = attr.ib(type=Dict, factory=dict)


@attr.s
class TransactionalOperations(object):
    """
    元数据事务。
    """

    operations = attr.ib(type=List[Operation])
    transaction_id = attr.ib(type=int, default=0)
    operate_id = attr.ib(type=str, default=str(uuid4()))
    state = attr.ib(type=str, default='Init', validator=in_(('Init', 'ApplyPrepare', 'Applied')))
    construct_time = attr.ib(type=datetime, default=datetime.now())
    applied_time = attr.ib(type=datetime, default=None)


@attr.s
class Action(object):
    """
    单个operator操作。
    """

    method = attr.ib()
    item = attr.ib()
    linked_record = attr.ib(type=Operation)


@attr.s
class BatchAction(object):
    """
    批量operator操作。
    """

    method = attr.ib()
    md_type_name = attr.ib()
    items = attr.ib(factory=list)
    linked_records = attr.ib(factory=list)


class OperationOrchestrator(object, metaclass=StrictABCMeta):
    """
    OperationOrchestrator基类。用于编排元数据操作执行。
    """

    __abstract__ = True
    action_operators = []

    def __init__(self, *args, **kwargs):
        backend_session = kwargs.get('backend_session', None)
        self.backend_session = backend_session
        self.logger = logging.getLogger(self.__class__.__name__)

    def dispatch(
        self,
        records,
    ):
        """
        调度Operation执行。

        :return:
        """
        self.construct_actions(records)
        self.dispatch_actions()

    @abstractmethod
    def construct_actions(self, *args, **kwargs):
        """
        构造本次需要执行的actions。

        :return:
        """
        pass

    @abstractmethod
    def dispatch_actions(self, *args, **kwargs):
        """
        执行actions。

        :return:
        """
        pass


class SingleOperationOrchestrator(OperationOrchestrator, metaclass=StrictABCMeta):
    """
    单次OperationOrchestrator基类。
    """

    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super(SingleOperationOrchestrator, self).__init__(*args, **kwargs)
        self.batch_operate = False
        self.actions_lst = []

    @abstractmethod
    def construct_actions(self, *args, **kwargs):
        pass

    def dispatch_actions(self, *args, **kwargs):
        for action in self.actions_lst:
            self.operate_action(action)

    def operate_action(self, action):
        """
        通用dispatch单个action操作，一般在dispatch_actions中自定义使用。
        """
        for operator_cls in self.action_operators:
            operator = operator_cls(action, self.backend_session, batch=self.batch_operate)
            if operator.in_scope:
                self.logger.info('Executing {} action {}.'.format(self.__class__.__name__, action))
                operator.execute()
                break
        else:
            raise InteractorError(_('Fail to get operator. The action is {}'.format(action)))


class BatchOperationOrchestrator(OperationOrchestrator, metaclass=StrictABCMeta):
    """
    批量OperationOrchestrator基类。
    """

    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super(BatchOperationOrchestrator, self).__init__(*args, **kwargs)
        self.batch_operate = True
        self.batch_actions_lst = []

    @abstractmethod
    def construct_actions(self, records):
        pass

    def dispatch_actions(self, *args, **kwargs):
        for batch_action in self.batch_actions_lst:
            self.operate_action(batch_action)

    def operate_action(self, batch_action):
        """
        通用dispatch单个batch_action操作，完成一次批量动作。
        """
        for operator_cls in self.action_operators:
            operator = operator_cls(batch_action, self.backend_session, batch=self.batch_operate)
            if operator.in_scope and operator.batch_support:
                self.logger.info('Executing {} batch_action {}.'.format(self.__class__.__name__, batch_action))
                operator.execute()
                break
        else:
            raise InteractorError(_('Fail to get operator. The batch_action is {}'.format(batch_action)))


class Operator(object, metaclass=StrictABCMeta):
    """
    元数据操作执行器，基类。
    """

    __abstract__ = True
    batch_support = False  # 是否支持批量操作

    def __init__(self, action, backend_session, batch=False, operate_filters=None):
        """
        :param action: 需要执行的操作
        :param backend_session: 后端事务实例
        """
        if operate_filters is None:
            operate_filters = []
        self.action = action
        self.backend_session = backend_session
        self.operate_filters = operate_filters
        self.batch = batch
        self.backend_type = None
        self.backend_id = None
        if self.batch:
            self.item = None
            self.items = [item for item in self.action.items]
            self.md_type_name = self.action.md_type_name
        else:
            self.item = self.action.item
            self.items = [self.action.item]
            self.md_type_name = self.action.linked_record.type_name

    @abstractmethod
    def create(self):
        """
        执行创建操作

        :return:
        """
        pass

    @abstractmethod
    def update(self):
        """
        执行更新操作

        :return:
        """
        pass

    @abstractmethod
    def delete(self):
        """
        执行删除操作

        :return:
        """
        pass

    @abstractmethod
    def in_scope(self):
        """
        指示操作是否使用此操作器执行。

        :return: True/False
        """
        return False

    def execute_monitor(self, func):
        """
        执行操作监听装饰器

        :param func: 监听方法
        :return: 装饰后的目标方法
        """

        target_listener = ['dgraph_mutation_listener']

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            mutation_result = func(*args, **kwargs)
            # 仅对dgraph的operator进行处理
            in_scope = 'Dgraph' in self.__class__.__name__ and not self.batch
            # 存在分发id作为action_id
            dispatch_id = getattr(rt_context, 'dispatch_id', None)
            # 事件系统开关
            service_switches = getattr(rt_context, 'service_switches', {})
            event_switch = service_switches.get('event', {}).get('master', False)
            if all([in_scope, dispatch_id, event_switch]):
                mutation_record = self.action.linked_record
                mutation = dict(
                    dispatch_id=dispatch_id,
                    record=mutation_record,
                    result=mutation_result,
                    target=dict(type=self.backend_type, id=self.backend_id),
                )
                gevent.spawn(listener_dispatcher(target_listener, mutation))
            return mutation_result

        return wrapper

    def execute(self):
        """
        根据action执行具体操作。

        :return:
        """
        if self.batch:
            items = []
            # 执行filter过滤应当忽略的action
            for item in self.items:
                for filter_ in self.operate_filters:
                    if filter_(md=item).filter():
                        break
                else:
                    items.append(item)
            if not items:
                return
            self.items = items
        else:
            # 执行filter过滤应当忽略的action
            for filter_ in self.operate_filters:
                if filter_(md=self.action.item).filter():
                    return
        execute_method = getattr(self, self.action.method.lower())
        return self.execute_monitor(execute_method)()
