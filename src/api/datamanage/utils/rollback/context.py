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


import uuid
from contextlib import contextmanager
from functools import wraps

from django.core.cache import cache
from django.utils.encoding import force_text
from django.db import models
from django.db.models.signals import pre_save, pre_delete

from common.log import logger

from datamanage import exceptions as dm_errors


CACHE_KEY_TEMPLATE = 'data_model_application_rollback_{}'


class _ContextWrapper(object):
    def __init__(self, func, args):
        self._func = func
        self._args = args
        self._context = func(*args)

    def __enter__(self):
        return self._context.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        return self._context.__exit__(exc_type, exc_value, traceback)

    def __call__(self, func):
        @wraps(func)
        def decorator(*args, **kwargs):
            with self._func(*self._args):
                return func(*args, **kwargs)

        return decorator


def create_temporary_reversion(watching_models, expired_time=3):
    """
    自动进行元数据同步的事务上下文管理器，不支持批量update的操作

    :param using: 需要做事务处理的数据库的别名，如Django默认的数据库别名为default
    :param manage_manually: 默认为False，是否需要手动提交事务，可以参考django原生的transaction.atomic()，不推荐使用

    Examples:
        with auto_meta_sync(using='bkdata_basic'):
            DemoModel.objects.create(param1=1, params2='abc')
    """
    return _ContextWrapper(_create_rollback_context, (watching_models, expired_time))


@contextmanager
def _create_rollback_context(watching_models, expired_time=3):
    """
    创建监听需要回滚的Django Model的上下文管理，在退出watch时把记录的所有变更都保存到django cache中，并配置一定的超时时间，以供
    调用方增删改时失败了需要进行回滚

    :param watching_models: 需要进行监听的Django Model列表
    :param expired_time: 待回滚目标过期时间
    """
    rollback_id = uuid.uuid4().hex

    model_managers = []
    for watching_model in watching_models:
        model_manager = RollbackModelManager(watching_model)
        model_manager.watch()
        model_managers.append(model_manager)

    yield rollback_id

    cache_content = {}
    for model_manager in model_managers:
        model_manager.unwatch()
        cache_content[model_manager.model._meta.model_name] = model_manager.cache_content()

    cache_key = CACHE_KEY_TEMPLATE.format(rollback_id)
    cache.set(cache_key, cache_content, expired_time)


def rollback_by_id(rollback_id):
    cache_key = CACHE_KEY_TEMPLATE.format(rollback_id)
    cache_content = cache.get(cache_key)

    if not cache_content:
        raise dm_errors.RollbackContentExpiredError()

    try:
        for model_name, model_cache_content in list(cache_content.items()):
            for primary_key_value, instance in list(model_cache_content['created'].items()):
                instance.delete()

            for primary_key_value, instance in list(model_cache_content['updated'].items()):
                instance.save()

            for primary_key_value, instance in list(model_cache_content['deleted'].items()):
                instance.delete()
    except Exception as e:
        logger.error('Rollback by {} error: {}'.format(rollback_id, e))
        raise dm_errors.RollbackError()


class RollbackModelManager(object):
    """管理待回滚Model"""

    def __init__(self, model):
        self._model = model

        self._created_models = {}
        self._updated_models = {}
        self._deleted_models = {}

    @property
    def model(self):
        return self._model

    def cache_content(self):
        """生成记录于cache用于回滚的内容"""
        return {
            'created': self._created_models,
            'updated': self._updated_models,
            'deleted': self._deleted_models,
        }

    def watch(self):
        if not issubclass(self.model, models.Model):
            raise dm_errors.OnlySupportModelRollbackError()

        for signal, signal_receiver in self.get_signals_and_receivers(self.model):
            signal.connect(signal_receiver, sender=self.model)

    def unwatch(self):
        for signal, signal_receiver in self.get_signals_and_receivers(self.model):
            signal.disconnect(signal_receiver, sender=self.model)

    def get_signals_and_receivers(self, model):
        yield pre_save, self.pre_save_receiver
        yield pre_delete, self.pre_delete_receiver

    def pre_save_receiver(self, sender, instance, **kwargs):
        primary_key_value = force_text(instance.pk)
        # 通过是否有已经生成的primary key来区分当前要保存的instance是需要创建还是需要更新
        if not instance._state.adding:
            if primary_key_value not in self._created_models and primary_key_value not in self._updated_models:
                self._updated_models[primary_key_value] = instance
        else:
            if primary_key_value not in self._created_models:
                self._created_models[primary_key_value] = instance

    def pre_delete_receiver(self, sender, instance, **kwargs):
        primary_key_value = force_text(instance.pk)
        if primary_key_value not in self._deleted_models:
            self._deleted_models[primary_key_value] = instance
