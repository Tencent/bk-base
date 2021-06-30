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
import datetime
import json
from collections import namedtuple
from contextlib import contextmanager
from copy import copy
from functools import wraps
from threading import local

from common.api import MetaApi
from common.base_utils import get_db_settings_by_model
from common.django_utils import CustomJSONEncoder
from common.local import get_request_username
from common.log import sys_logger
from common.meta.common import find_related_tag
from common.meta.models import MetaSyncSupport
from common.signals import post_meta_commit, pre_meta_commit, tag_maintain
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import DatabaseError, IntegrityError, models, transaction
from django.db.models.signals import post_delete, post_save, pre_save
from django.utils import timezone
from django.utils.encoding import force_text
from django.utils.translation import ugettext as _

from common import exceptions as errors


def auto_meta_sync(using, manage_manually=False):
    """
    自动进行元数据同步的事务上下文管理器，不支持批量update的操作

    Args:
        using 需要做事务处理的数据库的别名，如Django默认的数据库别名为default
        manage_manually 默认为False，是否需要手动提交事务，可以参考django原生的transaction.atomic()，不推荐使用

    Examples:
        with auto_meta_sync(using='bkdata_basic'):
            DemoModel.objects.create(param1=1, params2='abc')
    """
    return _ContextWrapper(_create_meta_sync_context, (using, manage_manually))


def meta_sync_register(model, fields=None, exclude=()):
    """
    注册ORM模型(model)为需要进行元数据同步的类

    Args:
        model 需要进行数据同步的model，必须继承至django.db.models.Model
        fields 需要进行数据同步的字段
        exclude 不需要进行数据同步的字段

    Examples:
        class DemoModel(models.Model):
            param1 = models.CharField(max_length=128)
        meta_sync_register(DemoModel)
    """
    if not issubclass(model, models.Model):
        raise errors.MetaSyncModelRegisterError()

    def register(model):
        # Prevent multiple registration.
        if is_registered(model):
            raise errors.MetaModelHasRegistered(
                "{model} has already been registered for sync data to metadata".format(
                    model=model,
                )
            )
        # Parse fields.
        opts = model._meta.concrete_model._meta
        db_settings = get_db_settings_by_model(model)
        sync_params = _SyncParams(
            fields=tuple(
                field_name
                for field_name in (
                    [field.name for field in opts.local_fields + opts.local_many_to_many] if fields is None else fields
                )
                if field_name not in exclude
            ),
            table=opts.db_table,
            database=db_settings["NAME"] if db_settings is not None else "",
        )
        # Register the model.
        _registered_models[_get_registration_key(model)] = sync_params
        # Connect signals.
        for sender, signal, signal_receiver in get_senders_and_signals(model):
            signal.connect(signal_receiver, sender=sender)
        # All done!
        return model

    # Return a class decorator if model is not given
    if model is None:
        return register
    # Register the model.
    return register(model)


def meta_sync_unregister(model):
    _assert_registered(model)
    del _registered_models[_get_registration_key(model)]
    # Disconnect signals.
    for sender, signal, signal_receiver in get_senders_and_signals(model):
        signal.disconnect(signal_receiver, sender=sender)


@contextmanager
def _create_meta_sync_context(using, manage_manually=False):
    _push_frame(manage_manually)
    err = None
    try:
        with transaction.atomic(
            using=using,
        ):
            yield
            current_frame = _current_frame()
            try:
                sync_model_data(current_frame.contexts)
            except errors.ApiResultError as e:
                err = e.errors
                raise IntegrityError(None, e.message)
            except Exception as e:
                raise IntegrityError(None, repr(e))
    except DatabaseError as e:
        if len(e.args) > 1:
            raise errors.MetaSyncError(e.args[1], errors=err)
        else:
            raise errors.MetaSyncError(str(e), errors=err)
    finally:
        _pop_frame()


def sync_model_data(db_operate_list):
    pre_meta_commit.send(sender=sync_model_data)
    response = None
    retries = 1
    while retries > 0:
        try:
            request_params = {"bk_username": get_request_username(), "db_operations_list": db_operate_list}
            response = MetaApi.sync_hook(request_params, timeout=300)
            post_meta_commit.send(sender=sync_model_data)
            break
        except errors.ApiRequestError as e:
            retries -= 1
            if retries == 0:
                sys_logger.error(e)
                raise errors.ApiRequestError(_("请求元数据接口异常({})，数据同步失败").format(e))

    if response is not None and not response.is_success():
        raise errors.ApiResultError(_("请求元数据接口失败({})，数据同步失败").format(response.message), errors=response.errors)


def add_to_meta_sync_wait_list(obj, method):
    """
    add sync info to the pending sync list.
    """
    if method == "DELETE":
        tag_maintain.send(sender=obj.__class__, instance=obj)

    contexts = _current_frame().contexts
    contexts = _copy_contexts(contexts)
    prevous_storage = _current_frame().meta["previous"]
    info = _construct_sync_info(obj, method, prevous_storage)
    if not info:
        return
    else:
        obj_key, sync_info = info
        contexts.append(sync_info)

    _update_frame(contexts=contexts)


def _construct_sync_info(obj, method, previous_storage=None):
    if obj.pk is None:
        return
    sync_params = _get_options(obj.__class__)
    object_id = force_text(obj.pk)
    opts = obj._meta.concrete_model._meta
    obj_key = (opts.app_label, opts.model_name, object_id)

    user_name = get_request_username()

    if method == "SNAPSHOT":
        changed_data = capture_model_values(obj)
    elif method == "UPDATE" and previous_storage:
        changed_data = json.dumps(_generate_update_content(previous_storage, obj_key, obj), cls=CustomJSONEncoder)
    else:
        changed_data = json.dumps(capture_model_values(obj), cls=CustomJSONEncoder)

    sync_info = {
        "method": method,
        "db_name": sync_params.database,
        "table_name": sync_params.table,
        "primary_key_value": object_id,
        "changed_data": changed_data,
        "change_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
    }

    if user_name:
        sync_info["created_by"] = user_name
        sync_info["updated_by"] = user_name
    return obj_key, sync_info


def _generate_update_content(previous_storage, obj_key, obj):
    """Todo:tag.code/id临时支持，需要改进"""
    pk_name = obj._meta.pk.name
    previous_sync_info = previous_storage[obj_key]
    pre_changed_data = previous_sync_info["changed_data"]
    now_diffed_changed_data = {
        k: v
        for k, v in list(capture_model_values(obj).items())
        if pre_changed_data[k] != v or k == pk_name or k == "code"
    }
    return now_diffed_changed_data


def capture_model_values(model):
    if isinstance(model, MetaSyncSupport):
        return model.capture_values()
    else:
        return MetaSyncSupport.model_to_dict(model)


def add_to_previous_storage(obj):
    meta_info = _current_frame().meta
    previous_storage = meta_info["previous"]
    obj_key, sync_info = _construct_sync_info(obj, "SNAPSHOT")
    previous_storage[obj_key] = sync_info

    _update_frame(meta=meta_info)


# 用于触发同步的Receiver


def post_save_receiver(sender, instance, created, **kwargs):
    if is_registered(sender) and is_active() and not is_manage_manually():
        if created:
            add_to_meta_sync_wait_list(instance, "CREATE")
        else:
            add_to_meta_sync_wait_list(instance, "UPDATE")


def pre_save_receiver(sender, instance, **kwargs):
    if is_registered(sender) and is_active() and not is_manage_manually():
        try:
            if not instance.pk:
                return
            pk_name = instance._meta.pk.name
            prev_instance = sender.objects.get(**{pk_name: instance.pk})
            add_to_previous_storage(prev_instance)
        except ObjectDoesNotExist:
            return


def post_delete_receiver(sender, instance, **kwargs):
    if is_registered(sender) and is_active() and not is_manage_manually():
        add_to_meta_sync_wait_list(instance, "DELETE")


if find_related_tag and getattr(settings, "TAG_RELATED_MODELS", None):

    def tag_maintain_receiver(sender, instance, **kwargs):
        if is_registered(sender) and is_active():
            if isinstance(settings.TAG_RELATED_MODELS[sender.__name__]["target_type"], (tuple, list)):
                target_types = settings.TAG_RELATED_MODELS[sender.__name__]["target_type"]
            else:
                target_types = [settings.TAG_RELATED_MODELS[sender.__name__]["target_type"]]
            for target_type in target_types:
                query = find_related_tag(
                    target_type,
                    instance.pk,
                )
                for tag_target in query:
                    tag_target.delete()


def get_senders_and_signals(model):
    yield model, post_save, post_save_receiver
    yield model, post_delete, post_delete_receiver
    yield model, pre_save, pre_save_receiver
    if (model.__name__ in getattr(settings, "TAG_RELATED_MODELS", [])) and tag_maintain_receiver:
        yield model, tag_maintain, tag_maintain_receiver


# 同步栈实现（用于支持嵌套事务同步）和相关工具方法


_SyncParams = namedtuple(
    "_SyncParams",
    (
        "fields",
        "table",
        "database",
    ),
)

_StackFrame = namedtuple(
    "StackFrame",
    (
        "manage_manually",
        "date_created",
        "meta",
        "contexts",
    ),
)


class _Local(local):
    def __init__(self):
        self.stack = ()


_local = _Local()


def is_active():
    return bool(_local.stack)


def current_frame():
    return _current_frame()


def push_frame(manage_manually):
    return _push_frame(manage_manually)


def pop_frame():
    return _pop_frame()


def _current_frame():
    if not is_active():
        raise errors.MetaSyncError("There is no active frame for this thread")
    return _local.stack[-1]


def _copy_contexts(contexts):
    return copy(contexts)


def _push_frame(manage_manually):
    if is_active():
        current_frame = _current_frame()
        contexts = _copy_contexts(current_frame.contexts)
        stack_frame = current_frame._replace(
            manage_manually=manage_manually,
            contexts=contexts,
        )
    else:
        stack_frame = _StackFrame(
            manage_manually=manage_manually,
            date_created=timezone.now(),
            contexts=[],
            meta={"previous": {}},
        )
    _local.stack += (stack_frame,)


def _update_frame(**kwargs):
    _local.stack = _local.stack[:-1] + (_current_frame()._replace(**kwargs),)


def _pop_frame():
    prev_frame = _current_frame()
    _local.stack = _local.stack[:-1]
    if is_active():
        contexts = []
        _update_frame(
            date_created=prev_frame.date_created,
            contexts=contexts,
            meta=prev_frame.meta,
        )


def is_manage_manually():
    return _current_frame().manage_manually


class _ContextWrapper:
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


def _get_registration_key(model):
    return (model._meta.app_label, model._meta.model_name)


_registered_models = {}


def is_registered(model):
    return _get_registration_key(model) in _registered_models


def get_registered_models():
    return (apps.get_model(*key) for key in list(_registered_models.keys()))


def _assert_registered(model):
    if not is_registered(model):
        raise errors.MetaSyncModelRegisterError(
            "{model} has not been registered with meta sync".format(
                model=model,
            )
        )


def _get_options(model):
    _assert_registered(model)
    return _registered_models[_get_registration_key(model)]
