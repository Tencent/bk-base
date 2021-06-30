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

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime

from common.local import get_request_username
from django.core import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, router, transaction
from django.db.models.query import QuerySet
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
from django.utils import timezone
from django.utils.encoding import force_text
from reversion.errors import RegistrationError, RevisionManagementError
from reversion.models import Revision, Version
from reversion.revisions import _get_content_type, _get_options, _Local, _StackFrame, _VersionOptions
from reversion.signals import post_revision_commit, pre_revision_commit

from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.models import FlowInfo, FlowLinkInfo, FlowNodeInfo, FlowVersionLog
from dataflow.shared.log import flow_logger as logger

try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps  # Python 2.4 fallback.

from common.transaction import auto_meta_sync
from django.apps import apps
from django.utils.decorators import available_attrs


def register_signal_handlers():
    handlers = [FlowNodeSignalHandler(), FlowLinkSignalHandler()]
    for handler in handlers:
        if not is_registered(handler.model()):
            my_register(handler.model())

        for signal in handler.signals:
            signal["signal"].connect(
                signal["receiver"],
                sender=handler.model(),
            )
    return handlers


def valid_signal_wrap():
    def _wrap(func):
        @wraps(func, assigned=available_attrs(func))
        def _deco(self, *args, **kwargs):
            try:
                self.flows[int(kwargs["instance"].flow_id)]
            except KeyError:
                # 如启动前非调用 view 类 PUT 操作来进行节点保存时会触发这个逻辑，并忽略版本历史流水记录
                logger.info("[flow_version]: ignore signal which is not from view")
            else:
                return func(self, *args, **kwargs)

        return _deco

    return _wrap


class SignalHandler(object):
    def post_save_receiver(self, sender, instance, using, **kwargs):
        return None

    def pre_save_receiver(self, sender, instance, using, **kwargs):
        return None

    def post_delete_receiver(self, sender, instance, using, **kwargs):
        return None

    def pre_delete_receiver(self, sender, instance, using, **kwargs):
        return None

    def update(self, _local, flow_id):
        self.flows.update(
            {
                int(flow_id): {
                    "delete_component_ids": [],
                    "add_components": [],
                    "is_changed": False,
                    "changed_num": 0,
                    "_local": _local,
                }
            }
        )
        return self

    def clear(self, flow_id):
        self.flows.pop(int(flow_id))

    def __init__(self):
        self.signals = [
            {"signal": post_save, "receiver": self.post_save_receiver},
            {"signal": pre_save, "receiver": self.pre_save_receiver},
            {"signal": post_delete, "receiver": self.post_delete_receiver},
            {"signal": pre_delete, "receiver": self.pre_delete_receiver},
        ]
        self.flows = {}


class FlowNodeSignalHandler(SignalHandler):
    def __unicode__(self):
        return "flow_node"

    @valid_signal_wrap()
    def post_save_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]

        if flow_dict["is_changed"]:
            add_to_revision(flow_dict["_local"], instance, model_db=using)
            flow_dict["add_components"].append(instance)
            flow_dict["changed_num"] += 1
        flow_dict["is_changed"] = False

    @valid_signal_wrap()
    def pre_save_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]

        try:
            config = FlowNodeInfo.objects.get(pk=instance.pk).node_config
        except FlowNodeInfo.DoesNotExist:
            # 创建节点
            flow_dict["is_changed"] = True
        else:
            if config != instance.node_config:
                try:
                    # 若修改了节点信息，更新节点的最新版本号
                    # 若 try 之下逻辑(记录版本流水)报错，也不应该影响这个逻辑
                    instance.latest_version = NodeUtils.gene_version()
                    pre_component = Version.objects.get_for_object(instance)
                    if len(pre_component) > 0:
                        delete_id = pre_component[0].id
                        flow_dict["delete_component_ids"].append(str(delete_id))
                except Exception as e:
                    logger.error("version record error for {}".format(e))
                else:
                    flow_dict["is_changed"] = True

    @valid_signal_wrap()
    def post_delete_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]

        flow_dict["changed_num"] += 1
        flow_dict["is_changed"] = False

    @valid_signal_wrap()
    def pre_delete_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]
        flow_dict["is_changed"] = True
        try:
            delete_id = Version.objects.get_for_object(instance)[0].id
        except Exception as e:
            logger.error("version record error for {}".format(e))
        else:
            flow_dict["delete_component_ids"].append(str(delete_id))

    @staticmethod
    def model():
        return FlowNodeInfo


class FlowLinkSignalHandler(SignalHandler):
    def __unicode__(self):
        return "flow_link"

    @valid_signal_wrap()
    def post_save_receiver(self, sender, instance, using, **kwargs):

        flow_dict = self.flows[int(instance.flow_id)]
        flow_dict["add_components"].append(instance)
        flow_dict["changed_num"] += 1
        flow_dict["is_changed"] = False
        add_to_revision(flow_dict["_local"], instance, model_db=using)

    @valid_signal_wrap()
    def pre_save_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]
        flow_dict["is_changed"] = True

    @valid_signal_wrap()
    def post_delete_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]
        flow_dict["changed_num"] += 1
        flow_dict["is_changed"] = False

    @valid_signal_wrap()
    def pre_delete_receiver(self, sender, instance, using, **kwargs):
        flow_dict = self.flows[int(instance.flow_id)]
        flow_dict["is_changed"] = True
        try:
            delete_id = Version.objects.get_for_object(instance)[0].id
        except IndexError:
            logger.error("version record error")
        else:
            flow_dict["delete_component_ids"].append(str(delete_id))

    @staticmethod
    def model():
        return FlowLinkInfo


def init_creations():
    revison, created = Revision.objects.get_or_create(
        defaults={
            "date_created": timezone.now(),
        },
        **{"id": 1}
    )
    if not created:
        return
    models = [FlowNodeInfo, FlowLinkInfo]
    for model in models:
        for obj in model.objects.all():
            version_options = _get_options(obj.__class__)
            content_type = _get_content_type(obj.__class__, "default")
            object_id = force_text(obj.pk)
            a = Version(
                content_type=content_type,
                object_id=object_id,
                db="default",
                format=version_options.format,
                serialized_data=serializers.serialize(
                    version_options.format,
                    (obj,),
                    fields=version_options.fields,
                ),
                object_repr=force_text(obj),
                revision_id=1,
            )
            try:
                a.save()
            except Exception:
                pass
    for flow_id in FlowInfo.objects.all().values_list("flow_id", flat=True):
        version_id = FlowVersionLog.gene_version()
        components = []
        for version in Version.objects.all():
            if version.field_dict["flow_id"] == flow_id:
                components.append(str(version.pk))
        s_components = ",".join(components)
        FlowVersionLog.objects.create(
            flow_id=flow_id,
            flow_version=version_id,
            version_ids=s_components,
            created_by="init",
            description="",
        )
        FlowInfo.objects.filter(flow_id=flow_id).update(version=version_id)


# using='dataflow'
def flow_version_wrap(handlers, using="default"):
    def _wrap(func):
        @wraps(func, assigned=available_attrs(func))
        def _deco(self, *args, **kwargs):
            # 从线程变量获取用户名
            username = get_request_username()
            try:
                flow_id = int(kwargs.get("flow_id", None))
            except KeyError:
                flow_id = int(kwargs.get("pk", None))

            _local = _Local()
            _push_frame(_local, False, using)
            signal_handlers = [handler.update(_local, flow_id) for handler in handlers]
            # _send_models_signals(signal_handlers)
            try:
                # 增加/删除节点/连线的操作由signal捕捉，并将待创建的Version对象放在栈中，节点对象放在sinal_handlers中
                run_func = func(self, *args, **kwargs)
            except RevisionManagementError as e:
                logger.exception("[flow_version]: %s" % e)
            else:
                try:
                    save_revision(_local, using)
                    update_version(signal_handlers, flow_id, username)
                except Exception as e:
                    logger.exception("[flow_version]version record error: %s" % e)
                finally:
                    _pop_frame(_local)
                    return run_func

        return _deco

    return _wrap


def _send_models_signals(handlers):
    for handler in handlers:
        if not is_registered(handler.model()):
            my_register(handler.model())

        for signal in handler.signals:
            _live_receivers = signal["signal"].receivers
            _live_str_receivers = [s[0][0] for s in _live_receivers]
            uid = str(signal["signal"]) + handler.__unicode__()
            if uid not in _live_str_receivers:
                signal["signal"].connect(signal["receiver"], sender=handler.model(), dispatch_uid=uid)


def _disconnect_signal(handlers):
    for handler in handlers:
        for signal in handler.signals:
            signal["signal"].disconnect(signal["receiver"], sender=handler.model())


def my_register(model):
    # Prevent multiple registration.
    if is_registered(model):
        raise RegistrationError(
            "{model} has already been registered with django-reversion".format(
                model=model,
            )
        )
    # Parse fields.
    opts = model._meta.concrete_model._meta
    version_options = _VersionOptions(
        fields=tuple(
            field_name for field_name in ([field.name for field in opts.local_fields + opts.local_many_to_many])
        ),
        follow=tuple(()),
        format="json",
        for_concrete_model=True,
        ignore_duplicates=False,
    )
    # Register the model.
    _registered_models[_get_registration_key(model)] = version_options


def save_revision(_local, using):
    if not any(using in frame.db_versions for frame in _local.stack[:-1]):
        current_frame = _current_frame(_local)
        _save_revision(
            versions=list(current_frame.db_versions[using].values()),
            user=current_frame.user,
            comment=current_frame.comment,
            meta=current_frame.meta,
            date_created=current_frame.date_created,
            using=using,
        )


def update_version(signal_handlers, flow_id, operator):
    is_changed = 0
    # 获取最近版本下的所有版本节点id列表
    current_component_ids = FlowVersionLog.list_component_ids(flow_id=flow_id)
    for handler in signal_handlers:
        for _k, _v in list(handler.flows.items()):
            if flow_id == _k:
                is_changed += handler.flows[_k]["changed_num"]
                for component in handler.flows[_k]["add_components"]:
                    # 对应创建操作，保存版本信息前需先生成Version记录
                    version_objects = Version.objects.get_for_object(component)
                    if len(version_objects) > 0:
                        _id = str(version_objects[0].id)
                        current_component_ids.append(_id)
                for delete_id in handler.flows[_k]["delete_component_ids"]:
                    # 对于删除操作，更新版本信息前不需更新或生成Version记录
                    if delete_id in current_component_ids:
                        current_component_ids.remove(delete_id)
                handler.clear(_k)
    if is_changed:
        # 若flow重要信息发生改变，一般是节点，需要更新flow的latest_version，同时
        # 1. 若节点发生改变，需更新节点的latest_version
        # 2. 创建一条FlowVersionLog记录
        current_component_ids = sorted(set(current_component_ids))
        # TODO: current_component_ids改造，使之包含当前所有component
        s_components = ",".join([_f for _f in current_component_ids if _f])
        # 最近一条不同时，才创建版本
        components = None
        components_list = FlowVersionLog.objects.filter(flow_id=flow_id).order_by("-id")
        if components_list:
            components = components_list[0].version_ids
        # 当前版本信息与经过排序的版本信息不同，需要创建版本号，并
        if s_components != components:
            generate_version(operator, flow_id, s_components)


def generate_version(operator, flow_id, components=None):
    components = _get_current_components(flow_id, components)
    version = FlowVersionLog.gene_version()
    with auto_meta_sync(using="default"):
        # 如果flow发生改变，则创建新版本
        # TODO：否则表明当前版本控制数据发生异常，如某个环节未创建版本
        # TODO: 私以为版本控制应每次都更新所有current_component_ids元素，而不是用append或remove等操作
        if components is not None:
            FlowVersionLog.objects.create(
                flow_id=flow_id,
                flow_version=version,
                version_ids=components,
                created_by=operator,
                description="",
            )
        else:
            # 为保证任务不因为版本控制出错导致无法生成一致的版本号，这里确保二者版本保号保持一致
            components_list = FlowVersionLog.objects.filter(flow_id=flow_id).order_by("-id")
            if components_list:
                components_list[0].update(flow_version=version)
        # 更改节点和连线后都会更新 flow 的 latest_version，并在启动时更新到 flow_instance
        FlowInfo.objects.filter(flow_id=flow_id).update(
            latest_version=version, updated_by=operator, updated_at=datetime.now()
        )
        return version


def _get_current_components(flow_id, components):
    """
    获取当前记录版本的节点版本id列表
    @param flow_id:
    @param components:
    @return: node_id1,node_id2
    """
    if components is None:
        components_list = FlowVersionLog.objects.filter(flow_id=flow_id).order_by("-id")
        # 若存在旧版本才可取
        # TODO：避免版本控制的数据库无数据异常导致无法启动
        if components_list:
            components = components_list[0].version_ids
    return components


def is_active(_local):
    return bool(_local.stack)


def _current_frame(_local):
    if not is_active(_local):
        raise RevisionManagementError("There is no active revision for this thread")
    return _local.stack[-1]


def _copy_db_versions(db_versions):
    return {db: versions.copy() for db, versions in list(db_versions.items())}


def _push_frame(_local, manage_manually, using):
    if is_active(_local):
        current_frame = _current_frame(_local)
        db_versions = _copy_db_versions(current_frame.db_versions)
        db_versions.setdefault(using, {})
        stack_frame = current_frame._replace(
            manage_manually=manage_manually,
            db_versions=db_versions,
        )
    else:
        stack_frame = _StackFrame(
            manage_manually=manage_manually,
            user=None,
            comment="",
            date_created=timezone.now(),
            db_versions={using: {}},
            meta=(),
        )
    _local.stack += (stack_frame,)


def _update_frame(_local, **kwargs):
    _local.stack = _local.stack[:-1] + (_current_frame(_local)._replace(**kwargs),)


def _pop_frame(_local):
    prev_frame = _current_frame(_local)
    _local.stack = _local.stack[:-1]
    if is_active(_local):
        current_frame = _current_frame(_local)
        db_versions = {db: prev_frame.db_versions[db] for db in list(current_frame.db_versions.keys())}
        _update_frame(
            _local,
            user=prev_frame.user,
            comment=prev_frame.comment,
            date_created=prev_frame.date_created,
            db_versions=db_versions,
            meta=prev_frame.meta,
        )


def _follow_relations(obj):
    version_options = _get_options(obj.__class__)
    for follow_name in version_options.follow:
        try:
            follow_obj = getattr(obj, follow_name)
        except ObjectDoesNotExist:
            continue
        if isinstance(follow_obj, models.Model):
            yield follow_obj
        elif isinstance(follow_obj, (models.Manager, QuerySet)):
            for follow_obj_instance in follow_obj.all():
                yield follow_obj_instance
        elif follow_obj is not None:
            raise RegistrationError(
                "{name}.{follow_name} should be a Model or QuerySet".format(
                    name=obj.__class__.__name__,
                    follow_name=follow_name,
                )
            )


def _follow_relations_recursive(obj):
    def do_follow(obj):
        if obj not in relations:
            relations.add(obj)
            for related in _follow_relations(obj):
                do_follow(related)

    relations = set()
    do_follow(obj)
    return relations


def _add_to_revision(_local, obj, using, model_db, explicit):
    from reversion.models import Version

    version_options = _get_options(obj.__class__)
    content_type = _get_content_type(obj.__class__, using)
    object_id = force_text(obj.pk)
    version_key = (content_type, object_id)
    # If the obj is already in the revision, stop now.
    db_versions = _current_frame(_local).db_versions
    versions = db_versions[using]
    if version_key in versions and not explicit:
        return
    # Get the version data.
    version = Version(
        content_type=content_type,
        object_id=object_id,
        db=model_db,
        format=version_options.format,
        serialized_data=serializers.serialize(
            version_options.format,
            (obj,),
            fields=version_options.fields,
        ),
        object_repr=force_text(obj),
    )
    # If the version is a duplicate, stop now.
    if version_options.ignore_duplicates and explicit:
        previous_version = Version.objects.using(using).get_for_object(obj, model_db=model_db).first()
        if previous_version and previous_version._local_field_dict == version._local_field_dict:
            return
    # Store the version.
    db_versions = _copy_db_versions(db_versions)
    db_versions[using][version_key] = version
    _update_frame(_local, db_versions=db_versions)
    # Follow relations.
    for follow_obj in _follow_relations(obj):
        _add_to_revision(_local, follow_obj, using, model_db, False)


def add_to_revision(_local, obj, model_db=None):
    model_db = model_db or router.db_for_write(obj.__class__, instance=obj)
    for db in list(_current_frame(_local).db_versions.keys()):
        _add_to_revision(_local, obj, db, model_db, True)


def get_version_model(version):
    """
    获取version对象保存的object对应的model信息
    通过调试源码发现：
        由于当前flow属于dataflow之下的子app，在执行apps.get_models('dataflow.flow')时会报错返回None，导致
        version._model为None
    @param version:
    @return:
    """
    return apps.all_models[version.content_type.app_label][version.content_type.model]


def _save_revision(versions, user=None, comment="", meta=(), date_created=None, using=None):
    from reversion.models import Revision

    # Only save versions that exist in the database.
    # {
    #   ModelType: {
    #       'db_name1': set([pk1,pk2])
    #   }
    # }
    model_db_pks = defaultdict(lambda: defaultdict(set))
    for version in versions:
        model_class = get_version_model(version)
        model_db_pks[model_class][version.db].add(version.object_id)
    model_db_existing_pks = {
        model: {
            db: frozenset(
                list(
                    map(
                        force_text,
                        model._default_manager.using(db).filter(pk__in=pks).values_list("pk", flat=True),
                    )
                )
            )
            for db, pks in list(db_pks.items())
        }
        for model, db_pks in list(model_db_pks.items())
    }
    versions = [
        version
        for version in versions
        if version.object_id in model_db_existing_pks[get_version_model(version)][version.db]
    ]
    # Bail early if there are no objects to save.
    if not versions:
        return
    # Save a new revision.
    revision = Revision(
        date_created=date_created,
        user=user,
        comment=comment,
    )
    # Send the pre_revision_commit signal.
    pre_revision_commit.send(
        sender=create_revision,
        revision=revision,
        versions=versions,
    )
    # Save the revision.
    revision.save(using=using)
    # Save version models.
    for version in versions:
        version.revision = revision
        version.save(using=using)
    # Save the meta information.
    for meta_model, meta_fields in meta:
        meta_model._default_manager.db_manager(using=using).create(revision=revision, **meta_fields)
    # Send the post_revision_commit signal.
    post_revision_commit.send(
        sender=create_revision,
        revision=revision,
        versions=versions,
    )


@contextmanager
def _dummy_context():
    yield


@contextmanager
def _create_revision_context(_local, manage_manually, using, atomic):
    _push_frame(_local, manage_manually, using)
    try:
        context = transaction.atomic(using=using) if atomic else _dummy_context()
        with context:
            yield
            # Only save for a db if that's the last stack frame for that db.
            if not any(using in frame.db_versions for frame in _local.stack[:-1]):
                current_frame = _current_frame(_local)
                _save_revision(
                    versions=list(current_frame.db_versions[using].values()),
                    user=current_frame.user,
                    comment=current_frame.comment,
                    meta=current_frame.meta,
                    date_created=current_frame.date_created,
                    using=using,
                )

    finally:
        _pop_frame(_local)


def create_revision(_local, manage_manually=False, using=None, atomic=True):
    from reversion.models import Revision

    using = using or router.db_for_write(Revision)
    return _ContextWrapper(_create_revision_context, (_local, manage_manually, using, atomic))


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
        def do_revision_context(*args, **kwargs):
            with self._func(*self._args):
                return func(*args, **kwargs)

        return do_revision_context


def is_manage_manually(_local):
    return _current_frame(_local).manage_manually


def _post_save_receiver(_local, sender, instance, using, **kwargs):
    if is_registered(sender) and is_active(_local) and not is_manage_manually(_local):
        add_to_revision(_local, instance, model_db=using)


def _get_registration_key(model):
    return (model._meta.app_label, model._meta.model_name)


_registered_models = {}


def is_registered(model):
    return _get_registration_key(model) in _registered_models
