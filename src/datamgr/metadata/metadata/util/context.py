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

from copy import copy, deepcopy
from functools import wraps

from lazy_import import lazy_module
from werkzeug.local import Local, LocalManager


class SimpleContext(object):
    """
    简易上下文数据类。
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class CompositeContext(object):
    """
    复合上下文数据类。按照contexts_lst所列顺序检索子上下文属性。本身不支持添加属性。
    """

    __slots__ = ('contexts', 'managers', 'g', 'local')

    def __init__(self, *args, **kwargs):
        self.contexts = args
        self.managers = kwargs[str('managers')] if kwargs.get(str('managers')) else {}
        self.g = kwargs[str('default_g')] if kwargs.get(str('default_g')) else {}
        self.local = kwargs[str('default_local')] if kwargs.get(str('default_local')) else {}

    def __getattr__(self, item):
        for proxy in self.contexts:
            try:
                return getattr(proxy, item)
            except AttributeError:
                continue
        else:
            raise AttributeError


def inherit_local_ctx(func, local, local_manager, deep_copy=False, variables=None):
    """
    用于嵌套并发环境下，传递父级本地变量合集。
    :param local_manager: 本地存储管理器。
    :param deep_copy: 是否使用深拷贝。
    :param func: 并发执行的函数
    :param local: 本地变量存储
    :param variables: 可选需要传递的变量
    :return:
    """
    l_dct = local.__storage__.get(local.__ident_func__())
    i_dct = {k: v for k, v in l_dct.items() if k in variables} if variables else l_dct
    if not deep_copy:
        i_dct = copy(i_dct)
    else:
        i_dct = deepcopy(i_dct)

    @wraps(func)
    def _func(*args, **kwargs):
        local.__storage__[local.__ident_func__()] = i_dct
        ret = func(*args, **kwargs)
        local_manager.cleanup()
        return ret

    return _func


def check_resource_load_eligibility(context):
    if not getattr(context, 'resource_load_eligibility', False):
        raise RuntimeError("Resource load eligibility doesn't match.")


def set_resource_load_eligibility(g):
    setattr(g, 'resource_load_eligibility', True)


_run_time_contexts = {}


def get_runtime_context(context_name):
    if not _run_time_contexts.get(context_name):
        g = SimpleContext()
        local = Local()
        local_manager = LocalManager(
            locals=[local],
        )
        context = CompositeContext(local, g, managers={local: local_manager}, default_local=local, default_g=g)
        _run_time_contexts[context_name] = context
    return _run_time_contexts[context_name]


def load_common_resource(rt_g, cc):
    rt_g.m_resource = lazy_module(str('metadata.resource'))
    rt_g.m_biz_resource = lazy_module(str('metadata_biz.resource'))
    biz_types = lazy_module(str('metadata_biz.types'))
    rt_g.md_types_registry = biz_types.default_registry
    rt_g.config_collection = cc
