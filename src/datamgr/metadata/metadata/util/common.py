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
import json
import re
from abc import ABCMeta
from collections import MutableMapping
from json import JSONEncoder

import jsonext

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def camel_to_snake(name):
    """将驼峰式命名变更为下划线小写命名。"""
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def snake_to_camel(snake_str, apply_to_first=False):
    """将标准Python变量名转换为驼峰式命名。"""
    components = snake_str.split('_')
    first_component = components[0] if not apply_to_first else components[0].title()
    return first_component + ''.join(x.title() for x in components[1:])


def version_compare(v1, v2, sep='.'):
    """比较两个版本号大小,判断是否v1>v2[-1:v1小;0:相等;1-v1大]"""
    t1 = tuple(int(val) if val.isdigit() else 0 for val in str(v1).split(sep))
    t2 = tuple(int(val) if val.isdigit() else 0 for val in str(v2).split(sep))
    if t1 == t2:
        return 0
    return 1 if t1 > t2 else -1


class StrictABCMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        """
        严格的抽象元类。除了ABCMeta本身功能，提供增强功能：禁止显式抽象类(__abstract__属性为True)实例化。
        """
        cls = super(StrictABCMeta, mcs).__new__(mcs, name, bases, namespace)

        if not namespace.get('__abstract__'):
            cls.__abstract__ = False

        # 底层抽象类, 不允许被实例化
        if cls.__abstract__ and (True not in [getattr(base, '__abstract__', False) for base in bases]):

            def new(now_cls, *args, **kwargs):
                """
                如果底层抽象类直接继承于object，new函数不能传递参数(object的__init__函数不接受参数)
                """
                if getattr(now_cls, '__abstract__', False):
                    raise TypeError("Can't instantiate explicit abstract class.")
                if [base.__name__ for base in bases] == ['object']:
                    return super(cls, now_cls).__new__(now_cls)
                return super(cls, now_cls).__new__(now_cls, *args, **kwargs)

        # 非抽象类或抽象类的衍生类
        else:

            def new(now_cls, *args, **kwargs):
                """
                抽象类的衍生类new函数重新定义，均可以传递参数
                """
                if getattr(now_cls, '__abstract__', False):
                    raise TypeError("Can't instantiate explicit abstract class.")
                return super(cls, now_cls).__new__(now_cls, *args, **kwargs)

        cls.__new__ = staticmethod(new)

        return cls


class RawMappingProxy(MutableMapping):
    def __init__(self, *args, **kwargs):
        self.raw = dict()
        self.raw.update(*args, **kwargs)

    def __setitem__(self, k, v):
        return self.raw.__setitem__(k, v)

    def __delitem__(self, k):
        return self.raw.__delitem__(k)

    def __getitem__(self, k):
        return self.raw.__getitem__(k)

    def __iter__(self):
        return self.raw.__iter__()

    def __len__(self):
        return self.raw.__len__()

    def __repr__(self):
        return self.raw.__repr__()

    def __str__(self):
        return self.raw.__str__()


class CamelKeyDict(RawMappingProxy):
    def __setitem__(self, k, v):
        k = snake_to_camel(k) if "_" in k else k
        return self.raw.__setitem__(k, v)


class SnakeKeyDict(RawMappingProxy):
    def __setitem__(self, k, v):
        return self.raw.__setitem__(camel_to_snake(k), v)


class CustomJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(
            o,
            (SnakeKeyDict, CamelKeyDict),
        ):
            dct = {}
            dct.update(o)
            return dct

        return super(CustomJSONEncoder, self).default(o)


def jsonext_patch():
    class CustomedJSONEncoder(CustomJSONEncoder, jsonext.JSONEncoder):
        pass

    jsonext.JSONEncoder = CustomedJSONEncoder
    jsonext.dumps = functools.partial(json.dumps, cls=jsonext.JSONEncoder)


class _Empty(object):
    def __bool__(self):
        return False


Empty = _Empty()


class _Temp(object):
    pass


Temp = _Temp()


def convert_string_template(string):
    _ = _Temp()
    _.index = -1

    def repl(matched):
        keyword = matched.group(1)
        if keyword:
            return "{%s}" % keyword.strip('()')
        else:
            _.index += 1
            return "{%d}" % _.index

    return re.sub(r'(?:%(\(\w+\))?[diouxXeEfFgGcrs])', repl, string)


class abstractclassmethod(classmethod):
    __isabstractmethod__ = True

    def __init__(self, callable):
        callable.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(callable)


def error_adapt_handler(logger, orgi_exc=Exception, exc=Exception, msg=None):
    def _wrapper(func):
        def _func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except orgi_exc as e:
                logger.exception('Some error occurred: {}'.format(e))
                raise exc()

        return _func

    return _wrapper
