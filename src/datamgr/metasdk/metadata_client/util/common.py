# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有


from __future__ import absolute_import, print_function, unicode_literals

import functools
import json
import re
from abc import ABCMeta
from collections import MutableMapping
from json import JSONEncoder

import jsonext

first_cap_re = re.compile("(.)([A-Z][a-z]+)")
all_cap_re = re.compile("([a-z0-9])([A-Z])")


def camel_to_snake(name):
    """将驼峰式命名变更为下划线小写命名。"""
    s1 = first_cap_re.sub(r"\1_\2", name)
    return all_cap_re.sub(r"\1_\2", s1).lower()


def snake_to_camel(snake_str, apply_to_first=False):
    """将标准Python变量名转换为驼峰式命名。"""
    components = snake_str.split("_")
    first_component = components[0] if not apply_to_first else components[0].title()
    return first_component + "".join(x.title() for x in components[1:])


class StrictABCMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        """
        严格的抽象元类。除了ABCMeta本身功能，提供增强功能：禁止显式抽象类(__abstract__属性为True)实例化。
        """
        cls = super(StrictABCMeta, mcs).__new__(mcs, name, bases, namespace)

        if not namespace.get("__abstract__"):
            cls.__abstract__ = False

        if cls.__abstract__ and (True not in [getattr(base, "__abstract__", False) for base in bases]):

            def new(now_cls, *args, **kwargs):
                """
                若类被标注为显式抽象类(__abstract__属性为True)，则无法实例化。
                """
                if getattr(now_cls, "__abstract__", False):
                    raise TypeError("Can't instantiate explicit abstract class.")
                return super(cls, now_cls).__new__(now_cls, *args, **kwargs)

            cls.__new__ = staticmethod(new)

        return cls


class abstractclassmethod(classmethod):
    __isabstractmethod__ = True

    def __init__(self, callable):
        callable.__isabstractmethod__ = True
        super(abstractclassmethod, self).__init__(callable)


class _Empty(object):
    def __nonzero__(self):
        return False


Empty = _Empty()
