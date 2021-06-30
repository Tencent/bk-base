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

import logging
from abc import abstractmethod
from functools import wraps

from metadata.backend.interface import BackendType
from metadata.exc import LayerError
from metadata.runtime import rt_context
from metadata.util.common import StrictABCMeta


class BaseAccessLayersGroup(object, metaclass=StrictABCMeta):
    """
    访问层合集基类。
    """

    __abstract__ = True

    def __init__(
        self,
        layers_group_storage,
        backends_group_storage,
    ):
        """
        访问层合集初始化。

        :param layers_group_storage: 访问层合集字典。
        :param backends_group_storage:  后端合集字典。
        """
        self.backends_group_storage = backends_group_storage
        self.layers_group_storage = layers_group_storage
        self.available_backend_names = [
            name for name, status in rt_context.config_collection.normal_config.AVAILABLE_BACKENDS.items() if status
        ]
        for backend_name in self.available_backend_names:
            backend = self.backends_group_storage[BackendType(backend_name)]
            setattr(self, '{}_backend'.format(backend_name), backend)
        for layer_name in layers_group_storage:
            setattr(self, layer_name, layers_group_storage[layer_name](self))
        self.logger = logging.getLogger(self.__class__.__name__)


class BaseAccessLayer(object, metaclass=StrictABCMeta):
    """
    访问层基类。
    """

    __abstract__ = True

    def __init__(self, layers_group):
        """
        访问层初始化。

        :param layers_group: 访问层合集实例。
        """
        self.layers_group = layers_group
        self.backends_group_storage = self.layers_group.backends_group_storage
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_collection = rt_context.config_collection
        self.normal_conf = self.config_collection.normal_config


class CommonAccessLayerInterface(object):
    """
    一般访问层接口规范。
    """

    @abstractmethod
    def query(self, *args, **kwargs):
        pass

    @abstractmethod
    def create(self, *args, **kwargs):
        pass

    @abstractmethod
    def update(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        pass


def check_backend(*o_args):
    allowed_backends = o_args

    def _check_backend(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            available_backends_info = rt_context.config_collection.normal_config.AVAILABLE_BACKENDS
            for backend in allowed_backends:
                if not available_backends_info.get(backend.value, False):
                    raise LayerError('The depended backend  is not available.')
            return func(*args, **kwargs)

        return wrapper

    return _check_backend
