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

from abc import ABCMeta, abstractmethod
from enum import Enum


class LineageDirection(Enum):
    BOTH = 'BOTH'
    INPUT = 'INPUT'
    OUTPUT = 'OUTPUT'


class BackendType(Enum):
    """
    应用层后端分类
    """

    DGRAPH = 'dgraph'
    MYSQL = 'mysql'
    CONFIG_DB = 'config_db'
    DGRAPH_BACKUP = 'dgraph_backup'
    DGRAPH_COLD = 'dgraph_cold'


class RawBackendType(Enum):
    """
    底层后端分类
    """

    DGRAPH = 'dgraph'
    MYSQL = 'mysql'


BackendType.DGRAPH_COLD.raw = RawBackendType.DGRAPH
BackendType.DGRAPH_BACKUP.raw = RawBackendType.DGRAPH
BackendType.DGRAPH.raw = RawBackendType.DGRAPH
BackendType.MYSQL.raw = RawBackendType.MYSQL
BackendType.CONFIG_DB.raw = RawBackendType.MYSQL


class BackendSessionHub(object, metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, uri, pool_size, pool_maxsize, **kwargs):
        self.pool_maxsize = pool_maxsize
        self.pool_size = pool_size
        self.uri = uri


class Backend(object, metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, session_hub, cache_manager=None, **kwargs):
        self.session_hub = session_hub
        self.cache_manager = cache_manager

    @abstractmethod
    def operate_session(self, in_local=None):
        pass


class BackendOperator(object, metaclass=ABCMeta):
    def __init__(self, session, cache_manager=None, **kwargs):
        self.session = session
        self.cache_manager = cache_manager

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

    @abstractmethod
    def operate(self, *args, **kwargs):
        pass
