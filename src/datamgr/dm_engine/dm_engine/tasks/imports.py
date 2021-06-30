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

summary:
    Configure task.task_loader=import, the ImportTaskCls must be inherited. As example:

    class FlyTask(ImportTaskCls):

        def run(self, params):
            print('Start to execute: {}'.format(params))
"""

from abc import ABCMeta, abstractmethod
from functools import wraps
from typing import Dict, Optional


class ImportTaskCls(metaclass=ABCMeta):
    def __init__(self):
        pass

    @abstractmethod
    def run(self, params: Optional[Dict] = None):
        """
        Entry function. It must be override and fill in self-defined execute code.

        :param params: It's from task.params of the configuration
        :type params: dict
        """
        pass


def import_task_deco(raw_func):
    """
    Decorator to fill in dm_task functions, make dm_engine framework can communicate with it.
    """

    def deco(params: Optional[Dict] = None):
        if raw_func.__code__.co_argcount >= 1:
            raw_func(params)
        else:
            raw_func()

    return wraps(raw_func)(deco)
