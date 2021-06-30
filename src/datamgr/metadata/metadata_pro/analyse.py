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

from metadata.util.common import StrictABCMeta


class AnalyseComponent(object, metaclass=StrictABCMeta):
    """
    分析组件父类。
    """

    __abstract__ = True

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)


class AnalyseProcedure(AnalyseComponent):
    """
    单元分析流程父类。所有分析流程都继承此父类。
    """

    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super(AnalyseProcedure, self).__init__()
        self.input, self.output = None, None

    def execute(self, _input=None, *args, **kwargs):
        if _input:
            self.input = _input
        return args, kwargs


class AnalyseFlow(AnalyseProcedure):
    """
    整体分析流程。该类流程直接面向业务，包含业务数据预处理，缓存，返回等。
    """

    __abstract__ = True
