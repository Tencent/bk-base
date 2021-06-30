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


class Topology(metaclass=ABCMeta):
    def __init__(self, builder):
        self.job_id = builder.job_id
        self.user_args = builder.user_args
        self.source_nodes = None
        self.sink_nodes = None
        self.user_main_module = builder.user_main_module
        self.user_main_class = builder.user_main_class
        self.engine_conf = builder.engine_conf

    @abstractmethod
    def map_nodes(self, type_tables):
        NotImplementedError("Not implement builder build method")


class Builder(metaclass=ABCMeta):
    def __init__(self, params):
        self.params = params
        self.job_id = self.params["job_id"]
        self.user_args = self.params["user_args"]
        self.engine_conf = self.params["engine_conf"]
        main_class_array = self.params["user_main_class"].rsplit(".", 1)
        self.user_main_class = main_class_array[1]
        self.user_main_module = main_class_array[0]

    @abstractmethod
    def build(self):
        NotImplementedError("Not implement builder build method")
