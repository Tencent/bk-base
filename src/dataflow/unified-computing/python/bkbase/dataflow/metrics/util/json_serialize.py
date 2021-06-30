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
import json
from threading import Lock

from bkbase.dataflow.metrics.meter.counter import Counter
from bkbase.dataflow.metrics.meter.literal import Literal


class ObjectEncoder(json.JSONEncoder):
    """
    Json序列化不能序列化对象
    序列化的同时对上报数据中的 counter 进行操作,判断是否需要重置或者自增
    """

    def default(self, obj):
        if isinstance(obj, Literal):
            return obj.get_literal()
        elif isinstance(obj, Counter):
            count = obj.get_count()
            # 在上报修改的同时, 不允许其它线程修改
            with Lock():
                # 上报完后重置 Counter 为 0
                if obj.reset:
                    obj._counter = 0
                # 上报完后 Counter 自增
                if obj.auto_increment:
                    obj.inc(1)
            return count
        else:
            return json.JSONEncoder.default(self, obj)
