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
from threading import Lock


class Counter(object):
    """
    An incrementing and decrementing metric
    """

    def __init__(self):
        super(Counter, self).__init__()
        self._lock = Lock()
        self._counter = 0
        # Counter 是否清零,每次上报后数值清零, 默认都清零
        self._reset = True
        # seq_no 每次上报是否自增, 默认不自增
        self._auto_increment = False

    def inc(self, val=1):
        "increment counter by val (default is 1)"
        with self._lock:
            self._counter = self._counter + val

    def dec(self, val=1):
        "decrement counter by val (default is 1)"
        self.inc(-val)

    def get_count(self):
        "return current value of counter"
        return self._counter

    def clear(self):
        "reset counter to 0"
        with self._lock:
            self._counter = 0

    def set_reset(self, reset):
        self._reset = reset

    def set_auto_increment(self, auto_increment):
        self._auto_increment = auto_increment

    @property
    def reset(self):
        return self._reset

    @property
    def auto_increment(self):
        return self._auto_increment
