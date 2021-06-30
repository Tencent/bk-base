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

from __future__ import print_function

import datetime
import sys

from bkbase.dataflow.metrics.reporter.reporter import Reporter

"""
仅供控制台调试使用
"""


class _ConsoleReporter(Reporter):
    def __init__(self, stream=sys.stderr):
        super(_ConsoleReporter, self).__init__()
        self.stream = stream

    # 负载原函数
    def report_now(self, registry_map=None):
        # 在遍历的时候, 不能修改字典元素, 因此将字典改为列表
        for key in list(registry_map.keys()):
            registry = registry_map[key]
            metrics = registry.dump_metrics()
            time_stamp = datetime.datetime.now()
            cur_time = time_stamp.strftime("%Y:%m:%d %H:%M:%S")
            print(key, cur_time, metrics, file=self.stream)
            # 判断是否上报结束, 若结束, 从 map 中移除
            if registry.is_end():
                self.remove(key)


# 单例对象
console_reporter = _ConsoleReporter()
