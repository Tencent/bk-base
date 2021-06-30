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
from functools import wraps
from io import BytesIO

from line_profiler import LineProfiler, show_text

module_logger = logging.getLogger(__name__)


def profile_by_line(func=None, modules_to_trace=None, funcs_to_trace=None):
    if funcs_to_trace is None:
        funcs_to_trace = []
    if modules_to_trace is None:
        modules_to_trace = []

    def _profile_by_line(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            lp = LineProfiler()
            try:
                lp.add_function(func)
                for item in funcs_to_trace:
                    lp.add_function(item)
                for item in modules_to_trace:
                    lp.add_module(item)
                lp.enable()
                ret = func(*args, **kwargs)
                lp.disable()
                lstat = lp.get_stats()
                io = BytesIO(b'There is profile info:')
                show_text(lstat.timings, lstat.unit, stream=io)
                content = io.getvalue().decode('utf8')
                module_logger.info(content)
            except Exception:
                lp.disable_by_count()
                raise
            return ret

        return _wrapper

    if func:
        return _profile_by_line(func)
    else:
        return _profile_by_line
