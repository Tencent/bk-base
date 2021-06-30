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

import time
from functools import wraps

from django.db.utils import OperationalError
from django.utils.decorators import available_attrs

from dataflow.flow.settings import RETRY_DB_OPERATION_MAX_COUNT
from dataflow.shared.log import flow_logger as logger


def ignore_exception(func):
    """
    忽略函数执行的异常处理
    """

    @wraps(func, assigned=available_attrs(func))
    def _wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.error(e)

    return _wrap


def retry_db_operation_on_exception(func):
    @wraps(func, assigned=available_attrs(func))
    def _wrap(*args, **kwargs):
        attempt_count = 0
        while True:
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                logger.exception(e)
                if attempt_count <= RETRY_DB_OPERATION_MAX_COUNT:
                    attempt_count += 1
                    time.sleep(attempt_count / 10.0)
                else:
                    logger.exception("raise OperationalError after retrying %s times" % attempt_count)
                    raise e

    return _wrap
