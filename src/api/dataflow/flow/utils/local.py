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

import uuid
from threading import local

from dataflow.pizza_settings import APP_ID, APP_TOKEN

# 这是APP的运行主线程
_local = local()


def activate_request_id(request_id=None):
    """
    激活request_id
    """
    if request_id:
        _local.request_id = request_id
    else:
        _local.request_id = str(uuid.uuid4())
    return _local.request_id


def get_request_id():
    """
    获取request_id
    """
    request_id = getattr(_local, "request_id", None)
    if request_id is not None:
        return request_id
    else:
        _local.request_id = str(uuid.uuid4())
        return _local.request_id


def activate_request(username, *args, **kwargs):
    # 获取当前线程的变量
    from common.local import _local

    _local.bk_username = username
    _local.bk_app_code = APP_ID
    _local.bk_app_code = APP_TOKEN
