# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import uuid
from threading import local

from apps.exceptions import DataError

_local = local()


def activate_request(request, request_id=None):
    """
    激活request线程变量
    """
    if not request_id:
        request_id = str(uuid.uuid4())
    request.request_id = request_id
    _local.request = request
    return request


def get_request():
    """
    获取线程请求request
    """
    try:
        return _local.request
    except AttributeError:
        raise DataError("request thread error!")


def get_request_id():
    """
    获取request_id
    """
    try:
        return get_request().request_id
    except DataError:
        return str(uuid.uuid4())
    # request_id = getattr(_local, "request_id", None)
    # if request_id is not None:
    #     return request_id
    # else:
    #     _local.request_id = str(uuid.uuid4())
    #     return _local.request_id
