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

import requests
from common.log import logger
from datahub.common.const import JSON_HEADERS

headers = JSON_HEADERS


# TODO pizza common包中已经有了，没必要这里重复写一遍
def _http_request(method, url, headers=None, data=None, timeout=60, auth=()):
    _start_time = int(time.time() * 1000)
    try:
        if method == "GET":
            resp = requests.get(url=url, headers=headers, params=data, timeout=timeout, auth=auth)
        elif method == "HEAD":
            resp = requests.head(url=url, headers=headers, auth=auth)
        elif method == "POST":
            resp = requests.post(url=url, headers=headers, json=data, timeout=timeout, auth=auth)
        elif method == "DELETE":
            resp = requests.delete(url=url, headers=headers, json=data, auth=auth)
        elif method == "PUT":
            resp = requests.put(url=url, headers=headers, json=data, auth=auth)
        else:
            return False, None
    except Exception:
        logger.error(f"http request error! type: {method}, url: {url}, data: {str(data)}", exc_info=True)
        return False, None
    else:
        # if no exception,get here
        if resp.status_code != 200:
            logger.error(
                f"http request error! type: {method}, url: {url}, data: {str(data)}, "
                f"response_status_code: {resp.status_code}, response_content: {resp.content}"
            )
            if resp.content is not None:
                return False, resp.content
            return False, None
        logger.debug(f"http_request|success|{method}|{url}|{str(data)}|{int(time.time() * 1000 - _start_time)}|")
        try:
            return True, resp.json()
        except Exception:
            return True, resp.content


def get(url, params=None, timeout=60, auth=()):
    return _http_request(method="GET", url=url, headers=headers, data=params, timeout=timeout, auth=auth)


def post(url, params=None, timeout=60, auth=()):
    if params is None:
        params = {}
    return _http_request(method="POST", url=url, headers=headers, data=params, timeout=timeout, auth=auth)


def delete(url, params=None, timeout=60, auth=()):
    if params is None:
        params = {}
    return _http_request(method="DELETE", url=url, headers=headers, data=params, timeout=timeout, auth=auth)


def put(url, params=None, timeout=60, auth=()):
    if params is None:
        params = {}
    return _http_request(method="PUT", url=url, headers=headers, data=params, timeout=timeout, auth=auth)
