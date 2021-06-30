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

from .log import sys_logger


def _http_request(method, url, headers=None, data=None):
    _start_time = int(time.time() * 1000)
    try:
        if method == "GET":
            resp = requests.get(url=url, headers=headers, params=data)
        elif method == "HEAD":
            resp = requests.head(url=url, headers=headers)
        elif method == "POST":
            resp = requests.post(url=url, headers=headers, json=data)
        elif method == "DELETE":
            resp = requests.delete(url=url, headers=headers, json=data)
        elif method == "PUT":
            resp = requests.put(url=url, headers=headers, json=data)
        else:
            return False, None
    except requests.exceptions.RequestException:
        sys_logger.exception("http request error! type: {}, url: {}, data: {}".format(method, url, str(data)))
        return False, None
    else:
        if resp.status_code != 200:
            sys_logger.error(
                "http request error! type: %s, url: %s, data: %s, response_status_code: %s, response_content: %s"
                % (method, url, str(data), resp.status_code, resp.content)
            )
            return False, None
        sys_logger.info(
            "http_request|success|%s|%s|%s|%d|" % (method, url, str(data), int(time.time() * 1000 - _start_time))
        )
        return True, resp.json()


def get(url, params=""):
    headers = {"Content-Type": "application/json"}
    return _http_request(method="GET", url=url, headers=headers, data=params)


def post(url, params={}):
    headers = {"Content-Type": "application/json"}
    return _http_request(method="POST", url=url, headers=headers, data=params)
