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
from copy import deepcopy

from rest_framework.test import APIClient


def get(url):
    client = APIClient()

    res = client.get(url)
    res = json.loads(res.content)

    _assert_return_format(res)

    return res


def post(url, data):
    client = APIClient()

    res = client.post(url, data, format="json")
    res = json.loads(res.content)

    _assert_return_format(res)

    return res


def put(url, data):
    client = APIClient()

    res = client.put(url, data, format="json")
    res = json.loads(res.content)

    _assert_return_format(res)

    return res


def patch(url, data):
    client = APIClient()

    res = client.patch(url, data, format="json")
    res = json.loads(res.content)

    _assert_return_format(res)

    return res


def delete(url, data=None):
    client = APIClient()
    res = client.delete(url, data=data, format="json")
    res = json.loads(res.content)

    _assert_return_format(res)
    return res


def _assert_return_format(res):
    assert isinstance(res.get("message"), str)


class NoField:
    def __init__(slef):
        pass


def mess_args(params):
    collection = []
    if isinstance(params, dict):

        for k in params:

            if not isinstance(params[k], dict):
                continue

            for invalid_param in params[k]["invalid"]:

                one_copy = deepcopy(params)
                for k2 in one_copy:
                    if not isinstance(one_copy[k2], dict):
                        continue

                    one_copy[k2] = one_copy[k2]["valid"]

                if invalid_param == NoField:
                    del one_copy[k]
                else:
                    one_copy[k] = invalid_param

                collection.append({"var": k, "invalid": invalid_param, "args": one_copy})

    return collection


def valid_args(params):
    ret = {}
    if isinstance(params, dict):
        for k in params:
            if isinstance(params[k], dict):
                ret[k] = params[k]["valid"]
            else:
                ret[k] = params[k]

    return ret
