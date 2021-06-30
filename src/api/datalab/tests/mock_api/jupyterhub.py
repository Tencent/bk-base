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
import re

import httpretty as hp
from datalab.pizza_settings import JUPYTERHUB_API_ROOT


def mock_create_user(jupyter_username):
    hp.register_uri(
        hp.POST,
        re.compile(JUPYTERHUB_API_ROOT + "/hub/api/users/%s" % jupyter_username),
        body=json.dumps({"result": True}),
        status=201,
    )


def mock_start_server(jupyter_username):
    hp.register_uri(
        hp.POST,
        re.compile(JUPYTERHUB_API_ROOT + "/hub/api/users/%s/server" % jupyter_username),
        body=json.dumps({"result": True}),
        status=201,
    )


def mock_create_notebook(jupyter_username):
    hp.register_uri(
        hp.POST,
        re.compile(JUPYTERHUB_API_ROOT + "/user/%s/api/contents" % jupyter_username),
        body=json.dumps({"name": "Untitled1.ipynb"}),
        status=201,
    )


def mock_create_template_notebook(jupyter_username):
    hp.register_uri(
        hp.POST,
        re.compile(JUPYTERHUB_API_ROOT + "/user/%s/api/contents" % jupyter_username),
        body=json.dumps({"name": "example.ipynb"}),
        status=201,
    )


def mock_create_token(jupyter_username):
    hp.register_uri(
        hp.POST,
        re.compile(JUPYTERHUB_API_ROOT + "/hub/api/users/%s/tokens" % jupyter_username),
        body=json.dumps({"token": 123}),
    )


def mock_pod_metrics():
    hp.register_uri(
        hp.GET,
        re.compile(JUPYTERHUB_API_ROOT + "/metrics"),
        body=json.dumps(
            [
                {
                    "pod_name": "jupyter-1",
                    "cpu_usage_rate": "0.00240",
                    "memory_usage_rate": "0.2553",
                    "metric_time": "2020-10-05 03:44:05",
                }
            ]
        ),
    )


def mock_retrieve_content_name(jupyter_username):
    hp.register_uri(
        hp.GET,
        re.compile(JUPYTERHUB_API_ROOT + "/user/%s/api/sessions" % jupyter_username),
        body=json.dumps([{"kernel": {"id": "id0"}, "path": "test_content"}]),
    )


def mock_retrieve_notebook_contents(jupyter_username):
    hp.register_uri(
        hp.GET,
        re.compile("http://datalab.com/user/%s/api/contents/Untitled.ipynb" % jupyter_username),
        body=json.dumps({"contents": {"cells": []}}),
    )
