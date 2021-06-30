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

import pytest
from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from common.api.modules.bklogin import BkLoginApi
from common.api.modules.cmsi import CmsiApi
from common.exceptions import ApiRequestError

DEMO_URL = "http://demo.com/"


class _StreamApi:
    def __init__(self):
        self.jobs = DataDRFAPISet(
            url=DEMO_URL + "jobs/",
            module="stream.job",
            primary_key="job_id",
            description="流程任务资源操作集合",
            default_return_value=(lambda: {"result": True, "data": "ok"}),
            custom_config={
                "start": DRFActionAPI(detail=True, method="post"),
                "top": DRFActionAPI(detail=False, method="get"),
            },
        )
        self.simple = DataAPI(url=DEMO_URL + "simple", method="get", module="stream.simple", description="简单接口")
        self.fail = DataAPI(url=DEMO_URL + "fail", method="get", module="stream.fail", description="异常接口")


streamapi = _StreamApi()


class TestStreamApi:
    def test_simple(self, requests_mock):
        requests_mock.get(f"{DEMO_URL}simple", complete_qs=False, text=json.dumps({"result": True, "data": "OK"}))
        assert streamapi.simple({}).data == "OK"

        requests_mock.get(f"{DEMO_URL}fail", complete_qs=False, text="Bad Gateway", status_code="502")
        with pytest.raises(ApiRequestError):
            streamapi.fail({})

    def test_list(self):
        api = streamapi.jobs.list
        assert api.url == "http://demo.com/jobs/"
        assert api.method == "get"

    def test_create(self):
        api = streamapi.jobs.create
        assert api.url == "http://demo.com/jobs/"
        assert api.method == "post"

    def test_update(self):
        api = streamapi.jobs.update
        assert api.url == "http://demo.com/jobs/{job_id}/"
        assert api.method == "put"

    def test_partial_update(self):
        api = streamapi.jobs.partial_update
        assert api.url == "http://demo.com/jobs/{job_id}/"
        assert api.method == "patch"

    def test_delete(self):
        api = streamapi.jobs.delete
        assert api.url == "http://demo.com/jobs/{job_id}/"
        assert api.method == "delete"

    def test_retrieve(self):
        api = streamapi.jobs.retrieve
        assert api.url == "http://demo.com/jobs/{job_id}/"
        assert api.method == "get"

    def test_start(self):
        api = streamapi.jobs.start
        assert api.url == "http://demo.com/jobs/{job_id}/start/"
        assert api.method == "post"

    def test_top(self):
        api = streamapi.jobs.top
        assert api.url == "http://demo.com/jobs/top/"
        assert api.method == "get"


class TestModuleAPI:
    def test_cmsi(self, requests_mock):
        requests_mock.post(
            "/api/c/compapi/v2/cmsi/send_rtx/",
            complete_qs=False,
            text=json.dumps({"result": True, "message": "企业微信消息发送成功"}),
        )
        response = CmsiApi.send_eewechat({"receivers": ["user01", "user02"], "message": "WTF, fighting...."})
        assert response.message == "企业微信消息发送成功"

    def test_bklogin(self):
        response = BkLoginApi.get_user_info({"bk_username": "admin"})
        assert response.is_success()
        assert response.data == {}
