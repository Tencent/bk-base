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
from __future__ import absolute_import

import json


def common_success(res):
    assert res["result"]
    assert res["code"] == "1500200"


def common_deploy_success(res):
    assert res["result"]
    assert res["data"]["status"] == "success"
    want_deploy_plans = [
        {
            "version": "V1.3.0.X",
            "system": "linux",
            "config": [{"raw_data_id": 123, "deploy_plan_id": [11]}],
            "host_list": [
                {
                    "status": "success",
                    "ip": "x.x.x.x",
                    "error_message": "成功",
                    "bk_cloud_id": 1,
                }
            ],
        }
    ]
    assert json.dumps(res["data"]["deploy_plans"], sort_keys=True) == json.dumps(want_deploy_plans, sort_keys=True)


def common_failure(res):
    assert res["result"]
    assert res["data"]["status"] == "failure"
    assert res["data"]["deploy_plans"][0]["host_list"][0]["status"] == "failure"
    assert len(res["data"]["deploy_plans"][0]["host_list"][0]["error_message"]) > 0
    assert len(res["data"]["log"]) > 0


def common_no_host_config_failure(res):
    assert not res["result"]
    assert res["code"] == "1577215"
    assert len(res["message"]) > 0


def common_param_error(res, key):
    assert not res["result"]
    assert res["code"] == "1500001"
    assert key in json.dumps(res)


def common_inner_error(res):
    assert not res["result"]
    assert len(res["message"]) > 0
