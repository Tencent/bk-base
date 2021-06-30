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
from datahub.access.tests.utils import post
from django.db import IntegrityError


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_success(mocker):
    params = {
        "result_table_id": "103_xxx_test",
        "storage": "hdfs",
        "event_type": "tdwFinishDir",
        "event_value": "20180330",
        "description": "test",
    }

    mocker.patch("datahub.databus.event.check_hdfs_event_and_notify", return_value=(True, "ok"))

    res = post("/v3/databus/events/", params)
    print(res)
    del res["data"]["id"]
    del res["data"]["created_by"]
    del res["data"]["updated_by"]
    del res["data"]["updated_at"]
    del res["data"]["created_at"]

    compare_json = {
        "message": "ok",
        "errors": None,
        "code": "1500200",
        "data": {
            "result_table_id": "103_xxx_test",
            "storage": "hdfs",
            "event_type": "tdwFinishDir",
            "event_value": "20180330",
            "description": "test",
        },
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_no_hdfs(mocker):
    params = {
        "result_table_id": "103_xxx_test",
        "storage": "mysql",
        "event_type": "tdwFinishDir",
        "event_value": "20180330",
        "description": "test",
    }
    mocker.patch("datahub.databus.rt.get_partitions", return_value=1)
    res = post("/v3/databus/events/", params)

    compare_json = {
        "errors": None,
        "message": u"保存总线事件失败, mysql 存储类型当前不支持",
        "code": "1570009",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_create_integrityerror(mocker):
    params = {
        "result_table_id": "103_xxx_test",
        "storage": "hdfs",
        "event_type": "tdwFinishDir",
        "event_value": "20180330",
        "description": "test",
    }

    mocker.patch(
        "datahub.databus.models.DatabusStorageEvent.objects.create",
        side_effect=IntegrityError,
    )

    res = post("/v3/databus/events/", params)

    assert not res["result"]

    compare_json = {
        "message": u"总线事件创建失败,result_table_id:103_xxx_test",
        "errors": None,
        "code": "1570026",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_notify_jobnavi_success(mocker):
    params = {"result_table_id": "103_xxx_test", "date_time": "20180330"}

    mocker.patch("datahub.databus.event.call_dataflow_batch_job", return_value=(True, "ok"))

    res = post("/v3/databus/events/notify_jobnavi/", params)

    assert res["result"]

    compare_json = {
        "message": "ok",
        "errors": None,
        "code": "1500200",
        "data": {"date_time": "20180330", "result_table_id": "103_xxx_test"},
        "result": True,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_notify_jobnavi_failed(mocker):
    params = {"result_table_id": "103_xxx_test", "date_time": "20180330"}

    mocker.patch("datahub.databus.event.call_dataflow_batch_job", return_value=(False, "error"))

    res = post("/v3/databus/events/notify_jobnavi/", params)

    assert not res["result"]

    compare_json = {
        "message": u"总线事件调用离线任务失败,result_table_id:103_xxx_test",
        "errors": None,
        "code": "1570019",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)


@pytest.mark.httpretty
@pytest.mark.django_db
def test_notify_jobnavi_date_not_valid():
    params = {"result_table_id": "103_xxx_test", "date_time": "2018-03-30"}

    res = post("/v3/databus/events/notify_jobnavi/", params)

    assert not res["result"]

    compare_json = {
        "message": u"总线调用离线任务失败,日期参数校验失败,date_time:2018-03-30",
        "errors": None,
        "code": "1570019",
        "data": None,
        "result": False,
    }
    assert json.dumps(res, sort_keys=True) == json.dumps(compare_json, sort_keys=True)
