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

import httpretty as hp
import pytest
from datahub.databus.event import (
    call_dataflow_batch_job,
    check_hdfs_event_and_notify,
    send_warn_info,
    validate_date_time,
)
from datahub.databus.models import DatabusStorageEvent  # noqa
from datahub.databus.rt import get_partitions  # noqa
from datahub.databus.tests.fixture.event_fixture import (  # noqa
    add_event,
    add_event_with_invalid_date,
)
from datahub.databus.tests.mock_api import dataflow


@pytest.mark.httpretty
@pytest.mark.django_db
def test_send_warn_info():
    msg = "failed call dataflow batch job!"
    send_warn_info(msg)


def test_validate_date_time_len8():
    date_time = "20180810"
    res = validate_date_time(date_time)
    assert res


def test_validate_date_time_len10():
    date_time = "2018081011"
    res = validate_date_time(date_time)
    assert res


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_event")
def test_check_hdfs_event_and_notify(mocker):
    rt_id = "103_xxx_test"
    event_type = "tdwFinishDir"
    event_value = "20181118"
    mocker.patch("datahub.databus.rt.get_partitions", return_value=1)
    mocker.patch("datahub.databus.event.call_dataflow_batch_job", return_value=(True, "ok"))
    res = check_hdfs_event_and_notify(rt_id, event_type, event_value)
    assert res[0]
    assert res[1] == "ok"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_event")
def test_check_hdfs_event_not_finish(mocker):
    rt_id = "103_xxx_test"
    event_type = "tdwFinishDir"
    event_value = "20181118"
    mocker.patch("datahub.databus.rt.get_partitions", return_value=2)
    mocker.patch("datahub.databus.event.call_dataflow_batch_job", return_value=(True, "ok"))
    res = check_hdfs_event_and_notify(rt_id, event_type, event_value)
    assert not res[0]
    assert res[1] == "process_not_finished"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_event_with_invalid_date")
def test_check_hdfs_event_not_valid(mocker):
    rt_id = "103_xxx_test"
    event_type = "tdwFinishDir"
    event_value = "201811181111"
    mocker.patch("datahub.databus.rt.get_partitions", return_value=1)
    res = check_hdfs_event_and_notify(rt_id, event_type, event_value)
    assert not res[0]
    assert res[1] == "201811181111 not a valid date time for offline calculate"


@pytest.mark.httpretty
@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_event")
def test_check_hdfs_event_failed(mocker):
    rt_id = "103_xxx_test"
    event_type = "tdwFinishDir"
    event_value = "20181118"
    mocker.patch(
        "datahub.databus.models.DatabusStorageEvent.objects.filter",
        side_effect=Exception,
    )
    res = check_hdfs_event_and_notify(rt_id, event_type, event_value)
    assert not res[0]
    assert res[1] == "failed to check and notify event with result_table_id：103_xxx_test"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_call_dataflow_batch_job():
    rt_id = "103_xxx_test"
    date_time = "20181118"
    hp.enable()
    dataflow.execute_ok()
    res = call_dataflow_batch_job(rt_id, date_time)
    print(res)
    assert res[0]
    assert res[1] == "ok"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_call_dataflow_batch_job_failed():
    rt_id = "103_xxx_test"
    date_time = "20181118"
    dataflow.execute_failed()
    res = call_dataflow_batch_job(rt_id, date_time)
    assert not res[0]
    assert res[1] == "[bad response!] failed to call dataflow batch job result_table_id:103_xxx_test"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_call_dataflow_batch_job_error(mocker):
    rt_id = "103_xxx_test"
    date_time = "20181118"
    mocker.patch("datahub.databus.api.DataFlowApi.batch_jobs.execute", side_effect=Exception)
    res = call_dataflow_batch_job(rt_id, date_time)
    assert not res[0]
    assert res[1] == "failed to call dataflow batch job result_table_id:103_xxx_test"
