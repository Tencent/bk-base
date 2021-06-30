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
import pytest
from rest_framework.reverse import reverse
from jobnavi.api_helpers.api.util.api_driver import APIResponseUtil
from tests.utils import UnittestClient


@pytest.mark.usefixtures("jobnavi_api_fixture")
def test_partial_update():
    cluster_id = "default"
    schedule_id = "test_schedule"
    url = reverse('jobnavi_schedule-detail', [cluster_id, schedule_id])
    data = {
        "schedule_id": "test",
        "type_id": "spark_sql",
        "description": "离线计算节点",
        "period": {
            "timezone": "Asia/shanghai",
            "cron_expression": "? ? ? ? ? ?",
            "frequency": 1,
            "first_schedule_time": 1539778425000,
            "delay": "1h",
            "period_unit": "h"
        },
        "parents": [{
            "parent_id": "P_xxx",
            "dependency_rule": "all_finished",
            "param_type": "fixed",
            "param_value": "1h"
        }],
        "recovery": {
            "enable": True,
            "interval_time": "1h",
            "retry_times": 3
        },
        "active": True,
        "exec_oncreate": True,
        "execute_before_now": True,
        "node_label": "calc_cluster",
        "decommission_timeout": "1h",
        "max_running_task": 1,
        "created_by": "xxx"
    }
    res = UnittestClient().patch(url, data)
    APIResponseUtil.check_response(res)
    assert res.message == "ok"
