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
def test_calculate_schedule_task_time():
    cluster_id = "default"
    url = reverse('jobnavi_admin_schedule-calculate-schedule-task-time', [cluster_id])
    data = {
        "rerun_processings": "123_clean,456_import,789_calculate",
        "rerun_model": "current_canvas",
        "start_time": 1537152075939,
        "end_time": 1537152075939,
    }
    res = UnittestClient().post(url, data)
    APIResponseUtil.check_response(res)
    assert "scheduleId" in res.data[0]
