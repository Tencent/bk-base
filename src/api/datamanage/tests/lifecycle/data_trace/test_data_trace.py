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

from django.test import TestCase
from rest_framework.reverse import reverse

from tests.utils import UnittestClient


@pytest.mark.usefixtures('patch_es_search_data', 'patch_es_bulk_data')
class DataTraceTest(TestCase):
    """
    数据足迹相关测试
    """

    def setUp(self):
        pass

    @pytest.mark.run(order=1)
    def test_get_data_traces(self):
        """获取数据足迹事件"""
        params = {'dataset_id': '591_test_indicator', 'dataset_type': 'result_table'}
        client = UnittestClient()
        url = reverse('data_traces-list')
        response = client.get(url, params)
        assert len(response.data) > 0

    @pytest.mark.run(order=2)
    def test_data_trace_event_report(self):
        """数据足迹事件上报"""
        opr_info = {
            "show_type": "add_tips",
            "kv_tpl": "{flow_id}",
            "alias": "\\u4efb\\u52a1\\u72b6\\u6001\\u53d8\\u5316",
            "desc_params": [{"flow_id": "1"}],
            "sub_type_alias": "flow\\u4efb\\u52a1\\u505c\\u6b62",
            "desc_tpl": "\\u5173\\u8054flow\\u4efb\\u52a1 {change_content} \\u505c\\u6b62",
            "jump_to": {"bk_biz_id": 591, "flow_id": "1", "jump_to": "flow"},
        }
        data_trace_event_dict = {
            "contents": [
                {
                    "opr_type": "update_task_status",
                    "dispatch_id": "test-6fb25074-9dfb-4d9a-96b0-9581ea1671eb",
                    "opr_sub_type": "task_stop",
                    "created_at": "2021-04-18 15:43:21",
                    "created_by": "admin",
                    "data_set_id": "591_data_trace_event_report_es",
                    "opr_info": json.dumps(opr_info),
                    "description": "\\u5173\\u8054flow\\u4efb\\u52a1 1294 \\u505c\\u6b62",  # noqa
                }
            ]
        }
        client = UnittestClient()
        url = reverse('data_traces-event-report')
        response = client.post(url, data_trace_event_dict)
        assert response.is_success() is True
