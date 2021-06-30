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


from django.test import TestCase
from rest_framework.reverse import reverse

from tests.utils import UnittestClient


class TestLifecycleTrend(TestCase):
    """
    生命周期趋势相关测试
    """

    def setUp(self):
        pass

    def test_asset_value_trend(self):
        """价值趋势相关测试"""
        params = {"dataset_id": "591_durant1115", "dataset_type": "result_table"}
        client = UnittestClient()
        url = reverse('asset_value-trend/asset-value')
        response = client.get(url, params)
        assert response.is_success()
        assert len(response.data['score']) > 0
        assert len(response.data['time']) > 0

    def test_assetvalue_to_cost_trend(self):
        """收益比趋势相关测试"""
        params = {"dataset_id": "591_durant1115", "dataset_type": "result_table"}
        client = UnittestClient()
        url = reverse('asset_value-trend/assetvalue-to-cost')
        response = client.get(url, params)
        assert response.is_success()
        assert len(response.data['score']) > 0
        assert len(response.data['time']) > 0

    def test_importance_trend(self):
        """重要度趋势相关测试"""
        params = {"dataset_id": "591_durant1115", "dataset_type": "result_table"}
        client = UnittestClient()
        url = reverse('asset_value-trend/importance')
        response = client.get(url, params)
        assert response.is_success()
        assert len(response.data['score']) > 0
        assert len(response.data['time']) > 0

    def test_range_trend(self):
        """广度趋势相关测试"""
        params = {"dataset_id": "591_durant1115", "dataset_type": "result_table"}
        client = UnittestClient()
        url = reverse('range-list-range-metric-by-influxdb')
        response = client.get(url, params)
        assert response.is_success()
        assert len(response.data['score']) > 0
        assert len(response.data['time']) > 0
        assert len(response.data['biz_count']) > 0
        assert len(response.data['proj_count']) > 0

    def test_heat_trend(self):
        """热度趋势相关测试"""
        params = {"dataset_id": "591_durant1115", "dataset_type": "result_table"}
        client = UnittestClient()
        url = reverse('heat-list-heat-metric-by-influxdb')
        response = client.get(url, params)
        assert response.is_success()
        assert len(response.data['score']) > 0
        assert len(response.data['time']) > 0
        assert len(response.data['query_count']) > 0
        assert len(response.data['day_query_count']) > 0
