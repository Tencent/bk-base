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


class TestSankeyDiagram(TestCase):
    """
    桑基图相关测试
    """

    def setUp(self):
        pass

    def test_sankey_diagram_distribution(self):
        """sankey图分布相关测试"""
        params = {
            "bk_biz_id": None,
            "project_id": None,
            "tag_ids": [],
            "keyword": "",
            "tag_code": "virtual_data_mart",
            "me_type": "tag",
            "has_standard": 1,
            "cal_type": ["standard"],
            "data_set_type": "all",
            "page": 1,
            "page_size": 10,
            "platform": "bk_data",
            "level": 4,
        }
        client = UnittestClient()
        url = reverse('sankey_diagram-distribution')
        response = client.post(url, params)
        assert len(response.data['target']) > 0
        assert len(response.data['value']) > 0
        assert len(response.data['source']) > 0

        # 按照标签、业务和项目过滤
        params = {
            "bk_biz_id": "591",
            "project_id": 298,
            "tag_ids": ["online"],
            "keyword": "",
            "tag_code": "virtual_data_mart",
            "me_type": "tag",
            "has_standard": 1,
            "cal_type": ["standard"],
            "data_set_type": "all",
            "page": 1,
            "page_size": 10,
            "platform": "bk_data",
            "level": 4,
        }
        client = UnittestClient()
        url = reverse('sankey_diagram-distribution')
        response = client.post(url, params)
        response.is_success()

        # 按照所有高级搜索参数过滤
        params = {
            "bk_biz_id": "591",
            "project_id": 298,
            "tag_ids": [],
            "keyword": "",
            "tag_code": 410,
            "me_type": "standard",
            "has_standard": 1,
            "cal_type": ["standard", "only_standard"],
            "data_set_type": "all",
            "page": 1,
            "page_size": 10,
            "platform": "bk_data",
            "storage_type": "tspider",
            "heat_operate": "ge",
            "heat_score": "0.00",
            "range_operate": "ge",
            "range_score": "13.10",
            "asset_value_operate": "ge",
            "asset_value_score": "0.00",
            "assetvalue_to_cost_operate": "ge",
            "assetvalue_to_cost": "0.00",
            "created_by": "admin",
            "importance_operate": "ge",
            "importance_score": "0.00",
            "storage_capacity_operate": "ge",
            "storage_capacity": "0.00",
            "standard_name": "登录-客户端（TCLS&WEGAME)",
            "standard_content_id": 1010,
            "standard_content_name": "登录-客户端-明细数据",
            "created_at_start": "2021-05-15 21:57:00",
            "created_at_end": "2021-05-22 21:57:00",
            "level": 4,
        }
        client = UnittestClient()
        url = reverse('sankey_diagram-distribution')
        response = client.post(url, params)
        print('================ test_sankey_diagram_distribution ================')
        print(
            'response.data:{}, response.message:{}, response.errors:{}'.format(
                response.data, response.message, response.errors
            )
        )
        response.is_success()
