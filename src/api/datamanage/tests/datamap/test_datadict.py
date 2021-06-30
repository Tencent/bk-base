# -*- coding:utf-8 -*- #
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


class TestDataDict(TestCase):
    """
    数据字典相关测试
    """

    def setUp(self):
        pass

    def test_get_data_dict_count(self):
        """获取数据字典count"""
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
            "is_standard": False,
            "token_pkey": "datamap_dict",
        }
        client = UnittestClient()
        url = reverse('datamap-get-data-dict-count')
        response = client.post(url, params)
        assert response.data['count'] > 0
        assert response.data['project_count'] > 0
        assert response.data['bk_biz_count'] > 0
        assert response.data['data_source_count'] > 0
        assert response.data['dataset_count'] > 0

    def test_get_data_dict_list(self):
        """获取数据字典list"""
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
            "is_standard": False,
            "token_pkey": "datamap_dict",
        }
        client = UnittestClient()
        url = reverse('datamap-get-data-dict-list')
        response = client.post(url, params)
        print('================ test_get_data_dict_list ================')
        print(
            'response.data:{}, response.message:{}, response.errors:{}'.format(
                response.data, response.message, response.errors
            )
        )
        assert len(response.data) > 0
