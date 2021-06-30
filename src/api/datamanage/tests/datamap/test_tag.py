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


class TestTag(TestCase):
    """
    标签选择器测试
    """

    def setUp(self):
        pass

    def test_get_tag_sort_count(self):
        """标签选择器相关测试"""
        params = {"top": 12, "exclude_source": 1, "is_overall_topn_shown": 1, "overall_top": 10}
        client = UnittestClient()
        url = reverse('datamap-get-tag-sort-count')
        response = client.get(url, params)
        print('================ test_get_tag_sort_count ================')
        print(
            'response.data:{}, response.message:{}, response.errors:{}'.format(
                response.data, response.message, response.errors
            )
        )
        assert response.is_success()
        assert len(response.data['tag_list']) > 0
        assert len(response.data['overall_top_tag_list']) > 0
