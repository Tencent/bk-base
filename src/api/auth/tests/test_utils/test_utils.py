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
from auth.tests.utils import BaseTestCase
from auth.utils import generate_random_string
from auth.utils.dag import check_circle
from auth.utils.set import intersect, remove_duplicate_dict


class UtilsTestCase(BaseTestCase):
    def test_dag(self):
        # 边界输入
        check_list_empty = []
        # 无环
        check_list_0 = [(1, 2), (2, 3), (3, 4), (3, 5), (5, 6), (6, 7)]
        # 有两个环
        check_list_1 = [(1, 2), (2, 3), (3, 1)]
        # 有两个环
        check_list_2 = [(1, 2), (2, 3), (3, 1), (3, 4), (5, 4), (5, 6), (6, 7), (7, 5)]

        self.assertEqual(check_circle(check_list_empty), False)
        self.assertEqual(check_circle(check_list_0), False)
        self.assertEqual(check_circle(check_list_1), True)
        self.assertEqual(check_circle(check_list_2), True)

    def test_intersect(self):
        # 边界输入
        check_list_empty = []
        # 交集为[3, 4]
        check_list_0 = [[1, 2, 3, 4], [2, 3, 4, 5], [3, 4, 5, 6], [1, 2, 3, 4, 5, 6]]
        # 交集为空
        check_list_1 = [[1, 2, 3, 4], [5, 6, 7, 8]]
        # 交集重合
        check_list_2 = [[1, 2], [1, 2]]
        # 单个重合
        check_list_3 = [[1, 2, 1, 2]]

        self.assertEqual(intersect(check_list_2), [1, 2])
        self.assertEqual(intersect(check_list_3), [1, 2])
        self.assertEqual(intersect(check_list_empty), [])
        self.assertEqual(intersect(check_list_0), [3, 4])
        self.assertEqual(intersect(check_list_1), [])

    def test_my_random(self):
        count = 1000
        length = 128
        random_strings = [generate_random_string(length=length) for _ in range(count)]
        for _str in random_strings:
            self.assertEqual(len(_str), length)
        if length > 5:
            # 长度太小的话无法避免会重复，只能靠后续的业务逻辑去判断
            self.assertEqual(len(set(random_strings)), count)

    def test_remove_duplicate_dict(self):
        origin_list = [
            {"bk_biz_id": 591, "result_table_id": "591_a"},
            {"bk_biz_id": 591, "result_table_id": "591_a"},
            {"bk_biz_id": 591, "result_table_id": "591_b"},
        ]
        target_list = remove_duplicate_dict(origin_list)
        self.assertEqual(len(target_list), 2)
