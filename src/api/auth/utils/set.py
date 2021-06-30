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


def intersect(_list):
    """
    取N个iterable对象的交集
    @param {List(iterable)]} _list 由可迭代对象作为元素组成的列表
    @paramExample [[1,2,3,4], [2,3,4,5], [3,4,5,6], [1,2,3,4,5,6]]
    @return: [3, 4]
    """
    if not _list:
        return []
    return list(set(_list[0]).intersection(*_list[1:]))


def remove_duplicate_dict(_list):
    """
    字典列表去重, 目前仅支持一级字典
    @param _list:
    @paramExample _list 由可迭代对象作为元素组成的列表
    [
        {"bk_biz_id": 591, "result_table_id": "591_a"},
        {"bk_biz_id": 591, "result_table_id": "591_a"},
        {"bk_biz_id": 591, "result_table_id": "591_b"},
    ]
    @return:
    [
        {"bk_biz_id": 591, "result_table_id": "591_a"},
        {"bk_biz_id": 591, "result_table_id": "591_b"},
    ]
    """
    if not _list:
        return []
    return [dict(t) for t in {tuple(d.items()) for d in _list}]
