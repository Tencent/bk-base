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


def list_diff(orig_list, new_list):
    """
    原有列表 和 新列表的差异
    :param orig_list: {List} 原有列表
    :param new_list: {List} 新列表
    :return:
    add_list: {List} 待新增的列表
    delete_list: {List} 待删除的列表
    same_list: {List} 不变的列表
    """
    new_obj_map = {hash(obj): obj for obj in new_list}
    # 在new_list但不在orig_list中，即待新增的列表
    add_list = list(set(new_list).difference(orig_list))
    # 在orig_list但不在new_list中，即待删除的列表
    delete_list = list(set(orig_list).difference(new_list))
    same_list = [new_obj_map[hash(same_obj)] for same_obj in list(set(orig_list).intersection(set(new_list)))]
    return add_list, delete_list, same_list


def split_list(target, separator=','):
    """按分割符号分割目标

    :param target 待分割的字符串或是已分割的列表
    :param separator 分隔符

    :return: 分割后不包含空字符串的列表
    """
    if isinstance(target, list):
        return target

    if target:
        return target.split(separator)
    else:
        return []
