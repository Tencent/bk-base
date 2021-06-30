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


from datamanage.utils.list_tools import list_diff


def fields_diff(orig_fields, new_fields):
    """
    对比新传入字段列表和模型已有字段列表的差异
    :param orig_fields: {List} 已有字段列表
    :param new_fields: {List} 新传入字段列表
    :return:
    add_hash_fields:{List} 待新增字段列表,
    delete_hash_fields:{List}待删除字段列表,
    update_hash_fields:{List}待变更字段列表
    """

    class HashDict(dict):
        def __hash__(self):
            return hash(self['field_name'])

        def __eq__(self, other):
            return self.__hash__() == other.__hash__()

    orig_hash_fields = [HashDict(field_dict) for field_dict in orig_fields]
    new_hash_fields = [HashDict(field_dict) for field_dict in new_fields]
    # 新传入字段列表 和 已有字段列表 的 差异
    add_hash_fields, delete_hash_fields, update_hash_fields = list_diff(orig_hash_fields, new_hash_fields)
    return add_hash_fields, delete_hash_fields, update_hash_fields


def update_orig_fields(orig_fields, new_fields):
    """
    按照new_fields修改原始字段列表的field_alias&description
    :param orig_fields: {List} 原始字段列表
    :param new_fields: {List} 新字段列表，示例:[{'field_name':'xx', 'field_alias':'yy', 'description':'zz'}]
    :return:
    """
    # 获取待更新的字段列表
    _, _, update_fields = fields_diff(orig_fields, new_fields)
    # 根据待更新的字段列表修改原始字段列表
    update_field_dict = {field_dict['field_name']: field_dict for field_dict in update_fields}
    for field_dict in orig_fields:
        if field_dict['field_name'] in update_field_dict:
            field_dict['field_alias'] = update_field_dict[field_dict['field_name']]['field_alias']
            field_dict['description'] = update_field_dict[field_dict['field_name']]['description']
