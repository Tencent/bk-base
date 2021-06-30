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


from datamanage.utils.api.meta import MetaApi
from datamanage.pro.datamodel.models.model_dict import ExcludeFieldType


def get_field_type_list():
    """
    字段数据类型
    :return: {List}:字段数据类型列表
    """
    field_type_config = MetaApi.get_field_type_configs(raise_exception=True).data
    return [field['field_type'] for field in field_type_config if field['field_type'] != 'text']


def get_field_type_configs(include_field_type=None, extra_exclude_field_type=None):
    """
    字段数据类型, 默认返回数值型和字符型
    :param include_field_type: {List} 额外返回的数据类型列表
    :param extra_exclude_field_type: {List} 不返回的数据类型列表
    :return: {List}:字段数据类型列表
    """
    # 1) 获取不返回的field_types
    exclude_field_type = ExcludeFieldType.get_enum_value_list()
    if include_field_type is None:
        include_field_type = []
    if extra_exclude_field_type is None:
        extra_exclude_field_type = []
    exclude_field_type += extra_exclude_field_type
    exclude_field_type = list(set(exclude_field_type).difference(set(include_field_type)))

    # 2）返回数据类型列表
    tmp_field_type_config = MetaApi.get_field_type_configs(raise_exception=True).data
    field_type_config = [
        field_dict for field_dict in tmp_field_type_config if field_dict['field_type'] not in exclude_field_type
    ]
    return field_type_config
