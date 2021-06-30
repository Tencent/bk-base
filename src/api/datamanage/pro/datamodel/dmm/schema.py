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


from django.db.models import Q

from datamanage.pro.datamodel.models.datamodel import DmmModelFieldStage
from datamanage.pro.datamodel.models.model_dict import TimeField


def get_schema_list(model_id, with_field_details=False):
    """
    拿到主表字段列表，用于统计口径和指标sql校验
    :param model_id: {int} 模型ID
    :param with_field_details: {bool} 是否返回字段类型
    :return:schema_list: {list} 主表字段列表
    """
    # 获取主表字段queryset
    field_queryset = DmmModelFieldStage.objects.filter(model_id=model_id)

    # 获取扩展字段对应的来源字段queryset
    source_field_queryset = get_source_field_queryset(field_queryset)
    source_field_obj_dict = {
        source_field_obj.field_name: source_field_obj for source_field_obj in source_field_queryset
    }

    schema_list = []
    for field_obj in field_queryset:
        if field_obj.field_name != TimeField.TIME_FIELD_NAME and field_obj.field_type != TimeField.TIME_FIELD_TYPE:
            # 如果是扩展字段, 数据类型和字段类型从关联维度表中继承
            if field_obj.source_model_id and field_obj.source_field_name:
                source_field_obj = source_field_obj_dict[field_obj.source_field_name]
                field_type = source_field_obj.field_type
                field_category = source_field_obj.field_category
            else:
                field_type = field_obj.field_type
                field_category = field_obj.field_category
            # 是否返回字段类型
            if not with_field_details:
                schema_list.append({'field_type': field_type, 'field_name': field_obj.field_name})
            else:
                schema_list.append(
                    {
                        'field_type': field_type,
                        'field_name': field_obj.field_name,
                        'field_category': field_category,
                        'description': field_obj.description,
                        'field_alias': field_obj.field_alias,
                    }
                )
    return schema_list


def get_source_field_queryset(field_queryset):
    """
    获取主表扩展字段对应来源字段的queryset
    :param field_queryset:{QuerySet} 主表字段QuerySet
    :return: source_field_queryset: {QuerySet} 来源字段QuerySet
    """
    condition = None
    for field_obj in field_queryset:
        if field_obj.source_model_id and field_obj.source_field_name:
            if condition is None:
                condition = Q(model_id=field_obj.source_model_id, field_name=field_obj.source_field_name)
            else:
                condition |= Q(model_id=field_obj.source_model_id, field_name=field_obj.source_field_name)
    source_field_queryset = DmmModelFieldStage.objects.filter(condition) if condition is not None else []
    return source_field_queryset
