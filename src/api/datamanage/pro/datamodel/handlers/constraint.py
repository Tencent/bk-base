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


import re
import six

from common.log import logger

from django.utils.translation import ugettext_lazy as _
from django.forms.models import model_to_dict

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import DmmFieldConstraintConfig
from datamanage.pro.datamodel.models.model_dict import ConstraintType, SpecificConstraint


def validate_regex(regex, s):
    """
    校验是否符合正则表达式要求的格式
    :param regex: {String} 正则表达式
    :param s: {String} 字符串
    :return: {Boolean} 符合正则表达式要求的格式
    """
    if not re.match(regex, s):
        return False
    return True


def is_regex(param):
    """
    判断参数是否是合法正则表达式字符串
    :param param: {String} 参数
    :return: {Boolean} 是否是合法正则表达式
    """
    try:
        re.compile(param)
        return True
    except re.error:
        return False


def validate_string(param):
    """
    校验是否为字符串类型参数
    :param param: {String} 参数值
    :return: {Boolean} 校验是否为字符串类型参数
    """
    if isinstance(param, (str, six.text_type)):
        return True
    return False


def validate_number(param):
    """
    校验是否为数值型参数
    :param param: {String} 参数值
    :return: {Boolean} 校验是否为数值型参数
    """
    try:
        int(param)
    except (TypeError, ValueError):
        return False
    return True


def get_field_constraint_configs():
    """
    获取字段约束列表
    :return: constraint_list: {List} 字段约束列表
    """
    constraint_queryset = DmmFieldConstraintConfig.objects.all().order_by('constraint_index')
    constraint_list = [model_to_dict(constraint_object) for constraint_object in constraint_queryset]
    return constraint_list


def get_field_constraint_tree_list():
    """
    获取字段约束列表树形结构
    :return: constraint_tree_list: {List} 字段约束列表树形列表
    """
    constraint_list = get_field_constraint_configs()
    constraint_tree_list = []
    specific_constraint_list = []
    for constraint_dict in constraint_list:
        if constraint_dict['constraint_type'] == ConstraintType.GENERAL.value:
            constraint_tree_list.append(constraint_dict)
        else:
            specific_constraint_list.append(constraint_dict)

    constraint_tree_list.append(
        {
            'allow_field_type': [SpecificConstraint.SPECIFIC_CONSTRAINT_FIELD_TYPE],
            'constraint_id': SpecificConstraint.SPECIFIC_CONSTRAINT_ID,
            'constraint_name': SpecificConstraint.SPECIFIC_CONSTRAINT_NAME,
            'group_type': SpecificConstraint.SPECIFIC_CONSTRAINT_GROUP_TYPE,
            'children': specific_constraint_list,
        }
    )
    return constraint_tree_list


TYPE_VALIDATOR_DICT = {
    'number_validator': validate_number,
    'string_validator': validate_string,
    'regex_validator': is_regex,
}


def get_constraint_validator_dict():
    """
    获取约束校验字典
    :return: constraint_validator_dict: {Dict} 约束校验字典
    """
    constraint_list = get_field_constraint_configs()
    constraint_validator_dict = {}
    for each_constraint in constraint_list:
        constraint_validator_dict[each_constraint['constraint_id']] = each_constraint['validator']
    return constraint_validator_dict


def validate_field_constraint_content(fields):
    """
    校验字段约束内容
    :param fields: {List} 字段列表
    :return:
    """
    for field_dict in fields:
        field_name = field_dict['field_name']
        if not field_dict['field_constraint_content']:
            return
        for group_dict in field_dict['field_constraint_content']['groups']:
            for item_dict in group_dict['items']:
                # 类型校验
                constraint_id = item_dict['constraint_id']
                constraint_content = item_dict['constraint_content']
                constraint_validator = ConstraintValidator(constraint_id, constraint_content)
                try:
                    constraint_validator.validate()
                except dm_pro_errors.FieldConstraintFormatError as e:
                    logger.error(
                        'field constraint err, field_name:{}, constraint_id:{}, constraint_content:{}'.format(
                            field_name, constraint_id, constraint_content
                        )
                    )
                    raise e.__class__(message=_('字段:{} {}'.format(field_name, str(e))))


class ConstraintValidator(object):
    # 校验字段约束内容
    def __init__(self, constraint_id, constraint_content):
        self.constraint_id = constraint_id
        self.constraint_content = constraint_content
        self._constraint_validator_dict = None
        self.validator_type = None
        self.validator_content = None
        self.validator_regex = None
        self.set_validator_type()
        self.set_validator_content()
        self.set_validator_regex()

    @property
    def constraint_validator_dict(self):
        if not self._constraint_validator_dict:
            self._constraint_validator_dict = get_constraint_validator_dict()
        return self._constraint_validator_dict

    def set_validator_type(self):
        if self.constraint_id:
            self.validator_type = self.constraint_validator_dict[self.constraint_id].get('type', None)
        else:
            self.validator_type = None

    def set_validator_content(self):
        if self.validator_type:
            self.validator_content = self.constraint_validator_dict[self.constraint_id].get('content', None)
        else:
            self.validator_content = None

    def set_validator_regex(self):
        if self.validator_content and 'regex' in self.validator_content:
            self.validator_regex = self.validator_content['regex']
        else:
            self.validator_regex = None

    def validate(self):
        # 校验约束内容的类型格式
        if self.validator_type:
            val_type_ret = TYPE_VALIDATOR_DICT[self.validator_type](self.constraint_content)
            if not val_type_ret:
                raise dm_pro_errors.FieldConstraintFormatError(message_kv={'format_error_type': _('类型错误')})

        # 校验约束内容的内容格式
        if self.validator_regex:
            val_content_ret = validate_regex(self.validator_regex, self.constraint_content)
            if not val_content_ret:
                raise dm_pro_errors.FieldConstraintFormatError(message_kv={'format_error_type': _('内容错误')})
