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


import json
import traceback

from django.core import exceptions
from django.db import models
from django.utils.translation import ugettext_lazy as _

from common.log import logger


class JsonField(models.TextField):
    """
    Json字段，入库json.dumps， 出库json.load
    """

    def from_db_value(self, value, expression, connection, context):
        # 如果是None或者空字符串，直接返回k'n'g
        if value is None or value == '':
            return {}
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            logger.error('failed to convert value->[%s] from json format for->[%s]' % (value, traceback.format_exc()))
            raise exceptions.ValidationError(
                error_message=_(self.error_messages['invalid']),
                code='invalid',
                params={'value': value},
            )

    def to_python(self, value):
        if value is None:
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except (TypeError, ValueError):
                logger.error(
                    'failed to convert value->[%s] from json format for->[%s]' % (value, traceback.format_exc())
                )
                raise exceptions.ValidationError(
                    error_message=_(self.error_messages['invalid']),
                    code='invalid',
                    params={'value': value},
                )

        return value

    def get_prep_value(self, value):
        try:
            return json.dumps(value)
        except ValueError:
            return value

    def meta_value_from_object(self, obj):
        mid = self.value_from_object(obj)
        return self.get_prep_value(mid)
