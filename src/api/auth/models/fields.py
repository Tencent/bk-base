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

from common.base_crypt import BaseCrypt
from django.core import exceptions
from django.db import models


class JsonField(models.TextField):
    """
    Json字段，入库json.dumps， 出库json.load
    """

    def from_db_value(self, value, expression, connection, context):
        if not value:
            return {}
        try:
            return json.loads(value)
        except (TypeError, KeyError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )

    def get_prep_value(self, value):
        if value is None:
            return value
        try:
            return json.dumps(value)
        except (TypeError, ValueError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )


class SemicolonSplitField(models.TextField):
    """
    分号分隔符字段，入库list -> str，出库 str -> list，首尾均加上分号";"，
    方便django orm的contains进行过滤也避免子集字符串越权问题
    """

    def from_db_value(self, value, expression, connection, context):
        if not value:
            return []
        try:
            value = value.split(";")
        except (TypeError, KeyError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )
        return [_v for _v in value if _v != ""]

    def to_python(self, value):
        if not value:
            return []
        try:
            value = ";%s;" % ";".join(value)
        except (TypeError, KeyError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )
        return value

    def get_prep_value(self, value):
        if not value:
            return []
        try:
            value = ";%s;" % ";".join(value)
        except (TypeError, KeyError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )
        return value


class CryptField(models.CharField):
    """
    加密字段
    """

    def to_python(self, string=None):
        if string is None:
            return ""

        string = BaseCrypt.bk_crypt().encrypt(string)
        return string
