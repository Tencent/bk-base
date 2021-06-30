# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import json

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

    def to_python(self, value):
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


class MultiStrSplitByCommaField(models.CharField):
    """
    多个字段，使用逗号隔开，入库list->str， 出库 str->list
    头尾都加逗号","，为了方便使用ORM的contains进行过滤且避免子集字符串的越权问题
    """

    def from_db_value(self, value, expression, connection, context):
        if not value:
            return []
        try:
            value = value.split(",")
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
            value = ",%s," % ",".join(value)
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
            value = ",%s," % ",".join(value)
        except (TypeError, KeyError):
            raise exceptions.ValidationError(
                self.error_messages["invalid"],
                code="invalid",
                params={"value": value},
            )
        return value
