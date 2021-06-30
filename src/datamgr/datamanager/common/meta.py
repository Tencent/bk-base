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
from __future__ import absolute_import, print_function, unicode_literals

import logging

import attr
from jinja2 import Template
from metadata_client import DEFAULT_SETTINGS, MetadataClient

from conf.settings import metadata_settings

logger = logging.getLogger("root")


class DMMetadataClient(MetadataClient):

    LIST_TEMPLATE = Template(
        """
        {
            target(func: has({{identifier_dgraph_field}}), offset: {{ offset }}, first: {{ first }}) {
                {% for (field, dgraph_field) in field_groups %}
                    {{ field }} : {{ dgraph_field }}
                {% endfor %}
            }
        }
    """
    )

    ONE_TEMPLATE = Template(
        """
        {
            target(func: eq({{identifier_dgraph_field}}, {{object_id}}), offset: {{ offset }}, first: {{ first }}) {
                {% for (field, dgraph_field) in field_groups %}
                    {{ field }} : {{ dgraph_field }}
                {% endfor %}
            }
        }
    """
    )

    def list(self, md):
        """
        根据类型返回列表数据

        @param {LocalMetadata} md 类型
        """
        field_groups = [(f.name, self.to_dgraph_field(md, f)) for f in attr.fields(md)]
        identifier_dgraph_field = self.to_dgraph_field(md, md.metadata["identifier"])

        dgraph_data = []
        page = 1
        page_size = 5000

        # 分批拉取数据
        while 1:
            statement = self.LIST_TEMPLATE.render(
                identifier_dgraph_field=identifier_dgraph_field,
                field_groups=field_groups,
                offset=page_size * (page - 1),
                first=page_size,
            )
            logger.info("[dgraph] List statement: {}".format(statement))
            response = response = self.query(statement)
            data = response["data"]["target"]
            dgraph_data.extend(data)
            if len(data) < page_size:
                break

            page += 1

        return [md(**d) for d in dgraph_data]

    def get(self, md, object_id):
        """
        根据类型返回对象数据

        @param {LocalMetadata} md 类型
        @param {String} object_id
        """
        field_groups = [(f.name, self.to_dgraph_field(md, f)) for f in attr.fields(md)]
        identifier_dgraph_field = self.to_dgraph_field(md, md.metadata["identifier"])

        statement = self.ONE_TEMPLATE.render(
            identifier_dgraph_field=identifier_dgraph_field, field_groups=field_groups
        )
        response = response = self.query(statement)
        if len(response["data"]["target"]) < 1:
            raise Exception("No object...")

        data = response["data"]["target"][0]
        return md(**data)

    def to_dgraph_field(self, md, field):
        """

        @param {LocalMetadata} md
        @param {attr.ib} field
        """
        table_name = md.__name__
        field_name = field.name

        if field_name in [
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
            "active",
        ]:
            return field_name

        return "{}.{}".format(table_name, field_name)


def create_client(settings):
    _settings = DEFAULT_SETTINGS.copy()
    _settings.update(settings)
    return DMMetadataClient(_settings)


metadata_client = create_client(metadata_settings)
