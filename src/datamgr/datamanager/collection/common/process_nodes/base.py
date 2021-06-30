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
import logging
from abc import ABCMeta

from jinja2 import Environment, PackageLoader
from jinja2.exceptions import TemplateNotFound

from conf.settings import RUN_VERSION

logger = logging.getLogger()

TEMPLATE_ENV = Environment(loader=PackageLoader("collection", "templates"))

template_cache = dict()


class ProcessTemplate(metaclass=ABCMeta):
    template = None

    # 保留渲染后的 Jinja 对象

    def __init__(self, context):
        self.context = context
        self.content = None

        self.render(context)

    def render(self, context):
        template_content = self.load_template()
        self.content = json.loads(template_content.render(**context))

    @classmethod
    def load_template(cls):
        if cls.template is None:
            raise Exception(
                "Override load_template method or define self.template attribute"
            )

        template_path = cls.template

        if template_path in template_cache:
            return template_cache[template_path]

        try:
            template_content = TEMPLATE_ENV.get_template(template_path)
        except TemplateNotFound:
            # 在通用路径下找不到模板，需要考虑是否是在指定环境下
            template_path_with_version = f"{RUN_VERSION}/{template_path}"
            template_content = TEMPLATE_ENV.get_template(template_path_with_version)

        template_cache[template_path] = template_content
        return template_content

    def __getattribute__(self, key):
        if key.startswith("get_") and object.__getattribute__(self, "content") is None:
            raise Exception(
                "The ProcessTemplate must be rendered before it can be accessed"
            )

        return object.__getattribute__(self, key)


class SimpleTemplate:
    def __init__(self, context):
        self.context = context
        self.content = context


class ProcessNode(object, metaclass=ABCMeta):
    """
    处理节点
    """

    def __init__(self, params_template):
        self.params_template = params_template
