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

from collections import OrderedDict

from attr import Factory, fields

from metadata.runtime import rt_context

doc_template_per_md = """
.. class:: {type_name}
    定位字段: {identifier}
    字段定义:

    {fields}

    反向关联字段定义:

    {reserve_fields}

    查看代码文档：:class:`~{type_path}`
"""


def to_standard_dict(x):
    return {k: v for k, v in x.items()}


field_attrs_to_display = {
    'default': lambda x: x if not isinstance(x, Factory) else 'Factory[{}]'.format(x.factory.__name__),
    'metadata': to_standard_dict,
    'type': lambda x: x.__name__,
}


def generate_expression(md_type):
    md_type_expression = {'fields': OrderedDict()}
    fields_info = fields(md_type)
    for field in fields_info:
        field_name = field.name
        md_type_expression['fields'][field_name] = {
            f_attr: func(getattr(field, f_attr)) for f_attr, func in field_attrs_to_display.items()
        }
    md_type_expression['identifier'] = (
        md_type.metadata.get('identifier').name if md_type.metadata.get('identifier') else None
    )
    md_type_expression['reverse_referenced'] = (
        {md_cls.__name__: md_attr.name for md_cls, md_attr in md_type.metadata.get('reverse_referenced')}
        if md_type.metadata.get('reverse_referenced')
        else {}
    )
    md_type_expression['path'] = md_type.__module__ + '.' + md_type.__name__
    return md_type_expression


def generate_def_expressions_doc():
    full_docs = []
    full_expressions = {}
    for md_name, md_type in rt_context.md_types_registry.items():
        full_expressions[md_name] = generate_expression(md_type)
    for k, v in full_expressions.items():
        if not v['identifier']:
            continue
        per_args = {
            'type_name': k,
            'type_path': v['path'],
            'fields': '\n    '.join(
                '* **{}** - type: {}, default: {}'.format(field_name, field_info['type'], field_info['default'])
                for field_name, field_info in v['fields'].items()
            ),
            'reserve_fields': '\n    '.join(
                '* **{}**'.format('.'.join([r_f_name, r_f_v])) for r_f_name, r_f_v in v['reverse_referenced'].items()
            )
            if v['reverse_referenced']
            else '无',
            'identifier': v['identifier'],
        }

        full_docs.append(doc_template_per_md.format(**per_args))
    return full_docs
