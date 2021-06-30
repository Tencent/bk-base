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

import arrow
import dateutil
from jinja2 import Template
from tinyrpc.dispatch import public

from metadata.backend.interface import BackendType
from metadata.exc import MetaDataTypeError, TargetSearchCriterionError
from metadata.runtime import rt_context
from metadata.service.access.layer_interface import BaseAccessLayer, check_backend
from metadata.type_system.core import get_all_primitive_attr_defs
from metadata.util.i18n import lazy_selfish as _
from metadata_biz.type_system.converters import tag_related_md_types
from metadata_biz.types import Tag, TagTarget


class TagAccessLayer(BaseAccessLayer):
    """
    Todo: 1,Count在match_policy=both情况下不准。
    """

    per_target_type_search_template = Template(
        '''{{ target_type }}_targets as var(func:has({{ target_type_edge }})) @cascade {{ target_exp }}
       {~Tag.targets {{ tag_exp }}
       {{ relation_edge }} {{ relation_exp }}}'''
    )

    union_template = Template(
        '''full_targets(func: uid({{ targets }}),offset: {{offset}}, first: {{limit}})
      { tags: ~Tag.targets {%s}
        relations: ~TagTarget.target {%s}
        {{ attributes }}}'''
        % (
            '\n'.join(
                [k + ':' + 'Tag.' + k for k in get_all_primitive_attr_defs(Tag)]
                + ['parent_tag: Tag.parent_tag {code: Tag.code}']
            ),
            '\n'.join(k + ':' + 'TagTarget.' + k for k in get_all_primitive_attr_defs(TagTarget)),
        )
    )

    post_template = Template(
        '''
    tag_matched(func: has(Tag.code)) {{ tag_exp }} {code: Tag.code}
    full_count(func: uid({{ targets }}))
      { count(uid) }'''
    )

    target_md_type_mappings = {}
    for k, v in tag_related_md_types.items():
        if isinstance(v['target_type'], set):
            for i in v['target_type']:
                target_md_type_mappings[i] = k
        else:
            target_md_type_mappings[v['target_type']] = k

    @public
    @check_backend(BackendType.DGRAPH)
    def search_targets(
        self,
        target_types,
        target_filter=None,
        tag_filter=None,
        relation_filter=None,
        offset=0,
        limit=50,
        match_policy='any',
        tag_source_type='all',
    ):
        """
        使用tag或者各类属性作为过滤条件查询目标实体。

        :param match_policy: 匹配策略：any：返回关联了至少一个标签的实体。both：返回拥有所有标签的实体。
        :param limit: 分页
        :param offset: 偏移
        :param target_types: 需要查询的目标实体
        :param target_filter: 应用于目标的属性filter，用于过滤实体
        :param tag_filter: 应用于tag的属性filter，然后用符合条件的tag过滤实体
        :param relation_filter: 应用于tag和实体之间的relation的属性filter，用于过滤实体
        :param tag_source_type: 参与查询过滤的tag范围: system-内置/user-用户自定义/all-全域
        :return:
        """
        if not target_types:
            raise TargetSearchCriterionError(_('At least one target type must be applied.'))
        if len(target_types) != 1 and target_filter:
            raise TargetSearchCriterionError(_('Only one target type is required if target filter is applied.'))
        target_md_types = []
        for target_type in target_types:
            md_type = self.target_md_type_mappings.get(target_type, None)
            if not md_type:
                raise MetaDataTypeError(_('Fail to get md type by target type.'))
            target_md_types.append(self.target_md_type_mappings[target_type])

        typed_attr_names = [i + '.typed' for i in target_md_types]
        query_expression = self._build_query_expression(
            target_md_types, target_filter, tag_filter, relation_filter, offset, limit
        )

        backend = self.backends_group_storage[(BackendType('dgraph'))]
        content = backend.query(query_expression)
        ret = content['data']['full_targets']
        self._ret_post_parse(ret, typed_attr_names)
        count = content['data']['full_count'][0]['count']
        tag_matched = {item['code'] for item in content['data']['tag_matched']}
        if tag_filter and match_policy == 'both':
            ret = [item for item in ret if {i['code'] for i in item['tags']} >= tag_matched]
        for item in ret:
            for tag in item['tags']:
                if tag['parent_tag']:
                    tag['parent_tag'] = tag['parent_tag'][0]
        return {'count': count, 'content': ret}

    @staticmethod
    def _ret_post_parse(ret, typed_attr_names):
        for item in ret:
            for typed_attr_name in typed_attr_names:
                if typed_attr_name in item:
                    item.pop(typed_attr_name)
                    item['_target_type'] = typed_attr_name.split('.')[0]
            for k, v in list(item.items()):
                k_lst = k.split('.')
                if len(k_lst) > 1:
                    item[k_lst[1]] = item.pop(k)

    def parse_filter(self, type_to_filter, filter_struct, condition=None, expression_characters_lst=None):
        """
        解析通用filter。

        :param type_to_filter:
        :param filter_struct:
        :param condition:
        :param expression_characters_lst:

        TODO: 针对Dgraph的优化：
            · 提取第一条件到func
        :return:
        """
        if expression_characters_lst is None:
            expression_characters_lst = []
        for n, criterion in enumerate(filter_struct):
            if isinstance(criterion, dict):
                if 'condition' in criterion:
                    expression_characters_lst.append('(')
                    son_condition = criterion['condition']
                    son_criterion = criterion['criterion']
                    son_exp_chars_lst = self.parse_filter(type_to_filter, son_criterion, son_condition)
                    expression_characters_lst.append(''.join(son_exp_chars_lst))
                    expression_characters_lst.append(')')
                else:
                    md_type_cls = rt_context.md_types_registry[type_to_filter]
                    if criterion['k'] in ('updated_at', 'created_at'):
                        criterion['v'] = (
                            arrow.get(criterion['v'])
                            .replace(tzinfo=dateutil.tz.gettz(self.normal_conf.TIME_ZONE))
                            .isoformat()
                        )
                    expression = ''.join(
                        str(item)
                        for item in [
                            criterion['func'],
                            '(',
                            '.'.join(
                                [type_to_filter, criterion['k']]
                                if criterion['k'] not in md_type_cls.__metadata__.get('dgraph').get('common_predicates')
                                else [criterion['k']]
                            ),
                            ',',
                            '"',
                            criterion['v'],
                            '"' ')',
                        ]
                    )
                    expression_characters_lst.append(expression)
                    if condition and n != len(filter_struct) - 1:
                        expression_characters_lst.append(' {} '.format(condition))
        return ''.join(expression_characters_lst)

    def _build_query_expression(self, target_md_types, target_filter, tag_filter, relation_filter, offset, limit):
        tag_exp = (
            self.parse_filter(
                'Tag',
                tag_filter,
            )
            if tag_filter
            else ''
        )
        relation_exp = (
            self.parse_filter(
                'TagTarget',
                relation_filter,
            )
            if relation_filter
            else ''
        )
        target_exp = (
            self.parse_filter(
                target_md_types[0],
                target_filter,
            )
            if target_filter
            else []
        )
        all_targets_search_exps = []
        for target_type in target_md_types:
            exp = self.per_target_type_search_template.render(
                target_type=target_type,
                target_type_edge=target_type + '.typed',
                target_exp='@filter({})'.format(target_exp) if target_exp else '',
                tag_exp='@filter({})'.format(tag_exp) if tag_exp else '',
                relation_exp='@filter({})'.format(relation_exp) if relation_exp else '',
                relation_edge='~TagTarget.target' if relation_exp else '',
            )
            all_targets_search_exps.append(exp)
        attributes = []
        for target_md_type in target_md_types:
            md_type_cls = rt_context.md_types_registry.get(target_md_type, None)
            if not md_type_cls:
                raise MetaDataTypeError(_('Fail to get md type by type_name.'))
            for attr in get_all_primitive_attr_defs(md_type_cls):
                attributes.append('.'.join([target_md_type, attr]))
        targets = ','.join(per_type + '_targets' for per_type in target_md_types)
        union_expression = self.union_template.render(
            targets=targets,
            offset=offset,
            limit=limit,
            attributes='\n'.join(attributes),
            if_both='@filter(tag_count==full_tag_count)',
        )
        count_expression = self.post_template.render(
            targets=targets,
            tag_exp='@filter({})'.format(tag_exp) if tag_exp else '',
        )
        query_expression = '{{\n{}\n{}\n{}}}'.format(
            '\n'.join(all_targets_search_exps), union_expression, count_expression
        )
        return query_expression
