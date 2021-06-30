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
"""
包含元数据系统，所有初始类型的实现。
"""


from collections import OrderedDict

import attr

from metadata.type_system.core import MetaData, as_metadata


def add_common_person_attr(md_type, registry):
    if 'extend_attrs' not in md_type.metadata:
        md_type.metadata['extend_attrs'] = OrderedDict()
    if 'updated_by' in attr.fields_dict(md_type):
        md_type.metadata['extend_attrs']['updated_by_person'] = {'type_name': 'Staff', 'default': None}
    if 'created_by' in attr.fields_dict(md_type):
        md_type.metadata['extend_attrs']['created_by_person'] = {'type_name': 'Staff', 'default': None}


@as_metadata
@attr.s
class Asset(MetaData):
    """
    数据资产
    """

    __abstract__ = True
    __definable__ = True
    __metadata__ = {
        'register_callbacks': [add_common_person_attr],
        'dgraph': {
            'common_predicates': {
                'created_by': {'dgraph': {'predicate_name': 'created_by', 'index': ['exact', 'trigram']}},
                'created_by_person': {'dgraph': {'predicate_name': 'created_by_person'}},
                'created_at': {'dgraph': {'index': ['hour'], 'predicate_name': 'created_at'}},
                'updated_by': {'dgraph': {'predicate_name': 'updated_by', 'index': ['exact', 'trigram']}},
                'updated_by_person': {'dgraph': {'predicate_name': 'updated_by_person'}},
                'updated_at': {'dgraph': {'index': ['hour'], 'predicate_name': 'updated_at'}},
                'active': {'dgraph': {'index': ['bool'], 'predicate_name': 'active'}},
            }
        },
    }


@as_metadata
@attr.s
class Entity(Asset):
    """数据实体-数据资产实体"""

    __abstract__ = True


@as_metadata
@attr.s
class Relation(Asset):
    """关联信息-数据实体之间的关联描述"""

    __abstract__ = True


@as_metadata
@attr.s
class AddOn(Asset):
    """附加信息-对数据属性的展开描述"""

    __abstract__ = True


@as_metadata
@attr.s
class Classification(Asset):
    """分类体系-标签系统特征系统等标注信息"""

    __abstract__ = True


@as_metadata
@attr.s
class Internal(Entity):
    """內构节点-用于临时处理"""

    role = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['hash']}})
