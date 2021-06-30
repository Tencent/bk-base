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

from datetime import datetime
from typing import List

import attr

from metadata.type_system.basic_type import Asset, Classification, Relation
from metadata.type_system.core import StringDeclaredType, as_metadata
from metadata.util.common import Empty


@as_metadata
@attr.s
class Tag(Classification):
    id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    code = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact']}})
    parent_id = attr.ib(type=int)
    seq_index = attr.ib(type=int)
    sync = attr.ib(type=int)
    tag_type = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    kpath = attr.ib(type=int)
    icon = attr.ib(type=str)
    description = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    parent_tag = attr.ib(type=StringDeclaredType.typed('Tag'), default=Empty)
    alias = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    targets = attr.ib(type=List[Asset], default=tuple([Empty]))


@as_metadata
@attr.s
class TagTarget(Relation):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    target_id = attr.ib(
        type=str,
    )
    target_type = attr.ib(
        type=str,
    )
    target = attr.ib(type=Asset)
    tag_code = attr.ib(type=str)
    tag = attr.ib(type=Tag)
    source_tag_code = attr.ib(type=str)
    source_tag = attr.ib(type=Tag)
    probability = attr.ib(
        type=float,
    )
    checked = attr.ib(
        type=bool,
    )
    description = attr.ib(
        type=str,
    )
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class CustomTagTarget(Relation):
    __abstract__ = True
