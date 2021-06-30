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

import attr

from metadata.type_system.basic_type import Entity, Relation
from metadata.type_system.core import as_metadata
from metadata_biz.types.entities.data_set import ResultTable
from metadata_biz.types.entities.management import ProjectInfo
from metadata_biz.types.entities.procedure import DataProcessing


@as_metadata
@attr.s
class DataflowInfo(Entity):
    flow_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    flow_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    project_id = attr.ib(type=int)
    project = attr.ib(type=ProjectInfo)
    status = attr.ib(type=str)
    is_locked = attr.ib(type=int)
    latest_version = attr.ib(type=str)
    bk_app_code = attr.ib(type=str)
    locked_by = attr.ib(type=str)
    locked_at = attr.ib(type=datetime)
    locked_description = attr.ib(type=str)
    description = attr.ib(type=str)
    tdw_conf = attr.ib(type=str)
    custom_calculate_id = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    active = attr.ib(type=bool, default=False)
    flow_tag = attr.ib(type=str, default=None)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DataflowNodeInfo(Entity):
    node_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    flow_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    flow = attr.ib(type=DataflowInfo)
    node_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    node_config = attr.ib(type=str)
    node_type = attr.ib(type=str)
    status = attr.ib(type=str, metadata={'event': ['on_update']})
    latest_version = attr.ib(type=str)
    running_version = attr.ib(type=str)
    description = attr.ib(type=str)
    frontend_info = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DataflowNodeRelation(Relation):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    project_id = attr.ib(type=int)
    flow_id = attr.ib(type=int)
    flow = attr.ib(type=DataflowInfo)
    node_id = attr.ib(type=int)
    node = attr.ib(type=DataflowNodeInfo)
    result_table_id = attr.ib(type=str)
    result_table = attr.ib(type=ResultTable)
    node_type = attr.ib(type=str)
    generate_type = attr.ib(type=str)
    is_head = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class DataflowProcessing(Relation):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    flow_id = attr.ib(type=int)
    node_id = attr.ib(type=int)
    node = attr.ib(type=DataflowNodeInfo)
    processing_id = attr.ib(type=str)
    processing = attr.ib(type=DataProcessing)
    processing_type = attr.ib(type=str)
    description = attr.ib(type=str)


@as_metadata
@attr.s
class BksqlFunctionDevConfig(Entity):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    func_name = attr.ib(type=str, metadata={'dgraph': {'index': ['term']}})
    version = attr.ib(type=str, metadata={'dgraph': {'index': ['hash']}})
    func_alias = attr.ib(type=str)
    func_language = attr.ib(type=str)
    func_udf_type = attr.ib(type=str)
    input_type = attr.ib(type=str)
    return_type = attr.ib(type=str)
    explain = attr.ib(type=str)
    example = attr.ib(type=str)
    example_return_value = attr.ib(type=str)
    code_config = attr.ib(type=str)
    debug_id = attr.ib(type=str)
    support_framework = attr.ib(type=str)
    description = attr.ib(type=str)
    locked_by = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    sandbox_name = attr.ib(type=str, default='default')
    released = attr.ib(type=int, default=0)
    locked = attr.ib(type=int, default=0)
    locked_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
