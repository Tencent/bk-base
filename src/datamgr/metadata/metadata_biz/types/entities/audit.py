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

import sys
from datetime import datetime

import attr

from metadata.type_system.basic_type import Entity
from metadata.type_system.core import as_metadata
from metadata_biz.types.entities.data_set import AccessRawData, ResultTable
from metadata_biz.types.entities.management import ProjectInfo, Staff

python_string_type = str
default_json = '{}'


@as_metadata
@attr.s
class Audit(Entity):
    """审计信息-用于记录数据资产变化流水"""

    __abstract__ = True


@as_metadata
@attr.s
class WhoQueryRT(Audit):
    id = attr.ib(type=python_string_type, metadata={'identifier': True, 'dgraph': {'index': ['exact', 'trigram']}})
    result_table_id = attr.ib(type=python_string_type)
    result_table = attr.ib(type=ResultTable)
    request_user_id = attr.ib(type=python_string_type)
    request_user = attr.ib(type=Staff)
    bk_app_code = attr.ib(type=python_string_type)
    query_cnt = attr.ib(type=int)  # 查询次数
    created_at = attr.ib(type=datetime, factory=datetime.now)  # 本条记录创建时间
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class DatabusMigrateTask(Audit):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}, 'event': ['on_create']})
    task_type = attr.ib(type=python_string_type)
    task_label = attr.ib(type=python_string_type)
    result_table_id = attr.ib(type=python_string_type)
    geog_area = attr.ib(type=python_string_type)
    start = attr.ib(type=python_string_type)
    end = attr.ib(type=python_string_type)
    source = attr.ib(type=python_string_type)
    source_config = attr.ib(type=python_string_type)
    source_name = attr.ib(type=python_string_type)
    dest = attr.ib(type=python_string_type)
    dest_config = attr.ib(type=python_string_type)
    dest_name = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type, metadata={'event': ['on_update']})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type, default='')
    parallelism = attr.ib(type=int, default=1)
    overwrite = attr.ib(type=bool, default=False)
    input = attr.ib(type=int, default=0)
    output = attr.ib(type=int, default=0)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ProjectData(Entity):
    """项目申请RT的关联记录"""

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    # project_id为申请该数据源的项目，非数据所属的项目
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(type=ProjectInfo)
    bk_biz_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    result_table_id = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    result_table = attr.ib(type=ResultTable)
    created_by = attr.ib(type=str)
    description = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ProjectRawData(Entity):
    """项目申请数据源的关联记录"""

    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    # project_id为申请该数据源的项目，非数据所属的项目
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(type=ProjectInfo)
    bk_biz_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    raw_data_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    raw_data = attr.ib(type=AccessRawData)
    created_by = attr.ib(type=str)
    description = attr.ib(type=str)
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
