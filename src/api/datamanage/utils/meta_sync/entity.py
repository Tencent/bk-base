# coding=utf-8
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
import attr

from metadata_client.type_system_lite.core import LocalMetaData, as_local_metadata


@as_local_metadata
class Range(LocalMetaData):
    # 广度得分及指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'count': True, 'index': ['exact']}})
    project_count = attr.ib(type=int)
    biz_count = attr.ib(type=int)
    depth = attr.ib(type=int)
    node_count_list = attr.ib(type=str)
    node_count = attr.ib(type=int)
    weighted_node_count = attr.ib(type=float)
    app_code_count = attr.ib(type=int)
    range_score = attr.ib(type=float)
    normalized_range_score = attr.ib(type=float, metadata={'dgraph': {'count': True, 'index': ['float']}})


@as_local_metadata
class Heat(LocalMetaData):
    # 热度得分及指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'count': True, 'index': ['exact']}})
    query_count = attr.ib(type=int)
    heat_score = attr.ib(type=float, metadata={'dgraph': {'count': True, 'index': ['float']}})
    day_query_count = attr.ib(type=int, default=0)
    day_query_count_dict = attr.ib(type=str, default='{}')
    app_code_dict = attr.ib(type=str, default='{}')
    queue_service_count = attr.ib(type=int, default=0)


@as_local_metadata
class LifeCycle(LocalMetaData):
    # 生命周期指标
    id = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'count': True, 'index': ['exact']}})
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    range_id = attr.ib(type=str)
    heat_id = attr.ib(type=str)
