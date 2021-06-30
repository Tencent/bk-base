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

from datetime import datetime

import attr
from metadata_client.type_system_lite.core import LocalMetaData, as_local_metadata


class Empty(object):
    pass


def as_dm_local_metadata(maybe_cls, registry=None, **kwargs):
    """
    修改 as_local_metadata 不能继承原类的方法
    """
    local_cls = as_local_metadata(maybe_cls, registry=None, **kwargs)

    @classmethod
    def part_init(cls, **kwargs):
        actual_kwargs = dict()
        for field in attr.fields(cls):
            if field.name not in kwargs:
                actual_kwargs[field.name] = Empty
            else:
                actual_kwargs[field.name] = kwargs[field.name]

        return cls(**actual_kwargs)

    # 补充原类的方法，写法待优化
    setattr(local_cls, "part_init", part_init)
    return local_cls


@as_dm_local_metadata
class ProjectInfo(LocalMetaData):
    project_id = attr.ib(
        type=int,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["int"]}},
    )
    project_name = attr.ib(type=str)
    bk_app_code = attr.ib(type=str)
    description = attr.ib(type=str)
    created_by = attr.ib(
        type=str,
    )
    updated_by = attr.ib(
        type=str,
    )
    deleted_by = attr.ib(type=str, default="")
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    deleted_at = attr.ib(type=datetime, factory=datetime.now)


@as_dm_local_metadata
class ResultTable(LocalMetaData):
    bk_biz_id = attr.ib(
        type=int, metadata={"dgraph": {"count": True, "index": ["int"]}}
    )
    project_id = attr.ib(
        type=int, metadata={"dgraph": {"count": True, "index": ["int"]}}
    )
    project = attr.ib(type=ProjectInfo)
    result_table_id = attr.ib(
        type=str,
        metadata={
            "identifier": True,
            "dgraph": {"count": True, "index": ["exact", "trigram"]},
        },
    )
    result_table_name = attr.ib(
        type=str,
    )
    result_table_name_alias = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}}
    )
    processing_type = attr.ib(type=str)
    created_by = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["exact", "trigram"]}}
    )
    updated_by = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["exact", "trigram"]}}
    )
    description = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}}
    )
    result_table_type = attr.ib(type=str, default=None)
    generate_type = attr.ib(
        type=str,
        default="user",
        metadata={
            "dgraph": {
                "count": True,
                "index": [
                    "exact",
                ],
            }
        },
    )
    sensitivity = attr.ib(type=str, default="public")
    count_freq = attr.ib(type=int, default=0)
    count_freq_unit = attr.ib(type=str, default="s")
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    is_managed = attr.ib(type=bool, default=True)
    platform = attr.ib(type=str, default="bkdata")
    data_category = attr.ib(type=str, default="")


@as_dm_local_metadata
class AccessRawData(LocalMetaData):
    id = attr.ib(
        type=int,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["int"]}},
    )
    bk_biz_id = attr.ib(
        type=int, metadata={"dgraph": {"count": True, "index": ["int"]}}
    )
    raw_data_name = attr.ib(
        type=str,
        metadata={
            "dgraph": {
                "count": True,
                "index": [
                    "exact",
                    "trigram",
                ],
            }
        },
    )
    raw_data_alias = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}}
    )
    data_source = attr.ib(type=str)
    data_scenario = attr.ib(type=str)
    bk_app_code = attr.ib(type=str)
    storage_partitions = attr.ib(type=int)
    description = attr.ib(
        type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}}
    )
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    storage_channel_id = attr.ib(
        type=int,
    )
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    data_encoding = attr.ib(type=str, default="UTF8")
    data_category = attr.ib(type=str, default=None)
    sensitivity = attr.ib(type=str, default="public")
    maintainer = attr.ib(type=str, default=None)
    active = attr.ib(type=bool, default=True)
    permission = attr.ib(type=str, default="all")


@as_dm_local_metadata
class Range(LocalMetaData):
    # 广度得分及指标
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    project_count = attr.ib(type=int)
    biz_count = attr.ib(type=int)
    depth = attr.ib(type=int)
    node_count_list = attr.ib(type=str)
    node_count = attr.ib(type=int)
    weighted_node_count = attr.ib(type=float)
    app_code_count = attr.ib(type=int)
    range_score = attr.ib(type=float)
    normalized_range_score = attr.ib(
        type=float, metadata={"dgraph": {"count": True, "index": ["float"]}}
    )
    range_score_ranking = attr.ib(type=float)


@as_dm_local_metadata
class Heat(LocalMetaData):
    # 热度得分及指标
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    query_count = attr.ib(type=int)
    heat_score = attr.ib(
        type=float, metadata={"dgraph": {"count": True, "index": ["float"]}}
    )
    heat_score_ranking = attr.ib(type=float)
    day_query_count = attr.ib(type=int, default=0)
    day_query_count_dict = attr.ib(type=str, default="{}")
    app_code_dict = attr.ib(type=str, default="{}")
    queue_service_count = attr.ib(type=int, default=0)


@as_dm_local_metadata
class Importance(LocalMetaData):
    # 重要度得分及指标
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    dataset_score = attr.ib(type=float)
    biz_score = attr.ib(type=float)
    project_score = attr.ib(type=float)
    oper_state_name = attr.ib(type=str)
    oper_state = attr.ib(type=float)
    bip_grade_name = attr.ib(type=str)
    bip_grade_id = attr.ib(type=float)
    app_important_level_name = attr.ib(type=str)
    app_important_level = attr.ib(type=float)
    importance_score_ranking = attr.ib(type=float)
    importance_score = attr.ib(
        type=float, metadata={"dgraph": {"count": True, "index": ["float"]}}
    )
    is_bip = attr.ib(type=bool, default=True)
    active = attr.ib(type=bool, default=True)


@as_dm_local_metadata
class AssetValue(LocalMetaData):
    # 价值得分及指标
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    range_id = attr.ib(type=str)
    range = attr.ib(type=Range)
    normalized_range_score = attr.ib(type=float)
    heat_id = attr.ib(type=str)
    heat = attr.ib(type=Heat)
    heat_score = attr.ib(type=float)
    importance_id = attr.ib(type=str)
    importance = attr.ib(type=Importance)
    importance_score = attr.ib(type=float)
    asset_value_score_ranking = attr.ib(type=float)
    asset_value_score = attr.ib(
        type=float, metadata={"dgraph": {"count": True, "index": ["float"]}}
    )


@as_dm_local_metadata
class StorageCapacity(LocalMetaData):
    # 存储成本
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    hdfs_capacity = attr.ib(type=float)
    tspider_capacity = attr.ib(type=float)
    total_capacity = attr.ib(type=float)
    log_capacity = attr.ib(type=float)
    capacity_score = attr.ib(
        type=float, metadata={"dgraph": {"count": True, "index": ["float"]}}
    )


@as_dm_local_metadata
class Cost(LocalMetaData):
    # 成本
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    capacity_id = attr.ib(type=str)
    capacity = attr.ib(type=StorageCapacity)
    capacity_score = attr.ib(type=float)


@as_dm_local_metadata
class LifeCycle(LocalMetaData):
    # 生命周期指标
    id = attr.ib(
        type=str,
        metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}},
    )
    target_id = attr.ib(type=str)
    target_type = attr.ib(type=str)
    range_id = attr.ib(type=str)
    range = attr.ib(type=Range)
    heat_id = attr.ib(type=str)
    heat = attr.ib(type=Heat)
    importance_id = attr.ib(type=str)
    importance = attr.ib(type=Importance)
    asset_value_id = attr.ib(type=str)
    asset_value = attr.ib(type=AssetValue)
    cost_id = attr.ib(type=str)
    cost = attr.ib(type=Cost)
    assetvalue_to_cost = attr.ib(type=float)
    assetvalue_to_cost_ranking = attr.ib(type=float)


@attr.s
class DataTraceInfo:
    """数据足迹建模"""

    id = attr.ib(type=str)
    dispatch_id = attr.ib(type=str)
    data_set_id = attr.ib(type=str)
    created_by = attr.ib(type=str)
    opr_type = attr.ib(type=str)
    opr_sub_type = attr.ib(type=str, default="")
    description = attr.ib(type=str, default="")
    opr_info = attr.ib(type=str, default="{}")  # json
    created_at = attr.ib(type=datetime, factory=datetime.now)
