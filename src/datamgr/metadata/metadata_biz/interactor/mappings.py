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

from sqlalchemy.ext.declarative import DeclarativeMeta

from metadata.db_models.meta_service import replica_models_collection
from metadata_biz.db_models.bkdata import managed_models_collection
from metadata_biz.interactor.operators import (
    JsonFieldMySQLReplicaOperator,
    LineageDgraphOperator,
    TagTargetDgraphOperator,
)
from metadata_biz.types import default_registry

# 元数据类型和db模型mappings
md_db_model_set_mappings = [
    (default_registry, managed_models_collection),
]
md_db_model_mappings = []

for md_type_registry, model in md_db_model_set_mappings:
    md_info = md_type_registry
    model_info = model
    for k, v in md_info.items():
        if k in model_info:
            md_db_model_mappings.append([v, model_info[k]])

# mysql后端原始表和映射表model声明模块对应关系
original_replica_models_set_mappings = [(managed_models_collection, replica_models_collection)]
# mysql后端原始表和映射表mappings
original_replica_db_model_mappings = {}
for origin, replica in original_replica_models_set_mappings:
    for k, v in origin.items():
        if k in replica and isinstance(v, DeclarativeMeta):
            original_replica_db_model_mappings[v] = replica[k]

# action和md对应operator或converter
dgraph_action_operators = [LineageDgraphOperator, TagTargetDgraphOperator]
replica_mysql_action_operators = [JsonFieldMySQLReplicaOperator]
db_model_md_name_mappings = {v.__tablename__: k.__name__ for k, v in md_db_model_mappings}
