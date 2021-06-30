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

# 老版本model模块model

from sqlalchemy import TIMESTAMP, Column, Enum, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER, MEDIUMTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base._db_name_ = 'bkdata_algorithm'


class AlgorithmBaseModel(Base):
    __tablename__ = 'algorithm_base_model'

    model_id = Column(String(64), primary_key=True)
    model_name = Column(String(250), nullable=False)
    description = Column(String(256), nullable=False)
    project_id = Column(INTEGER(11), nullable=False)
    model_type = Column(Enum('existed', 'flow', 'process'), nullable=False)
    is_public = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(32))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(32))
    updated_at = Column(TIMESTAMP)
    scene_name = Column(String(64))
    model_alias = Column(String(64))
    properties = Column(MEDIUMTEXT)
    train_mode = Column(String(32))


class AlgorithmModelInstance(Base):
    __tablename__ = 'algorithm_model_instance'

    instance_id = Column(INTEGER(11), primary_key=True)
    model_release_id = Column(INTEGER(11), index=True)
    model_node_id = Column(String(128))
    model_version_id = Column(INTEGER(11), nullable=False)
    model_id = Column(String(64), nullable=False, index=True)
    biz_id = Column(INTEGER(11), nullable=False)
    project_id = Column(INTEGER(11), nullable=False)
    flow_id = Column(INTEGER(11))
    flow_node_id = Column(INTEGER(11))
    training_job_id = Column(String(127))
    training_when_serving = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    serving_job_id = Column(String(127), nullable=False)
    instance_config = Column(MEDIUMTEXT)
    instance_status = Column(
        Enum('disable', 'enable', 'serving', 'restart'), nullable=False, server_default=text("'disable'")
    )
    serving_mode = Column(Enum('realtime', 'offline', 'api'))
    api_serving_parallel = Column(INTEGER(11))
    training_reuse = Column(TINYINT(1), server_default=text("'0'"))
    reuse_instance_name = Column(String(64))
    training_from_instance = Column(INTEGER(11))
    is_deleted = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    created_by = Column(String(32))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(32))
    updated_at = Column(TIMESTAMP, nullable=False, server_default=text("'0000-00-00 00:00:00'"))
    api_serving_key = Column(String(64))
    auto_upgrade = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    prediction_feedback = Column(TINYINT(4), server_default=text("'1'"))


class SampleSet(Base):
    __tablename__ = 'sample_set'

    sample_set_id = Column(INTEGER(11), primary_key=True)
    sample_set_name = Column(String(64), nullable=False)
    project_id = Column(INTEGER(11), nullable=False, index=True)
    scene_name = Column(String(64), nullable=False)
    is_public = Column(TINYINT(1), server_default=text("'0'"))
    group_fields = Column(Text)
    properties = Column(MEDIUMTEXT)
    ts_properties = Column(MEDIUMTEXT)
    sample_render = Column(String(128))
    sample_latest_time = Column(TIMESTAMP)
    samples_table_name = Column(String(64))
    description = Column(String(255), server_default=text("''"))
    is_deleted = Column(TINYINT(1), server_default=text("'0'"))
    created_at = Column(TIMESTAMP)
    created_by = Column(String(32))
    updated_at = Column(TIMESTAMP)
    updated_by = Column(String(32))
    deleted_at = Column(TIMESTAMP)
    deleted_by = Column(String(32))
    sample_set_status = Column(String(32), server_default=text("'inited'"))


class StorageSampleSet(Base):
    __tablename__ = 'storage_sample_set'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    physical_table_name = Column(String(64), nullable=False)
    expires = Column(String(32))
    storage_config = Column(Text, nullable=False)
    storage_cluster_config_id = Column(INTEGER(11), nullable=False)
    storage_cluster_name = Column(String(32), nullable=False)
    storage_cluster_type = Column(String(32), nullable=False)
    active = Column(TINYINT(4))
    priority = Column(INTEGER(11), nullable=False, server_default=text("'0'"), comment='优先级, 优先级越高, 先使用这个存储')
    generate_type = Column(
        String(32), nullable=False, server_default=text("'system'"), comment='用户还是系统产生的user / system'
    )
    content_type = Column(String(32))
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class TimeseriesSampleConfig(Base):
    __tablename__ = 'timeseries_sample_config'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    ts_depend = Column(String(32), nullable=False, server_default=text("'0d'"), comment='历史依赖')
    ts_freq = Column(String(32), nullable=False, server_default=text("'0'"), comment='统计频率')
    ts_depend_config = Column(Text)
    ts_freq_config = Column(Text)
    description = Column(String(255), comment='描述')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(TIMESTAMP)


class TimeseriesSampleFragment(Base):
    __tablename__ = 'timeseries_sample_fragments'

    id = Column(INTEGER(11), primary_key=True)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    generate_type = Column(
        String(32), nullable=False, server_default=text("'manual'"), comment='来源manual还是sample_feedback'
    )
    labels = Column(Text, comment='这个片段的labels信息')
    status = Column(String(32), nullable=False, server_default=text("'developing'"))
    labels_type = Column(
        String(32), nullable=False, server_default=text("'timeseries'"), comment='Label的类别,默认是teimseries'
    )
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    line_id = Column(String(255), nullable=False)
    group_fields = Column(Text)
    group_dimension = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(TIMESTAMP)


class SampleFeature(Base):
    __tablename__ = 'sample_features'

    id = Column(INTEGER(11), primary_key=True)
    sample_set_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(255), nullable=False)
    field_alias = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    field_index = Column(INTEGER(11), nullable=False)
    is_dimension = Column(TINYINT(4), nullable=False, server_default=text("'0'"))
    field_type = Column(String(32), nullable=False)
    active = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    origin = Column(Text, comment='从哪些原始的样本集字段计算出来的。列表 []')
    attr_type = Column(String(32), nullable=False, comment='字段的属性类型 feature / metric / label / ts_field')
    properties = Column(Text)
    generate_type = Column(String(32), nullable=False, comment='原生 还是 生成的, origin / generate')
    field_evaluation = Column(Text, comment='字段评估结果,如特征重要性 importance 和 质量评分 quality')
    parents = Column(Text, comment='父字段')
    feature_transform_node_id = Column(INTEGER(11), comment='特征来自于哪个特征准备的节点')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(TIMESTAMP)


class SampleResultTable(Base):
    __tablename__ = 'sample_result_table'

    id = Column(INTEGER(11), primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(INTEGER(11), nullable=False)
    fields_mapping = Column(Text, comment='字段映射信息,json结构')
    bk_biz_id = Column(INTEGER(11), nullable=False, comment='样本集业务')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), nullable=False, comment='更新者')
    updated_at = Column(TIMESTAMP)
