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

from sqlalchemy import (
    TIMESTAMP,
    Column,
    DateTime,
    Enum,
    Float,
    Index,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, LONGTEXT, MEDIUMTEXT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base._db_name_ = 'bkdata_basic'


class AccessRawData(Base):
    __tablename__ = 'access_raw_data'

    id = Column(INTEGER(11), primary_key=True)
    bk_biz_id = Column(INTEGER(11), nullable=False)
    raw_data_name = Column(String(128), nullable=False)
    raw_data_alias = Column(String(128), nullable=False)
    sensitivity = Column(String(32), server_default=text("'public'"))
    data_source = Column(String(32), nullable=False)
    data_encoding = Column(String(32), server_default=text("'UTF8'"))
    data_category = Column(String(32), server_default=text("'UTF8'"))
    data_scenario = Column(String(128), nullable=False)
    bk_app_code = Column(String(128), nullable=False)
    storage_channel_id = Column(INTEGER(11))
    storage_partitions = Column(INTEGER(11))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)
    maintainer = Column(String(255))
    active = Column(INTEGER(11), server_default=text("'1'"))
    permission = Column(String(30), server_default=text("'all'"))
    topic_name = Column(String(128), nullable=False)


class BelongsToConfig(Base):
    __tablename__ = 'belongs_to_config'

    belongs_id = Column(String(255), primary_key=True)
    belongs_name = Column(String(255), nullable=False)
    belongs_alias = Column(String(255))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text)


class ClusterGroupConfig(Base):
    __tablename__ = 'cluster_group_config'

    cluster_group_id = Column(String(255), primary_key=True)
    cluster_group_name = Column(String(255), nullable=False)
    cluster_group_alias = Column(String(255))
    scope = Column(Enum('private', 'public'), nullable=False)
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)


class ContentLanguageConfig(Base):
    __tablename__ = 'content_language_config'

    id = Column(INTEGER(7), primary_key=True)
    content_key = Column(String(511), nullable=False)
    language = Column(String(64), nullable=False)
    content_value = Column(MEDIUMTEXT, nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text)


class DataProcessing(Base):
    __tablename__ = 'data_processing'

    project_id = Column(INTEGER(11), nullable=False)
    processing_id = Column(String(255), primary_key=True)
    processing_alias = Column(String(255), nullable=False)
    processing_type = Column(String(32), nullable=False)
    platform = Column(String(32), nullable=False, server_default=text("'bkdata'"))
    generate_type = Column(String(32), nullable=False, server_default=text("'user'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class DataProcessingDel(Base):
    __tablename__ = 'data_processing_del'

    id = Column(INTEGER(7), primary_key=True)
    processing_id = Column(String(255), nullable=False)
    processing_content = Column(Text, nullable=False)
    processing_type = Column(String(32), nullable=False)
    status = Column(String(255), nullable=False)
    disabled_by = Column(String(50))
    disabled_at = Column(TIMESTAMP)
    deleted_by = Column(String(50), nullable=False, server_default=text("''"))
    deleted_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class DataProcessingRelation(Base):
    __tablename__ = 'data_processing_relation'

    id = Column(INTEGER(7), primary_key=True)
    data_directing = Column(String(128))
    data_set_type = Column(String(128))
    data_set_id = Column(String(255))
    storage_cluster_config_id = Column(INTEGER(11))
    channel_cluster_config_id = Column(INTEGER(11))
    storage_type = Column(String(32))
    processing_id = Column(String(255))


class DataTransferring(Base):
    __tablename__ = 'data_transferring'

    project_id = Column(INTEGER(11), nullable=False)
    transferring_id = Column(String(255), primary_key=True)
    transferring_alias = Column(String(255), nullable=False)
    transferring_type = Column(String(32), nullable=False)
    generate_type = Column(String(32), nullable=False, server_default=text("'user'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class DataTransferringDel(Base):
    __tablename__ = 'data_transferring_del'

    id = Column(INTEGER(7), primary_key=True)
    transferring_id = Column(String(255), nullable=False)
    transferring_content = Column(Text, nullable=False)
    transferring_type = Column(String(32), nullable=False)
    status = Column(String(255), nullable=False)
    disabled_by = Column(String(50))
    disabled_at = Column(TIMESTAMP)
    deleted_by = Column(String(50), nullable=False, server_default=text("''"))
    deleted_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class DataTransferringRelation(Base):
    __tablename__ = 'data_transferring_relation'

    id = Column(INTEGER(7), primary_key=True)
    data_directing = Column(String(128))
    data_set_type = Column(String(128))
    data_set_id = Column(String(255))
    storage_cluster_config_id = Column(INTEGER(11))
    channel_cluster_config_id = Column(INTEGER(11))
    storage_type = Column(String(32), nullable=False)
    transferring_id = Column(String(255))


class DatabusChannelClusterConfig(Base):
    __tablename__ = 'databus_channel_cluster_config'

    id = Column(INTEGER(11), primary_key=True)
    cluster_name = Column(String(32), nullable=False, unique=True)
    cluster_type = Column(String(32), nullable=False)
    cluster_role = Column(String(32), nullable=False)
    cluster_domain = Column(String(128), nullable=False)
    cluster_backup_ips = Column(String(128), nullable=False)
    cluster_port = Column(INTEGER(8), nullable=False)
    zk_domain = Column(String(128), nullable=False)
    zk_port = Column(INTEGER(8), nullable=False)
    zk_root_path = Column(String(128), nullable=False)
    active = Column(TINYINT(1), server_default=text("'1'"))
    priority = Column(INTEGER(8), server_default=text("'0'"))
    attribute = Column(String(128), server_default=text("'bkdata'"))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)
    ip_list = Column(Text)
    stream_to_id = Column(INTEGER(11))
    storage_name = Column(String(32), nullable=True)


class EncodingConfig(Base):
    __tablename__ = 'encoding_config'

    id = Column(INTEGER(10), primary_key=True)
    encoding_name = Column(String(128), nullable=False)
    encoding_alias = Column(String(128))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class FieldTypeConfig(Base):
    __tablename__ = 'field_type_config'

    field_type = Column(String(128), primary_key=True)
    field_type_name = Column(String(128), nullable=False)
    field_type_alias = Column(String(128), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class JobStatusConfig(Base):
    __tablename__ = 'job_status_config'

    status_id = Column(String(255), primary_key=True)
    status_name = Column(String(255), nullable=False)
    status_alias = Column(String(255))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text)


class ProcessingTypeConfig(Base):
    __tablename__ = 'processing_type_config'

    id = Column(INTEGER(7), primary_key=True)
    processing_type_name = Column(String(255), nullable=False, unique=True)
    processing_type_alias = Column(String(255), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text, nullable=False)


class ProjectClusterGroupConfig(Base):
    __tablename__ = 'project_cluster_group_config'

    id = Column(INTEGER(11), primary_key=True)
    project_id = Column(INTEGER(11), nullable=False)
    cluster_group_id = Column(String(256), nullable=False)
    created_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    description = Column(Text)


class ProjectData(Base):
    """项目申请RT的关联记录"""

    __tablename__ = 'project_data'

    id = Column(INTEGER(11), primary_key=True)
    # project_id为申请该数据源的项目，非数据所属的项目
    project_id = Column(INTEGER(11), nullable=False)
    bk_biz_id = Column(INTEGER(11), nullable=False)
    result_table_id = Column(String(128))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    description = Column(Text)


class ProjectRawData(Base):
    """项目申请数据源的关联记录"""

    __tablename__ = 'project_rawdata'

    id = Column(INTEGER(11), primary_key=True)
    # project_id为申请该数据源的项目，非数据所属的项目
    project_id = Column(INTEGER(11), nullable=False)
    bk_biz_id = Column(INTEGER(11), nullable=False)
    raw_data_id = Column(INTEGER(11), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    description = Column(Text)


class ProjectDel(Base):
    __tablename__ = 'project_del'

    id = Column(INTEGER(7), primary_key=True)
    project_id = Column(INTEGER(11), nullable=False)
    project_name = Column(String(255), nullable=False)
    project_content = Column(Text, nullable=False)
    status = Column(String(255), nullable=False)
    disabled_by = Column(String(50))
    disabled_at = Column(TIMESTAMP)
    deleted_by = Column(String(50), nullable=False, server_default=text("''"))
    deleted_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class ProjectInfo(Base):
    __tablename__ = 'project_info'

    project_id = Column(INTEGER(7), primary_key=True)
    project_name = Column(String(255), nullable=False, unique=True)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    bk_app_code = Column(String(255), nullable=False)
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    deleted_by = Column(String(50))
    deleted_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class ResultTable(Base):
    __tablename__ = 'result_table'

    bk_biz_id = Column(INTEGER(7), nullable=False)
    project_id = Column(INTEGER(11))
    result_table_id = Column(String(255), primary_key=True)
    result_table_name = Column(String(255), nullable=False)
    result_table_name_alias = Column(String(255), nullable=False)
    result_table_type = Column(String(32))
    processing_type = Column(String(32), nullable=False)
    generate_type = Column(String(32), nullable=False, server_default=text("'user'"))
    sensitivity = Column(String(32), server_default=text("'public'"))
    count_freq = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)
    count_freq_unit = Column(String(32), nullable=False, server_default=text("'s'"))
    is_managed = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    platform = Column(String(32), nullable=False, server_default=text("'bkdata'"))
    data_category = Column(String(32), server_default=text("'UTF8'"))


class ResultTableDel(Base):
    __tablename__ = 'result_table_del'

    id = Column(INTEGER(7), primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    result_table_content = Column(Text, nullable=False)
    status = Column(String(255), nullable=False)
    disabled_by = Column(String(50))
    disabled_at = Column(TIMESTAMP)
    deleted_by = Column(String(50), nullable=False, server_default=text("''"))
    deleted_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class ResultTableField(Base):
    __tablename__ = 'result_table_field'
    __table_args__ = (Index('rt_field', 'result_table_id', 'field_name', unique=True),)

    id = Column(INTEGER(7), primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    field_index = Column(INTEGER(7), nullable=False)
    field_name = Column(String(255), nullable=False)
    field_alias = Column(String(255), nullable=False)
    description = Column(Text)
    field_type = Column(String(255), nullable=False)
    is_dimension = Column(TINYINT(1), nullable=False)
    origins = Column(String(1024))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    roles = Column(String(255))


class ResultTableTypeConfig(Base):
    __tablename__ = 'result_table_type_config'

    id = Column(INTEGER(7), primary_key=True)
    result_table_type_name = Column(String(255), nullable=False, unique=True)
    result_table_type_alias = Column(String(255))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text)


class StorageClusterConfig(Base):
    __tablename__ = 'storage_cluster_config'
    __table_args__ = (Index('cluster_name', 'cluster_name', 'cluster_type', unique=True),)

    id = Column(INTEGER(11), primary_key=True)
    cluster_name = Column(String(128), nullable=False)
    cluster_type = Column(String(128))
    cluster_group = Column(String(128), nullable=False)
    connection_info = Column(Text)
    priority = Column(INTEGER(11), nullable=False)
    version = Column(String(128), nullable=False)
    belongs_to = Column(String(128), server_default=text("'bkdata'"))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)
    expires = Column(Text)


class StorageClusterExpiresConfig(Base):
    __tablename__ = 'storage_cluster_expires_config'
    __table_args__ = (Index('cluster_name', 'cluster_name', 'cluster_type', 'cluster_subtype', unique=True),)

    id = Column(INTEGER(11), primary_key=True)
    cluster_name = Column(String(128), nullable=False)
    cluster_type = Column(String(128))
    cluster_subtype = Column(String(128))
    expires = Column(Text)
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)


class StorageResultTable(Base):
    __tablename__ = 'storage_result_table'
    __table_args__ = (
        Index('result_table_id_2', 'result_table_id', 'storage_cluster_config_id', 'physical_table_name', unique=True),
        Index('result_table_id', 'result_table_id', 'storage_cluster_config_id', 'physical_table_name'),
    )

    id = Column(INTEGER(11), primary_key=True)
    result_table_id = Column(String(128), nullable=False)
    storage_cluster_config_id = Column(INTEGER(11))
    physical_table_name = Column(String(255))
    expires = Column(String(45))
    storage_channel_id = Column(INTEGER(11))
    storage_config = Column(Text)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    priority = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    generate_type = Column(String(32), nullable=False, server_default=text("'user'"))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)
    data_type = Column(String(32))
    previous_cluster_name = Column(String(128))


class StorageScenarioConfig(Base):
    __tablename__ = 'storage_scenario_config'
    __table_args__ = (Index('storage_scenario_name', 'storage_scenario_name', 'cluster_type', unique=True),)

    id = Column(INTEGER(11), primary_key=True)
    storage_scenario_name = Column(String(128), nullable=False)
    storage_scenario_alias = Column(String(128), nullable=False)
    cluster_type = Column(String(128))
    active = Column(TINYINT(1), server_default=text("'1'"))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)


class StorageTask(Base):
    __tablename__ = 'storage_task'

    id = Column(INTEGER(11), primary_key=True)
    task_type = Column(String(64), nullable=False)
    result_table_ids = Column(String(64))
    cluster_type = Column(String(64))
    status = Column(String(64), nullable=False)
    begin_time = Column(DATETIME(fsp=6))
    end_time = Column(DATETIME(fsp=6))
    logs_zh = Column(LONGTEXT)
    logs_en = Column(LONGTEXT)
    expires = Column(String(64))
    created_by = Column(String(128), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    description = Column(Text)


class TimeFormatConfig(Base):
    __tablename__ = 'time_format_config'

    id = Column(INTEGER(10), primary_key=True)
    time_format_name = Column(String(128), nullable=False)
    time_format_alias = Column(String(128))
    time_format_example = Column(String(128))
    timestamp_len = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    format_unit = Column(String(12), server_default=text("'d'"))
    active = Column(TINYINT(1), server_default=text("'1'"))
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)


class TransferringTypeConfig(Base):
    __tablename__ = 'transferring_type_config'

    id = Column(INTEGER(7), primary_key=True)
    transferring_type_name = Column(String(255), nullable=False, unique=True)
    transferring_type_alias = Column(String(255), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text, nullable=False)


class PlatformConfig(Base):
    __tablename__ = 'platform_config'

    id = Column(INTEGER(7), primary_key=True)
    platform_name = Column(String(32), nullable=False, unique=True)
    platform_alias = Column(String(255), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(Text, nullable=False)


class TagAttribute(Base):
    __tablename__ = 'tag_attribute'

    id = Column(INTEGER(11), primary_key=True)
    tag_attr_id = Column(INTEGER(11), nullable=False)
    attr_value = Column(String(128))
    active = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50))
    created_at = Column(DATETIME(fsp=6))
    updated_by = Column(String(50))
    updated_at = Column(DATETIME(fsp=6))
    tag_target_id = Column(INTEGER(11), nullable=False)


class TagAttributeSchema(Base):
    __tablename__ = 'tag_attribute_schema'
    __table_args__ = (
        Index('tag_attribute_schema_tag_code_attr_name_099c9c56_uniq', 'tag_code', 'attr_name', unique=True),
    )

    id = Column(INTEGER(11), primary_key=True)
    tag_code = Column(String(128), nullable=False)
    attr_name = Column(String(128), nullable=False)
    attr_alias = Column(String(128))
    attr_type = Column(String(128), nullable=False)
    constraint_value = Column(LONGTEXT)
    attr_index = Column(INTEGER(11), nullable=False)
    description = Column(LONGTEXT)
    active = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50))
    created_at = Column(DATETIME(fsp=6))
    updated_by = Column(String(50))
    updated_at = Column(DATETIME(fsp=6))


class Tag(Base):
    __tablename__ = 'tag'

    id = Column(INTEGER(11), primary_key=True)
    code = Column(String(128), nullable=False, unique=True)
    alias = Column(String(128))
    parent_id = Column(INTEGER(11), nullable=False)
    seq_index = Column(INTEGER(11), nullable=False)
    sync = Column(INTEGER(11), nullable=False)
    tag_type = Column(String(32), nullable=False)
    kpath = Column(INTEGER(11), nullable=False)
    icon = Column(LONGTEXT)
    active = Column(INTEGER(11), nullable=False)
    created_by = Column(String(50), nullable=False)
    created_at = Column(DATETIME(fsp=6))
    updated_by = Column(String(50))
    updated_at = Column(DATETIME(fsp=6))
    description = Column(LONGTEXT)


class TagTarget(Base):
    __tablename__ = 'tag_target'
    __table_args__ = (
        Index(
            'tag_target_tag_code_target_type_tar_6ab827a6_uniq',
            'tag_code',
            'target_type',
            'target_id',
            'source_tag_code',
            unique=True,
        ),
    )

    id = Column(INTEGER(11), primary_key=True)
    target_id = Column(String(128), nullable=False)
    target_type = Column(String(64), nullable=False)
    tag_code = Column(String(128), nullable=False)
    probability = Column(Float(asdecimal=True))
    checked = Column(INTEGER(11), nullable=False)
    active = Column(INTEGER(11), nullable=False)
    description = Column(LONGTEXT)
    created_by = Column(String(50), nullable=False)
    created_at = Column(DATETIME(fsp=6))
    updated_by = Column(String(50))
    updated_at = Column(DATETIME(fsp=6))
    tag_type = Column(String(32))
    bk_biz_id = Column(INTEGER(11))
    project_id = Column(INTEGER(11))
    scope = Column(String(128), nullable=False)
    source_tag_code = Column(String(128), nullable=False)


class DatabusConnectorClusterConfig(Base):
    __tablename__ = 'databus_connector_cluster_config'

    id = Column(INTEGER(11), primary_key=True)
    cluster_name = Column(String(32), nullable=False, unique=True)
    cluster_type = Column(String(64), nullable=False, server_default=text("'kafka'"))
    channel_name = Column(String(32))
    cluster_rest_domain = Column(String(64), nullable=False, server_default=text("''"))
    cluster_rest_port = Column(INTEGER(11), nullable=False, server_default=text("'8083'"))
    cluster_bootstrap_servers = Column(String(255), nullable=False)
    cluster_props = Column(Text)
    consumer_bootstrap_servers = Column(String(255), server_default=text("''"))
    consumer_props = Column(Text)
    monitor_props = Column(Text)
    other_props = Column(Text)
    state = Column(String(20), server_default=text("'RUNNING'"))
    limit_per_day = Column(INTEGER(11), nullable=False, server_default=text("'1440000'"))
    priority = Column(INTEGER(11), nullable=False, server_default=text("'10'"))
    created_by = Column(String(128))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(128))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)
    module = Column(String(32), nullable=False, server_default=text("''"))
    component = Column(String(32), nullable=False, server_default=text("''"))
    ip_list = Column(Text)


class DatamonitorAlertConfig(Base):
    __tablename__ = 'datamonitor_alert_config'

    id = Column(INTEGER(11), primary_key=True)
    monitor_target = Column(Text, nullable=False)
    monitor_config = Column(Text, nullable=False)
    notify_config = Column(Text, nullable=False)
    trigger_config = Column(Text, nullable=False)
    convergence_config = Column(Text, nullable=False)
    receivers = Column(Text, nullable=False)
    extra = Column(String(1024))
    generate_type = Column(String(32), nullable=False)
    active = Column(TINYINT(1), nullable=False)
    status = Column(String(32))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)


class DatamonitorAlertConfigRelation(Base):
    __tablename__ = 'datamonitor_alert_config_relation'

    id = Column(INTEGER(11), primary_key=True)
    target_type = Column(String(64), nullable=False)
    flow_id = Column(String(64), nullable=False)
    node_id = Column(String(512))
    alert_config_id = Column(INTEGER(11), nullable=False)
    alert_config_type = Column(String(32), nullable=False)


class DmComparisonOperatorConfig(Base):
    __tablename__ = 'dm_comparison_operator_config'

    id = Column(INTEGER(11), primary_key=True)
    comparison_operator_name = Column(String(128), nullable=False)
    comparison_operator_alias = Column(String(128), nullable=False, server_default=text("''"))
    data_type_group = Column(String(128), nullable=False)
    description = Column(Text)
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmComputeModelConfig(Base):
    __tablename__ = 'dm_compute_model_config'

    id = Column(INTEGER(11), primary_key=True)
    field_type = Column(String(32), nullable=False, server_default=text("'dimension'"))
    data_type_group = Column(String(32), nullable=False, server_default=text("'string'"))
    compute_model = Column(Text, nullable=False)
    description = Column(Text)
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmConstraintConfig(Base):
    __tablename__ = 'dm_constraint_config'

    id = Column(INTEGER(11), primary_key=True)
    constraint_name = Column(String(128), nullable=False)
    rule = Column(Text, nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmDetaildataFieldConfig(Base):
    __tablename__ = 'dm_detaildata_field_config'

    id = Column(INTEGER(11), primary_key=True)
    standard_content_id = Column(INTEGER(11), nullable=False)
    source_record_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(128), nullable=False)
    field_alias = Column(String(128))
    field_type = Column(String(128), nullable=False)
    field_index = Column(INTEGER(11), nullable=False)
    unit = Column(String(128))
    description = Column(Text)
    constraint_id = Column(INTEGER(11))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmIndicatorFieldConfig(Base):
    __tablename__ = 'dm_indicator_field_config'

    id = Column(INTEGER(11), primary_key=True)
    standard_content_id = Column(INTEGER(11), nullable=False)
    source_record_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(128), nullable=False)
    field_alias = Column(String(128))
    field_type = Column(String(128), nullable=False)
    is_dimension = Column(TINYINT(1), server_default=text("'1'"))
    add_type = Column(String(128))
    unit = Column(String(128))
    field_index = Column(INTEGER(11), nullable=False)
    constraint_id = Column(INTEGER(11))
    compute_model_id = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    description = Column(Text)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmStandardConfig(Base):
    __tablename__ = 'dm_standard_config'
    __table_args__ = (Index('uniq_standard_name_category_id', 'standard_name', 'category_id', unique=True),)

    id = Column(INTEGER(11), primary_key=True)
    standard_name = Column(String(128), nullable=False)
    description = Column(Text)
    category_id = Column(INTEGER(11), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmStandardContentConfig(Base):
    __tablename__ = 'dm_standard_content_config'
    __table_args__ = (
        Index(
            'uniq_standard_content_name_standard_version_id',
            'standard_content_name',
            'standard_version_id',
            unique=True,
        ),
    )

    id = Column(INTEGER(11), primary_key=True)
    standard_version_id = Column(INTEGER(11), nullable=False)
    standard_content_name = Column(String(128), nullable=False)
    parent_id = Column(String(256), nullable=False)
    source_record_id = Column(INTEGER(11), nullable=False)
    standard_content_sql = Column(Text)
    category_id = Column(INTEGER(11), nullable=False)
    standard_content_type = Column(String(128), nullable=False)
    description = Column(Text)
    window_period = Column(Text)
    filter_cond = Column(Text)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmStandardVersionConfig(Base):
    __tablename__ = 'dm_standard_version_config'
    __table_args__ = (Index('uniq_standard_id_standard_version', 'standard_id', 'standard_version', unique=True),)

    id = Column(INTEGER(11), primary_key=True)
    standard_id = Column(INTEGER(11), nullable=False)
    standard_version = Column(String(128), nullable=False)
    description = Column(Text)
    standard_version_status = Column(String(32), nullable=False)
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmTaskConfig(Base):
    __tablename__ = 'dm_task_config'

    id = Column(INTEGER(11), primary_key=True)
    task_name = Column(String(128), nullable=False)
    project_id = Column(INTEGER(11), nullable=False)
    standard_version_id = Column(INTEGER(11), nullable=False)
    description = Column(Text)
    data_set_type = Column(String(128), nullable=False)
    data_set_id = Column(String(128))
    standardization_type = Column(INTEGER(11), nullable=False)
    flow_id = Column(INTEGER(11))
    task_status = Column(String(128), nullable=False)
    edit_status = Column(String(50), nullable=False)
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmTaskContentConfig(Base):
    __tablename__ = 'dm_task_content_config'

    id = Column(INTEGER(11), primary_key=True, comment='标准任务内容id')
    task_id = Column(INTEGER(11), nullable=False, comment='关联的dm_task_config表id')
    parent_id = Column(String(256), nullable=False, comment='父表id,格式例子:[1,2,3...]')
    standard_version_id = Column(INTEGER(11), nullable=False, comment='关联的dm_standard_version_config的id')
    standard_content_id = Column(INTEGER(11), nullable=False, server_default=text("'0'"), comment='标准内容id')
    source_type = Column(String(128), nullable=False, comment='标识来源[standard:标准模板sql;user:配置字段]')
    result_table_id = Column(String(128), nullable=False, comment='标准化结果表id')
    result_table_name = Column(String(128), nullable=False, comment='标准化结果表中文名')
    task_content_name = Column(String(128), nullable=False, comment='子流程名称')
    task_type = Column(String(128), nullable=False, comment='任务类型[detaildata/indicator]')
    task_content_sql = Column(Text, comment='标准化sql')
    node_config = Column(Text, comment='配置详情,json表达,参考dataflow的配置定义')
    created_by = Column(String(50), nullable=False, comment='created by')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='create time')
    updated_by = Column(String(50), comment='updated by')
    updated_at = Column(TIMESTAMP)
    request_body = Column(Text, comment='记录完整的请求信息')
    flow_body = Column(Text, comment='记录请求dataflow创建接口的完整请求信息')


class DmTaskDetail(Base):
    __tablename__ = 'dm_task_detail'
    __table_args__ = (Index('unified_index', 'standard_version_id', 'data_set_id', 'data_set_type', unique=True),)

    id = Column(INTEGER(11), primary_key=True, comment='自增id')
    task_id = Column(INTEGER(11), nullable=False, comment='关联的dm_task_config表id')
    task_content_id = Column(INTEGER(11), nullable=False, comment='关联dm_task_content_config表id')
    standard_version_id = Column(INTEGER(11), nullable=False, index=True, comment='关联的dm_standard_version_config的id')
    standard_content_id = Column(INTEGER(11), nullable=False, index=True, server_default=text("'0'"), comment='标准内容id')
    bk_biz_id = Column(INTEGER(11), comment='所属业务id')
    project_id = Column(INTEGER(11), comment='所属项目id')
    data_set_type = Column(String(128), nullable=False, comment='输入数据集类型:raw_data/result_table')
    data_set_id = Column(String(256), nullable=False, index=True, comment='标准化后的数据集id:result_table_id/data_id')
    task_type = Column(String(128), nullable=False, comment='详情类型[detaildata/indicator]')
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"), comment='是否有效')
    created_by = Column(String(50), nullable=False, comment='created by')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='create time')
    updated_by = Column(String(50), comment='updated by')
    updated_at = Column(TIMESTAMP)


class DmTaskDetaildataFieldConfig(Base):
    __tablename__ = 'dm_task_detaildata_field_config'

    id = Column(INTEGER(11), primary_key=True)
    task_content_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(128), nullable=False)
    field_alias = Column(String(128))
    field_type = Column(String(128), nullable=False)
    field_index = Column(INTEGER(11), nullable=False)
    compute_model = Column(Text, nullable=False)
    unit = Column(String(128))
    description = Column(Text)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmTaskIndicatorFieldConfig(Base):
    __tablename__ = 'dm_task_indicator_field_config'

    id = Column(INTEGER(11), primary_key=True)
    task_content_id = Column(INTEGER(11), nullable=False)
    field_name = Column(String(128), nullable=False)
    field_alias = Column(String(128))
    field_type = Column(String(128), nullable=False)
    is_dimension = Column(TINYINT(1), server_default=text("'0'"))
    add_type = Column(String(128), nullable=False)
    unit = Column(String(128))
    field_index = Column(INTEGER(11), nullable=False)
    compute_model = Column(Text, nullable=False)
    description = Column(Text)
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)


class DmUnitConfig(Base):
    __tablename__ = 'dm_unit_config'

    id = Column(INTEGER(11), primary_key=True)
    name = Column(String(64), nullable=False)
    alias = Column(String(64), nullable=False)
    category_name = Column(String(64), nullable=False)
    category_alias = Column(String(64), nullable=False)
    description = Column(Text)
    created_by = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(64))
    updated_at = Column(TIMESTAMP)


class ResourceGroupInfo(Base):
    __tablename__ = 'resource_group_info'

    resource_group_id = Column(String(45), primary_key=True, comment='资源组英文标识')
    group_name = Column(String(255), nullable=False, comment='资源组名称')
    group_type = Column(String(45), nullable=False, comment='资源组类型(public：公开,protected：业务内共享,private：私有)')
    bk_biz_id = Column(INTEGER(11), nullable=False, comment='所属业务，涉及到成本的核算')
    status = Column(
        String(45),
        nullable=False,
        server_default=text("'approve'"),
        comment='状态(approve:审批中,succeed:审批通过,reject:拒绝的,delete:删除的)',
    )
    process_id = Column(String(45), nullable=False, comment='资源组创建流程ID（当前生效的）')
    description = Column(Text, comment='描述')
    created_by = Column(String(45), nullable=False, comment='创建人')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(45), comment='更新人')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DatamapLayout(Base):
    """
    数据地图-页面显示相关内容的配置，比如节点左右位置、失效信息等等
    """

    __tablename__ = 'datamap_layout'

    id = Column(INTEGER(11), primary_key=True, comment='ID')
    category = Column(String(64), nullable=False, comment='分类的名称')
    loc = Column(TINYINT(1), nullable=False, comment='页面展示位置: 0-left,1-right;')
    seq_index = Column(INTEGER(11), nullable=False)
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"), comment='是否有效')
    config = Column(Text, comment='显示配置信息（JSON格式）比如显示颜色，显示大小等信息')
    description = Column(Text, comment='描述信息')
    created_by = Column(String(64), comment='创建者')
    created_at = Column(TIMESTAMP, comment='创建时间')
    updated_by = Column(String(64), comment='更新者')
    updated_at = Column(TIMESTAMP, comment='更新时间')


class TagTypeConfig(Base):
    """
    标签类型配置
    """

    __tablename__ = 'tag_type_config'

    id = Column(INTEGER(11), primary_key=True, comment='ID')
    name = Column(String(32), nullable=False, comment='标签类型名称')
    alias = Column(String(32), nullable=False, comment='标签类型中文')
    seq_index = Column(INTEGER(11), nullable=False, comment='位置')
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"), comment='是否有效')
    description = Column(Text, comment='描述信息')
    created_by = Column(String(64), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(64), comment='更新者')
    updated_at = Column(TIMESTAMP, comment='更新时间')


class DatabusMigrateTask(Base):
    """
    数据迁移记录
    """

    __tablename__ = 'databus_migrate_task'

    id = Column(INTEGER(11), primary_key=True)
    task_type = Column(String(8))
    task_label = Column(String(255))
    result_table_id = Column(String(255))
    geog_area = Column(String(16))
    parallelism = Column(INTEGER(11), server_default=text("'1'"))
    overwrite = Column(TINYINT(4), server_default=text("'0'"))
    start = Column(String(32))
    end = Column(String(32))
    source = Column(String(32))
    source_config = Column(Text)
    source_name = Column(String(255))
    dest = Column(String(32))
    dest_config = Column(Text)
    dest_name = Column(String(255))
    status = Column(String(12))
    input = Column(INTEGER(11), server_default=text("'0'"))
    output = Column(INTEGER(11), server_default=text("'0'"))
    description = Column(Text)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


# DataModel
class DmmModelInfo(Base):
    """
    数据模型
    """

    __tablename__ = 'dmm_model_info'

    model_id = Column(INTEGER(11), primary_key=True, comment='模型ID')
    model_name = Column(String(255), nullable=False, comment='模型名称，英文字母加下划线，全局唯一')
    model_alias = Column(String(255), nullable=False, comment='模型别名')
    model_type = Column(String(32), nullable=False, comment='模型类型，可选事实表、维度表')
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    description = Column(Text, comment='模型描述')
    publish_status = Column(String(32), nullable=False, comment='发布状态，可选 developing/published/re-developing')
    active_status = Column(
        String(32), nullable=False, server_default=text("'active'"), comment='可用状态, active/disabled/conflicting'
    )
    table_name = Column(String(255), nullable=False, comment='主表名称')
    table_alias = Column(String(255), nullable=False, comment='主表别名')
    step_id = Column(INTEGER(11), nullable=False, server_default=text("'0'"), comment='模型构建&发布完成步骤')
    latest_version_id = Column(String(64), nullable=False, comment='模型版本ID')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelField(Base):
    """
    数据模型字段
    """

    __tablename__ = 'dmm_model_field'

    id = Column(INTEGER(11), primary_key=True, comment='自增ID')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    field_name = Column(String(255), nullable=False, comment='字段名称')
    field_alias = Column(String(255), nullable=False, comment='字段别名')
    field_type = Column(String(32), nullable=False, comment='数据类型，可选 long/int/...')
    field_category = Column(String(32), nullable=False, comment='字段类型，可选维度、度量、主键')
    is_primary_key = Column(TINYINT(1), nullable=False, server_default=text("'0'"), comment='是否是主键 0：否，1：是')
    description = Column(Text, comment='字段描述')
    field_constraint_content = Column(Text, comment='字段约束')
    field_clean_content = Column(Text, comment='清洗规则')
    origin_fields = Column(Text, comment='计算来源字段')
    field_index = Column(INTEGER(11), nullable=False, comment='字段位置')
    source_model_id = Column(INTEGER(11), comment='来源模型，若有则为扩展字段，目前仅针对维度扩展字段使用')
    source_field_name = Column(String(255), comment='来源字段，若有则为扩展字段，目前仅针对维度扩展字段使用')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelTop(Base):
    """
    数据模型用户置顶表
    """

    __tablename__ = 'dmm_model_top'

    id = Column(INTEGER(11), nullable=False, primary_key=True, comment='主键ID')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmFieldConstraintConfig(Base):
    """
    字段约束表
    """

    __tablename__ = 'dmm_field_constraint_config'

    constraint_id = Column(String(64), nullable=False, primary_key=True, comment='字段约束ID')
    constraint_type = Column(String(32), nullable=False, comment='约束类型 通用约束/特定场景约束')
    constraint_name = Column(String(64), nullable=False, comment='约束名称')
    constraint_value = Column(Text, comment='约束规则')
    validator = Column(Text, comment='约束规则校验')
    description = Column(Text, comment='字段约束说明')
    editable = Column(TINYINT(1), comment='是否可以编辑')
    allow_field_type = Column(Text, comment='允许的字段数据类型')


class DmmModelRelation(Base):
    """
    数据模型关系表
    """

    __tablename__ = 'dmm_model_relation'

    id = Column(INTEGER(11), nullable=False, primary_key=True, comment='主键ID')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    field_name = Column(String(255), nullable=False, comment='字段名称')
    related_model_id = Column(INTEGER(11), nullable=False, comment='关联模型ID')
    related_field_name = Column(String(255), nullable=False, comment='关联字段名称')
    related_method = Column(
        String(32), nullable=False, comment='关联方式: left-join/right-join/inner-join', server_default=text("'left-join'")
    )
    related_model_version_id = Column(String(64), comment='维度模型版本ID', nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelRelease(Base):
    """
    数据模型发布记录
    """

    __tablename__ = 'dmm_model_release'

    version_id = Column(String(64), primary_key=True, nullable=False, comment='模型版本ID')
    version_log = Column(Text, nullable=False, comment='模型版本日志')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    model_content = Column(Text, nullable=False, comment='模型版本内容')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelCalculationAtom(Base):
    """
    数据模型统计口径表
    """

    __tablename__ = 'dmm_model_calculation_atom'

    calculation_atom_name = Column(String(255), nullable=False, comment='模型名称', primary_key=True)
    calculation_atom_alias = Column(String(255), nullable=False, comment='模型别名')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    description = Column(Text, comment='模型描述')
    field_type = Column(String(32), comment='数据类型')
    calculation_content = Column(Text, comment='统计方式')
    calculation_formula = Column(Text, comment='统计SQL')
    origin_fields = Column(Text, comment='计算来源字段')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelCalculationAtomImage(Base):
    """
    数据模型统计口径引用表
    """

    __tablename__ = 'dmm_model_calculation_atom_image'

    id = Column(INTEGER(11), nullable=False, comment='主键ID', primary_key=True)
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    calculation_atom_name = Column(String(255), nullable=False, comment='模型名称')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelIndicator(Base):
    """
    数据模型指标表
    """

    __tablename__ = 'dmm_model_indicator'

    indicator_name = Column(String(255), nullable=False, comment='指标名称', primary_key=True)
    indicator_alias = Column(String(255), comment='指标别名')
    model_id = Column(INTEGER(11), nullable=False, comment='模型ID')
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    description = Column(Text, comment='指标描述', nullable=True)
    calculation_atom_name = Column(String(255), comment='模型名称')
    aggregation_fields = Column(Text, comment='聚合字段列表')
    filter_formula = Column(Text, comment='过滤SQL', nullable=True)
    condition_fields = Column(Text, comment='过滤字段，若有过滤逻辑，则该字段有效', nullable=True)
    scheduling_type = Column(String(32), comment='调度类型')
    scheduling_content = Column(Text, comment='调度内容')
    parent_indicator_name = Column(String(255), comment='指标名称', nullable=True)
    hash = Column(String(64), comment='指标关键参数对应hash值', nullable=True)
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmCalculationFunctionConfig(Base):
    """
    SQL 统计函数表
    """

    __tablename__ = 'dmm_calculation_function_config'

    function_name = Column(String(64), comment='SQL 统计函数名称', primary_key=True)
    output_type = Column(String(32), comment='输出字段类型')
    allow_field_type = Column(Text, nullable=False, comment='允许的数据类型')


class DmmModelInstance(Base):
    """
    数据模型实例
    在数据开发阶段，基于已构建的数据模型创建的任务，将作为该数据模型应用阶段的实例，每个模型应用实例会包含一个主表和多个指标
    """

    __tablename__ = 'dmm_model_instance'

    # 实例配置和属性
    instance_id = Column(INTEGER(11), nullable=False, comment='模型实例ID', primary_key=True)
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    model_id = Column(INTEGER(11), nullable=False, comment='模型id')
    version_id = Column(String(64), nullable=False, comment='模型版本ID')

    # 任务相关配置
    flow_id = Column(INTEGER(11), nullable=False, comment='外部应用的 DataFlowID')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelInstanceTable(Base):
    """
    数据模型实例主表
    即明细数据表
    """

    __tablename__ = 'dmm_model_instance_table'

    # 主表配置及属性
    result_table_id = Column(String(255), nullable=False, comment='主表ID', primary_key=True)
    bk_biz_id = Column(INTEGER(11), nullable=False, comment='业务Id')
    instance_id = Column(INTEGER(11), nullable=False, comment='模型应用实例id')
    model_id = Column(INTEGER(11), nullable=False, comment='模型id')
    flow_node_id = Column(INTEGER(11), nullable=False, comment='原Flow节点ID')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelInstanceField(Base):
    """
    数据模型实例主表字段
    主要记录了主表输出哪些字段及字段应用阶段的清洗规则
    """

    __tablename__ = 'dmm_model_instance_field'

    # 字段基本属性
    id = Column(INTEGER(11), nullable=False, comment='模型实例自增ID', primary_key=True)
    instance_id = Column(INTEGER(11), nullable=False, comment='模型应用实例id')
    model_id = Column(INTEGER(11), nullable=False, comment='模型id')
    field_name = Column(String(255), nullable=False, comment='输出字段')
    # 字段来源信息
    input_result_table_id = Column(String(255), nullable=False, comment='输入结果表')
    input_field_name = Column(String(255), nullable=False, comment='输入字段')
    application_clean_content = Column(Text, comment='应用阶段清洗规则')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelInstanceRelation(Base):
    """
    数据模型实例字段映射关联关系
    对于需要进行维度关联的字段，需要在关联关系表中记录维度关联的相关相信
    """

    __tablename__ = 'dmm_model_instance_relation'

    id = Column(INTEGER(11), nullable=False, comment='自增ID', primary_key=True)
    instance_id = Column(INTEGER(11), nullable=False, comment='模型应用实例id')
    model_id = Column(INTEGER(11), nullable=False, comment='模型id')
    field_name = Column(String(255), nullable=False, comment='输出字段')
    # 关联字段来源及关联信息
    related_model_id = Column(INTEGER(11), nullable=False, comment='关联模型ID')
    input_result_table_id = Column(String(255), nullable=False, comment='输入结果表')
    input_field_name = Column(String(255), nullable=False, comment='输入字段')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelInstanceIndicator(Base):
    """
    数据模型实例指标
    """

    __tablename__ = 'dmm_model_instance_indicator'

    # 应用实例指标基本配置和属性
    result_table_id = Column(String(255), nullable=False, comment='指标结果表ID', primary_key=True)
    project_id = Column(INTEGER(11), nullable=False, comment='项目ID')
    bk_biz_id = Column(INTEGER(11), nullable=False, comment='业务Id')
    instance_id = Column(INTEGER(11), nullable=False, comment='模型应用实例id')
    model_id = Column(INTEGER(11), nullable=False, comment='模型id')
    # 关联结果表和节点的信息
    parent_result_table_id = Column(String(255), nullable=False, comment='上游结果表ID')
    flow_node_id = Column(INTEGER(11), nullable=False, comment='来源节点ID')
    # 来自模型定义的指标继承写入，可以重载
    calculation_atom_name = Column(String(255), nullable=False, comment='统计口径名称')
    aggregation_fields = Column(Text, comment='聚合字段列表(使用逗号分割)')
    filter_formula = Column(Text, comment='过滤SQL')
    scheduling_type = Column(String(32), nullable=False, comment='计算类型')
    scheduling_content = Column(Text, comment='调度内容')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )


class DmmModelInstanceSource(Base):
    """
    模型实例输入表
    """

    __tablename__ = 'dmm_model_instance_source'

    id = Column(INTEGER(11), nullable=False, comment='自增ID', primary_key=True)
    instance_id = Column(INTEGER(11), nullable=False, comment='模型应用实例id')
    input_type = Column(String(255), nullable=False, comment='输入表类型')
    input_result_table_id = Column(String(255), nullable=False, comment='输入结果表')
    created_by = Column(String(50), nullable=False, comment='创建者')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新者')
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), comment='更新时间'
    )
