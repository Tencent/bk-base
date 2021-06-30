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

from sqlalchemy import TIMESTAMP, Column, Index, String, Text, text
from sqlalchemy.dialects.mysql import (
    BIGINT,
    DATETIME,
    INTEGER,
    LONGTEXT,
    MEDIUMTEXT,
    TINYINT,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base._db_name_ = 'bkdata_flow'


class DataflowJobnaviClusterConfig(Base):
    __tablename__ = 'dataflow_jobnavi_cluster_config'

    id = Column(INTEGER(11), primary_key=True)
    cluster_name = Column(String(32), nullable=False, unique=True)
    cluster_domain = Column(Text, nullable=False)
    version = Column(String(32), nullable=False)
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    description = Column(Text, nullable=False)
    geog_area_code = Column(String(50), nullable=False)


class ProcessingClusterConfig(Base):
    __tablename__ = 'processing_cluster_config'

    id = Column(INTEGER(11), primary_key=True)
    cluster_domain = Column(String(255), server_default=text("'default'"))
    cluster_group = Column(String(128), nullable=False)
    cluster_name = Column(String(128), nullable=False)
    cluster_label = Column(String(128), nullable=False, server_default=text("'standard'"))
    priority = Column(INTEGER(11), nullable=False, server_default=text("'1'"))
    version = Column(String(128), nullable=False)
    belong = Column(String(128), server_default=text("'bkdata'"))
    component_type = Column(String(128), nullable=False)
    created_by = Column(String(50), nullable=False, server_default=text("''"))
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False)
    geog_area_code = Column(String(50), nullable=False)


class DataflowInfo(Base):
    __tablename__ = 'dataflow_info'

    flow_id = Column(INTEGER(11), primary_key=True, comment='flow的id')
    flow_name = Column(String(255), nullable=False, comment='flow的名名称')
    project_id = Column(INTEGER(11), nullable=False, comment='flow所属项目id')
    status = Column(String(32), comment='flow运行状态')
    is_locked = Column(INTEGER(11), nullable=False, comment='是否被锁住')
    latest_version = Column(String(255), comment='最新版本号')
    bk_app_code = Column(String(255), nullable=False, comment='哪些有APP在使用？')
    active = Column(TINYINT(1), nullable=False, server_default=text("'0'"), comment='0:无效，1:有效')
    created_by = Column(String(128), comment='创建人')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    locked_by = Column(String(255))
    locked_at = Column(DATETIME(fsp=6))
    locked_description = Column(Text, comment='任务被锁原因')
    updated_by = Column(String(50), comment='开始人 ')
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False, comment='备注信息')
    tdw_conf = Column(Text)
    custom_calculate_id = Column(String(255), comment='')
    flow_tag = Column(String(255), nullable=True)


class DataflowNodeInfo(Base):
    __tablename__ = 'dataflow_node_info'

    node_id = Column(INTEGER(11), primary_key=True, comment='node的id')
    flow_id = Column(INTEGER(11), nullable=False, comment='flow的id')
    node_name = Column(String(255), comment='节点名称')
    node_config = Column(LONGTEXT, nullable=False, comment='节点配置')
    node_type = Column(String(255), comment='节点类型')
    status = Column(String(9), nullable=False, comment='节点状态')
    latest_version = Column(String(255), nullable=False, comment='最新版本')
    running_version = Column(String(255), comment='正在运行的版本')
    created_by = Column(String(128), comment='创建人')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='开始人 ')
    updated_at = Column(TIMESTAMP)
    description = Column(Text, nullable=False, comment='备注信息')
    frontend_info = Column(LONGTEXT, comment='前端信息')


class DataflowNodeRelation(Base):
    __tablename__ = 'dataflow_node_relation'

    __table_args__ = (
        Index('dataflow_node_relation_node_id_result_table_id_uniq', 'node_id', 'result_table_id', unique=True),
    )
    id = Column(INTEGER(11), primary_key=True)
    bk_biz_id = Column(INTEGER(11), comment='业务id')
    project_id = Column(INTEGER(11), nullable=False, comment='项目id')
    flow_id = Column(INTEGER(11), nullable=False, comment='flow的id')
    node_id = Column(INTEGER(11), nullable=False, comment='flow中节点id')
    result_table_id = Column(String(255), nullable=False, comment='result_table_id, data_id节点的输入数据')
    node_type = Column(String(64), nullable=False, comment='节点类型')
    generate_type = Column(String(32), nullable=False, server_default=text("'user'"), comment='结果表生成类型 user/system')
    is_head = Column(TINYINT(4), nullable=False, server_default=text("'1'"), comment='是否是节点的头部RT')


class DataflowProcessing(Base):
    __tablename__ = 'dataflow_processing'

    id = Column(INTEGER(11), primary_key=True)
    flow_id = Column(INTEGER(11), nullable=False, comment='flow的id')
    node_id = Column(INTEGER(11), comment='node的id')
    processing_id = Column(String(255), nullable=False, comment='processing的id')
    processing_type = Column(String(255), nullable=False, comment='包括实时作业stream、离线作业batch')
    description = Column(Text, nullable=False, comment='备注信息')


class BksqlFunctionDevConfig(Base):
    __tablename__ = 'bksql_function_dev_config'

    id = Column(INTEGER(11), primary_key=True)
    func_name = Column(String(255), nullable=False)
    version = Column(String(255), nullable=False)
    func_alias = Column(String(255))
    func_language = Column(String(255))
    func_udf_type = Column(String(255))
    input_type = Column(Text)
    return_type = Column(Text)
    explain = Column(Text)
    example = Column(Text)
    example_return_value = Column(Text)
    code_config = Column(Text)
    sandbox_name = Column(String(255), server_default=text("'default'"))
    released = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    locked = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    locked_by = Column(String(50), server_default=text("''"), comment='锁定人')
    locked_at = Column(TIMESTAMP)
    created_by = Column(String(50), comment='创建人')
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment='创建时间')
    updated_by = Column(String(50), comment='更新人')
    updated_at = Column(TIMESTAMP)
    debug_id = Column(String(255))
    support_framework = Column(String(255))
    description = Column(Text)
