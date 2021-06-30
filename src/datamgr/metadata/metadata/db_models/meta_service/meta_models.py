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
    LargeBinary,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base.db_name = 'bkdata_meta'


class MetadataTypeDef(Base):
    """
    TODO:Meta库或Replica库迁移到其他地方。
    """

    __tablename__ = 'metadata_type_def'

    current_type = Column(String(64), primary_key=True)
    base_type = Column(String(64), nullable=False)
    category = Column(Enum('Entity', 'Tag'), nullable=False)
    type_source = Column(LargeBinary, nullable=False)
    description = Column(Text)
    disabled = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    created_by = Column(String(50), nullable=False, server_default=text("'meta_admin'"))
    updated_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    updated_by = Column(String(50), nullable=False, server_default=text("'meta_admin'"))


class DmCategoryConfig(Base):
    __tablename__ = 'dm_category_config'

    id = Column(INTEGER(11), primary_key=True, server_default=text("'0'"))
    category_name = Column(String(128), nullable=False)
    category_alias = Column(String(128), nullable=False)
    parent_id = Column(INTEGER(11), nullable=False)
    seq_index = Column(INTEGER(11), nullable=False)
    icon = Column(String(64))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    visible = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(50))
    updated_at = Column(TIMESTAMP)
    description = Column(Text)


class DmLayerConfig(Base):
    __tablename__ = 'dm_layer_config'

    id = Column(INTEGER(11), primary_key=True, server_default=text("'0'"))
    layer_name = Column(String(64), nullable=False)
    layer_alias = Column(String(128), nullable=False)
    parent_id = Column(INTEGER(11), nullable=False)
    seq_index = Column(INTEGER(11), nullable=False)
    icon = Column(String(128))
    active = Column(TINYINT(1), nullable=False, server_default=text("'1'"))
    created_by = Column(String(64), nullable=False, server_default=text("'admin'"))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_by = Column(String(64))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    description = Column(Text)
