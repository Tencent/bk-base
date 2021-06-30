# coding: utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from sqlalchemy import BOOLEAN, TIMESTAMP, Column, Enum, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER, MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base.db_name = "bkdata_log"


class DbOperateLog(Base):
    __tablename__ = "db_operate_log"

    id = Column(INTEGER(11), primary_key=True)
    method = Column(Enum(u"QUERY", u"CREATE", u"UPDATE", u"DELETE"), nullable=False)
    db_name = Column(String(128), nullable=False)
    table_name = Column(String(128), nullable=False)
    primary_key_value = Column(Text, nullable=False)
    changed_data = Column(MEDIUMTEXT)
    change_time = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    )
    synced = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    description = Column(Text)
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    created_by = Column(String(50), nullable=False, server_default=text("'meta_admin'"))
    updated_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    updated_by = Column(String(50), nullable=False, server_default=text("'meta_admin'"))
