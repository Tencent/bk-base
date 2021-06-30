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
from sqlalchemy import BOOLEAN, TIMESTAMP, Column, Enum, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER, MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata
Base.db_name = 'bkdata_log'


class DbOperateLog(Base):
    __tablename__ = 'db_operate_log'

    id = Column(INTEGER(11), primary_key=True)
    method = Column(
        Enum('QUERY', 'CREATE', 'UPDATE', 'DELETE', 'CONDITIONAL_DELETE', 'CONDITIONAL_UPDATE'), nullable=False
    )
    db_name = Column(String(128), nullable=False)
    table_name = Column(String(128), nullable=False)
    primary_key_value = Column(Text)
    conditional_filter = Column(Text)
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
