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

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine

from metadata.backend.dgraph.backend import DgraphSessionHub
from metadata.backend.mysql.backend import MySQLSessionHub
from metadata.runtime import rt_context

db_conf = rt_context.config_collection.db_config
normal_conf = rt_context.config_collection.normal_config
dgraph_conf = rt_context.config_collection.dgraph_backend_config
bkdata_log_engine = create_engine(
    db_conf.BKDATA_LOG_DB_URL, pool_recycle=db_conf.DB_POOL_RECYCLE, pool_pre_ping=True, pool_size=db_conf.DB_POOL_SIZE
)
bkdata_meta_engine = create_engine(
    db_conf.BKDATA_META_DB_URL, pool_recycle=db_conf.DB_POOL_RECYCLE, pool_pre_ping=True, pool_size=db_conf.DB_POOL_SIZE
)

mysql_session = MySQLSessionHub(
    db_conf.BKDATA_META_DB_URL, pool_size=db_conf.DB_POOL_SIZE, pool_maxsize=db_conf.DB_POOL_SIZE + 10, db_conf=db_conf
)
# dgraph的session连接会在之后state初始化时被重置,此处为缺省的无效连接
dgraph_session = DgraphSessionHub(dgraph_conf.SERVERS, dgraph_conf.POOL_SIZE, dgraph_conf.POOL_MAX_SIZE)

http_session = requests.session()
http_session.mount(
    'https://', HTTPAdapter(pool_connections=normal_conf.HTTP_POOL_SIZE, pool_maxsize=normal_conf.HTTP_POOL_MAX_SIZE)
)
http_session.mount(
    'http://', HTTPAdapter(pool_connections=normal_conf.HTTP_POOL_SIZE, pool_maxsize=normal_conf.HTTP_POOL_MAX_SIZE)
)

__all__ = ['bkdata_log_engine', 'bkdata_meta_engine', 'mysql_session', 'dgraph_session', 'http_session']
