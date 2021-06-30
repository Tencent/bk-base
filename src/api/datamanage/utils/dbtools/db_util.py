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
import pymysql
from pymysql.cursors import DictCursor

from common.log import logger

from datamanage.pizza_settings import DB_SETTINGS
from datamanage.exceptions import DatamanageErrorCode

try:
    DB_CONFIG = DB_SETTINGS
except Exception as e:
    logger.warning(f'{str(e)}')
    DB_CONFIG = {}

_db_cache = {}


def init_db_conn(db='mapleleaf_monitor', config=None):
    db_config = DB_CONFIG.get(db, {})
    if config and config is not None:
        db_config = config
    try:
        if _db_cache.get(db, False) and _db_cache[db].ping():
            _dbconn = _db_cache[db]
        else:
            _dbconn = pymysql.connect(
                host=db_config.get('db_host', ''),
                port=db_config.get('db_port', 3306),
                user=db_config.get('db_user', ''),
                passwd=db_config.get('db_password', ''),
                charset=db_config.get('charset', 'utf8'),
                database=db_config.get('db_name', db),
            )
            _db_cache[db] = _dbconn
        return _dbconn
    except Exception as e:
        logger.error('connect db(%s) failed, reason %s' % (db, e), result_code=DatamanageErrorCode.MYSQL_CONN_ERR)
        return False


def db_query(sql, db='mapleleaf_monitor', config={}, is_dict=True):
    try:
        _dbconn = init_db_conn(db, config)
        if is_dict:
            _dbcursor_class = DictCursor
            _dbcursor = _dbconn.cursor(_dbcursor_class)
        else:
            _dbcursor = _dbconn.cursor()
        _dbcursor.execute(sql)
        _dbconn.commit()
        result = _dbcursor.fetchall()
    except Exception as e:
        logger.error(
            'query db%s failed, reason %s, sql %s' % (db, e, sql), result_code=DatamanageErrorCode.MYSQL_EXEC_ERR
        )
        return False

    return result
