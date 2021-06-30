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

import logging

import pymysql
from dm_engine.base.exceptions import DBConnectError
from dm_engine.config import settings
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


DEFAULT_DB_NAME = "default"


class _DBClient(object):
    def __init__(self):
        self.db_cache = {}

    @staticmethod
    def exist(db=DEFAULT_DB_NAME):
        """Whether db name configuration exist, return YES or NO"""
        if hasattr(settings, "DB_CONFIG"):
            return db in settings.DB_CONFIG
        return False

    def connect(self, db=DEFAULT_DB_NAME):
        if not self.exist(db):
            raise Exception(f"No {db} connection information")

        db_config = settings.DB_CONFIG[db]
        try:
            if db in self.db_cache and self.db_cache[db].ping():
                _dbconn = self.db_cache[db]
            else:
                _dbconn = pymysql.connect(
                    host=db_config["db_host"],
                    port=db_config["db_port"],
                    user=db_config["db_user"],
                    passwd=db_config["db_password"],
                    charset=db_config["charset"],
                    database=db_config["db_name"],
                )
                self.db_cache[db] = _dbconn
                logger.info("Database connection OK, config={}".format(db_config))
            return _dbconn
        except Exception as e:
            err_message = "Connect {db} failed, reason={err}, config={config}".format(db=db, err=e, config=db_config)
            logger.exception(err_message)
            raise DBConnectError(err_message)

    def query(self, sql, db=DEFAULT_DB_NAME, is_dict=True):
        """
        Common SQL query.
        """
        conn = self.connect(db)
        cursor_class = DictCursor if is_dict else None
        with conn.cursor(cursor_class) as cursor:
            cursor.execute(sql)
        conn.commit()

        return cursor.fetchall()

    def execute(self, sql, db=DEFAULT_DB_NAME):
        """Execute alter statetments

        :return: the execute result of SQL
        """
        conn = self.connect(db)

        with conn.cursor() as cursor:
            result = cursor.execute(sql)

        conn.commit()
        return result


DBClient = _DBClient()
