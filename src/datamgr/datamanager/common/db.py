# -* -coding: utf-8 -*-
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
from __future__ import absolute_import, print_function, unicode_literals

from urllib.parse import quote_plus as urlquote

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from conf import settings


class MySQLSession(Session):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.expunge_all()
        if exc_type:
            self.rollback()
        self.close()


class MySQLConncetion(object):
    """
    Connection save engine to provide session
    """

    def __init__(self, database):
        self.engine = self.create_engine(database)
        self.session_factory = sessionmaker(bind=self.engine, class_=MySQLSession)

    def session(self, **kwargs):
        """
        :return {sqlalchemy.session}
        """
        return self.session_factory()

    def create_engine(self, database):

        db_url = "mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8".format(
            user=urlquote(database["USER"]),
            pwd=urlquote(database["PASSWORD"]),
            host=database["HOST"],
            port=database["PORT"],
            db=database["NAME"],
        )

        pool_recycle = database.get("POOL_RECYCLE", 3600)
        pool_size = database.get("POOL_SIZE", 3600)

        return create_engine(
            db_url, pool_recycle=pool_recycle, pool_pre_ping=True, pool_size=pool_size
        )


class ConnctionHandler(object):
    """
    databases is an optional dictionary of database definitions (structured
    like settings.DATABASES).
    """

    def __init__(self, databases):
        self.databases = databases
        self._connections = {}

    def __getitem__(self, alias):
        if alias in self._connections:
            return self._connections[alias]

        conn = MySQLConncetion(self.databases[alias])
        self._connections[alias] = conn
        return conn


connections = ConnctionHandler(settings.DATABASES)
