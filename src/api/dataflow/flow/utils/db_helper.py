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

import contextlib
import logging

import django.db
import django.db.transaction

LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def open_cursor(db_name):
    with django.db.transaction.atomic(db_name):
        c = django.db.connections[db_name].cursor()
        try:
            yield c
        finally:
            c.close()


def get(cursor, sql, **kwargs):
    cursor.execute(sql, kwargs)
    rows = __dictfetchall(cursor)
    if not rows:
        return None
    return rows[0]


def list(cursor, sql, **kwargs):
    # print sql
    cursor.execute(sql, kwargs)
    return __dictfetchall(cursor)


def insert(cursor, table, **kwargs):
    columns = ", ".join(list(kwargs.keys()))
    variables = ", ".join(["%({})s".format(k) for k in list(kwargs.keys())])
    sql = "".join(["INSERT INTO ", table, "(", columns, ") VALUES (", variables, ")"])
    cursor.execute(sql, kwargs)
    return cursor.lastrowid


def update(cursor, table, u_kwargs, f_kwargs):
    kwargs = {}
    u_fields = ",".join(["{}=%(u_{})s".format(_k, _k) for _k in u_kwargs])
    kwargs.update({("u_%s" % _k): u_kwargs[_k] for _k in u_kwargs})

    u_conditions = " AND ".join(["{}=%(f_{})s".format(_k, _k) for _k in f_kwargs])
    kwargs.update({("f_%s" % _k): f_kwargs[_k] for _k in f_kwargs})

    sql = " ".join(["UPDATE", table, "SET", u_fields, "WHERE", u_conditions])

    return cursor.execute(sql, kwargs)


def execute(cursor, sql, **kwargs):
    try:
        cursor.execute(sql, kwargs)
        return cursor.rowcount
    except BaseException:
        LOGGER.error("{}\n{}".format(sql, kwargs))
        raise


def __dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [dict(list(zip([col[0] for col in desc], row))) for row in cursor.fetchall()]
