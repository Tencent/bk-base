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
import time

import django.db
import django.db.transaction


@contextlib.contextmanager
def open_cursor(db_name):
    atomic = None
    for i in range(1, 4):
        try:
            atomic = django.db.transaction.atomic(db_name)
        except Exception as e:
            if i == 3:
                raise e
        if atomic:
            break
        else:
            time.sleep(2)
    with atomic:
        c = django.db.connections[db_name].cursor()
        try:
            yield c
        finally:
            c.close()


def getAll(cursor, sql, **kwargs):
    cursor.execute(sql, kwargs)
    rows = __dictfetchall(cursor)
    if not rows:
        return None
    return rows


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


def execute(cursor, sql, **kwargs):
    try:
        cursor.execute(sql, kwargs)
        return cursor.rowcount
    except BaseException:
        raise


# ########### internal #################
def __dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [dict(list(zip([col[0] for col in desc], row))) for row in cursor.fetchall()]
