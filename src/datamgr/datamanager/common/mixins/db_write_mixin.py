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
import json
import logging
import sys
import weakref

import gevent
from gevent.pool import Pool
from gevent.queue import Queue

from common.db import connections
from common.exceptions import DbWriterNotRunningError


class DbWriteMixin(object):
    def __init__(self, *args, **kwargs):
        super(DbWriteMixin, self).__init__(*args, **kwargs)
        self._writing_queue = Queue()

        self._writing_running = False
        self._writer_tasks = []
        self._writing_exceptinos = {}

    def __del__(self):
        if self._writing_running:
            self.db_write_stop()

    def db_write_stop(self):
        while self._writing_queue.qsize() > 0:
            gevent.sleep(0.1)
        self._writing_running = False

    def db_write_start(self, writer_count=4):
        self._writing_running = True
        self._writer_tasks = self._setup_writer_tasks(writer_count)

    def _setup_writer_tasks(self, writer_count):
        self = weakref.proxy(self)

        self._db_write_task_pool = Pool(writer_count)

        def _handler(task_id):
            logging.info("Start dbwriter handler({})".format(task_id))
            while True:
                try:
                    if not self._writing_running:
                        break

                    task = self._writing_queue.get_nowait()
                    sqla_conn, parse_results = self.parse_wrting_data(task)
                    conn = sqla_conn.engine.raw_connection()

                    store_type = task.get("store_type")
                    if store_type == "insert":
                        result = self.do_insert(conn, task, parse_results)
                    elif store_type == "update":
                        result = self.do_update(conn, task, parse_results)
                    elif store_type == "replace":
                        result = self.do_replace(conn, task, parse_results)
                    elif store_type == "sql":
                        result = self.do_sql(conn, task, parse_results)
                    else:
                        logging.error(
                            "Store type({}) is not supported".format(store_type)
                        )
                        continue

                    if not result:
                        logging.error(
                            "Failed to do db write for the task({})".format(
                                json.dumps(task)
                            )
                        )

                    gevent.sleep(0.01)
                except ReferenceError:
                    break
                except Exception:
                    gevent.sleep(1)
                    self._writing_exceptinos[task_id] = sys.exc_info()
            logging.info("Stop dbwriter handler({})".format(task_id))

        return [
            self._db_write_task_pool.spawn(_handler, task_id)
            for task_id in range(writer_count)
        ]

    def push_task(self, task):
        if not self._writing_running:
            raise DbWriterNotRunningError()

        self._writing_queue.put_nowait(task)

    def parse_wrting_data(self, task):
        data = task.get("data", False)
        if not task.get("table_name", False):
            return False
        if not task.get("columns", False):
            if type(data) is dict:
                task["columns"] = data.keys()
            elif (type(data) is list) and len(data) > 0 and (type(data[0]) is dict):
                task["columns"] = data[0].keys()

        parse_results = []
        if type(data) is dict:
            row_data = ()
            for column in task.get("columns", []):
                row_data += (data.get(column, ""),)
            parse_results.append(row_data)
        elif type(data) is list:
            for item in data:
                row_data = ()
                for column in task.get("columns", []):
                    row_data += (item.get(column, ""),)
                parse_results.append(row_data)

        # 初始化DB
        db_name = task.get("db_name", "basic")
        try:
            db_conn = connections[db_name]
        except Exception as e:
            logging.error(e, exc_info=True)
        return db_conn, parse_results

    def do_sql(self, conn, task, parse_results):
        sql = task.get("sql", False)
        try:
            logging.info("Start to execute sql: {}".format(sql))
            if not sql:
                return False
            cursor = conn.cursor()
            result = cursor.executemany(sql, parse_results)
            conn.commit()
            logging.info("Finish executing sql: {}".format(sql))
        except Exception as e:
            logging.error(
                "Failed to executing sql: {}, error: {}".format(sql, e), exc_info=True
            )
            return False
        return result

    def do_insert(self, conn, task, parse_results):
        sql = task.get("sql", False)
        try:
            if not sql:
                table_name = task.get("table_name", "")
                columns = task.get("columns", [])
                columns_str = ",".join(columns)
                value_format = ",".join(["%s" for i in columns])
                sql = "INSERT IGNORE INTO {} ({}) values ({})".format(
                    table_name, columns_str, value_format
                )
                task["sql"] = sql
            return self.do_sql(conn, task, parse_results)
        except Exception as e:
            logging.error(
                "Failed to executing sql: {}, error: {}".format(sql, e), exc_info=True
            )
            return False

    def do_update(self, conn, task, parse_results):
        sql = task.get("sql", False)
        try:
            if not sql:
                table_name = task.get("table_name", "")
                columns = [column + "=%s" for column in task.get("columns", [])]
                columns = ", ".join(columns)
                where_columns = [
                    where_column + "=%s"
                    for where_column in task.get("where_columns", [])
                ]
                where_data = ()
                where_values = task.get("where_data", {})
                for where_column in task.get("where_columns", []):
                    where_data += (where_values.get(where_column, ""),)
                where_columns = ",".join(where_columns)
                sql = "UPDATE {} SET {} WHERE {}".format(
                    table_name, columns, where_columns
                )
                task["sql"] = sql
                if type(parse_results) is list:
                    parse_results = [
                        data_obj + where_data for data_obj in parse_results
                    ]
                elif type(parse_results) is set:
                    parse_results += where_data
            return self.do_sql(conn, task, parse_results)
        except Exception as e:
            logging.error(
                "Failed to executing sql: {}, error: {}".format(sql, e), exc_info=True
            )
            return False

    def do_replace(self, conn, task, parse_results):
        sql = task.get("sql", False)
        try:
            if not sql:
                table_name = task.get("table_name", "")
                columns = task.get("columns", [])
                columns_str = ",".join(columns)
                value_format = ",".join(["%s" for i in columns])
                sql = "REPLACE INTO {} ({}) values ({})".format(
                    table_name, columns_str, value_format
                )
                task["sql"] = sql
            return self.do_sql(conn, task, parse_results)
        except Exception as e:
            logging.error(
                "Failed to executing sql: {}, error: {}".format(sql, e), exc_info=True
            )
            return False


if __name__ == "__main__":
    from gevent import monkey

    monkey.patch_all()

    mixin = DbWriteMixin()
    mixin.start()

    for i in range(100):
        mixin.push_data(str(i))

    gevent.sleep(10)
