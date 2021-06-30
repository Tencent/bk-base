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
import time

from utils.influx_util import influx_save

import gevent
from gevent import pool
from gevent.queue import Queue


class InfluxSenderMixin(object):
    MAX_MSG_LEN = 10000
    MAX_MSG_WAIT = 5
    BUCKET_SIZE = 2000

    def __init__(self, *args, **kwargs):
        super(InfluxSenderMixin, self).__init__(*args, **kwargs)

        self._influx_sending = Queue()
        try:
            self._influx_pool = pool.Pool(5)
            for index in range(5):
                self._influx_pool.spawn(self.influx_sender, index)
        except Exception as e:
            logging.error("Influx Sender thread start failed %s " % e)

    def influx_sender(self, task_index):
        message_buffer = {}
        last_output_time = {}
        while True:
            try:
                msg = self._influx_sending.get()
                database = msg.get("database", "")
                retention_policy = msg.get("retention_policy", None)
                if not message_buffer.get(database, False):
                    message_buffer[database] = {}
                if not message_buffer[database].get(retention_policy, False):
                    message_buffer[database][retention_policy] = []
                message = msg.get("message", "")
                message_buffer[database][retention_policy].append(message)
                for db in message_buffer.keys():
                    for rp in message_buffer[db].keys():
                        self.write_data_to_influx(
                            db, rp, last_output_time, message_buffer, task_index
                        )
            except Exception as e:
                logging.error(e, exc_info=True)

    def write_data_to_influx(
        self, db, rp, last_output_time, message_buffer, task_index
    ):
        cur_time = time.time()

        db_rp_key = "{}_{}".format(db, rp)
        if not last_output_time.get(db_rp_key, False):
            last_output_time[db_rp_key] = time.time()

        if (len(message_buffer[db][rp]) > self.MAX_MSG_LEN) or (
            cur_time - last_output_time[db_rp_key] > self.MAX_MSG_WAIT
        ):
            start_time = time.time()
            logging.info(
                "Task%s cur queue len %s, influx_sender send database %s(rp: %s), buffer size %s"
                % (
                    task_index,
                    self._influx_sending.qsize(),
                    db,
                    rp,
                    len(message_buffer[db][rp]),
                )
            )
            try:
                # 批量写入
                tasks = []
                length = len(message_buffer[db][rp])
                for i in range(length // self.BUCKET_SIZE + 1):
                    if (
                        len(
                            message_buffer[db][rp][
                                i * self.BUCKET_SIZE : (i + 1) * self.BUCKET_SIZE
                            ]
                        )
                        > 0
                    ):
                        tasks.append(
                            gevent.spawn(
                                influx_save,
                                message_buffer[db][rp][
                                    i * self.BUCKET_SIZE : (i + 1) * self.BUCKET_SIZE
                                ],
                                db,
                                {},
                                "line",
                                rp,
                            )
                        )
                gevent.joinall(tasks)
                logging.info(
                    "Task%s finish to send, cost %.3fs"
                    % (task_index, time.time() - start_time)
                )
            except Exception as e:
                logging.error("write message into influx exception %s" % e)

            message_buffer[db][rp] = []
            last_output_time[db_rp_key] = time.time()
