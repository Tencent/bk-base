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

import gevent

from dataquality import redis
from dataquality.session.server import SessionServer
from dataquality.settings import (
    SESSION_RESTART_RETRY_COUNT,
    SESSION_RESTART_WAITING_INTERVAL,
)


class SessionManager(object):
    def __init__(self, session_key, session_count=3, init_codes=[]):
        self._session_key = session_key
        self._session_count = session_count
        self._sessions = {}
        self._init_codes = init_codes

        self._redis_key = "data_quality_{}_sessions".format(self._session_key)

        self.init_all_session_servers()

    def init_all_session_servers(self):
        redis.delete(self._redis_key)
        for index in range(self._session_count):
            session_id = "{}_{}".format(self._session_key, index)
            self._sessions[session_id] = SessionServer(session_id, self._init_codes)
            redis.sadd(self._redis_key, session_id)

    def check_all_sessions_status(self):
        for session_id, session_server in self._sessions.items():
            if session_server.status() != "started":
                redis.srem(self._redis_key, session_server.server_id)
                if self.restart_session(session_server):
                    redis.sadd(self._redis_key, session_server.server_id)

    def heartbeat_all_sessions(self):
        for session_id, session_server in self._sessions.items():
            session_server.heartbeat()

    def all_sessions_execute_codes(self, codes):
        for session_id, session_server in self._sessions.items():
            session_server.execute_codes(codes)

    def restart_session(self, session_server):
        retry_count = SESSION_RESTART_RETRY_COUNT
        while retry_count > 0:
            session_server.start()
            if session_server.status() != "started":
                logging.info(
                    "Failed to restart session({server_id})".format(
                        server_id=session_server.server_id
                    )
                )
                retry_count -= 1
                gevent.sleep(SESSION_RESTART_WAITING_INTERVAL)
            else:
                logging.info(
                    "Success to restart session({server_id})".format(
                        server_id=session_server.server_id
                    )
                )
                return True
        return False
