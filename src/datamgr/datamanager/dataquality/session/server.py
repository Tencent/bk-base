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

from api import dataflow_api
from dataquality.exceptions import (
    SessionServerCodeError,
    StartSessionError,
    StopSessionError,
)


class SessionServer(object):
    CODE_EXECUTE_TIMEOUT = 60

    def __init__(self, server_id, init_codes=[]):
        self._server_id = server_id
        self._init_codes = init_codes
        self.stop()
        self.start()

    @property
    def server_id(self):
        return self._server_id

    def heartbeat(self):
        try:
            dataflow_api.batch_interactive_servers_codes.create(
                {
                    "code": "1 + 1",
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                },
                raise_exception=True,
            )
        except Exception as e:
            logging.error(e, exc_info=True)

    def status(self):
        try:
            res = dataflow_api.batch_interactive_servers.retrieve(
                {
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                },
                raise_exception=True,
            )
            return res.data.get("server_status")
        except Exception as e:
            logging.error(e, exc_info=True)
        return None

    def session_status(self):
        try:
            res = dataflow_api.batch_interactive_servers.session_status(
                {
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                },
                raise_exception=True,
            )
            return res.data.get("state")
        except Exception as e:
            logging.error(e, exc_info=True)
        return None

    def start(self):
        try:
            dataflow_api.batch_interactive_servers.create(
                {
                    "server_id": self._server_id,
                    "bk_user": "admin",
                    "geog_area_code": "inland",
                    "engine_conf": {
                        "spark.bkdata.auth.project.id.enable": "false",
                        "spark.bkdata.auth.user.id.enable": "false",
                        "spark.bkdata.auth.cross.business.enable": "false",
                        "spark.bkdata.auth.storage.path.enable": "true",
                        "spark.executor.memory": "2g",
                        "spark.executor.cores": "1",
                        "spark.dynamicAllocation.maxExecutors": "50",
                    },
                },
                raise_exception=True,
            )

            if len(self._init_codes) > 0:
                self.execute_init_codes()
        except Exception as e:
            logging.error(e, exc_info=True)
            raise StartSessionError(message_kv={"server_id": self._server_id})

    def execute_init_codes(self, timeout=60):
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                raise SessionServerCodeError(
                    "Session({server_id})执行初始化代码超时".format(
                        server_id=self._server_id,
                    )
                )

            if self.session_status() != "idle":
                time.sleep(1)
                continue

            return self.execute_codes(self._init_codes)

    def stop(self):
        try:
            dataflow_api.batch_interactive_servers.delete(
                {
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                }
            )
        except Exception as e:
            logging.error(e, exc_info=True)
            raise StopSessionError(message_kv={"server_id": self._server_id})

    def execute_codes(self, codes):
        try:
            res = dataflow_api.batch_interactive_servers_codes.create(
                {
                    "code": "\n".join(codes),
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                },
                raise_exception=True,
            )

            code_id = res.data.get("id")

            return self.fetch_executing_result(code_id)
        except Exception as e:
            logging.error(e, exc_info=True)

    def fetch_executing_result(self, code_id):
        start_time = time.time()
        while True:
            execute_res = dataflow_api.batch_interactive_servers_codes.retrieve(
                {
                    "code_id": code_id,
                    "server_id": self._server_id,
                    "geog_area_code": "inland",
                },
                raise_exception=True,
            )

            if execute_res.data.get("state") == "available":
                if execute_res.data.get("output", {}).get("status") == "ok":
                    return execute_res.data.get("output", {}).get("data")
                else:
                    raise SessionServerCodeError(
                        message_kv={
                            "server_id": self._server_id,
                            "error": execute_res.data.get("output", {}).get("evalue"),
                        }
                    )

            if time.time() - start_time > self.CODE_EXECUTE_TIMEOUT:
                raise SessionServerCodeError(
                    "Session({server_id})执行代码({code_id})超时".format(
                        server_id=self._server_id,
                        code_id=code_id,
                    )
                )
