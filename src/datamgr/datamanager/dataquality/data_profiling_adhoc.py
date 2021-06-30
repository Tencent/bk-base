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

from gevent import monkey

from dataquality.session.manager import SessionManager
from dataquality.settings import (
    DATA_PROFILING_SESSION_KEY,
    MANAGER_SESSION_COUNT,
    PROFILING_REGISTERED_UDFS,
    SESSION_HEATBEAT_INTERVAL,
    SESSION_STATUS_CHECK_INTERVAL,
)

monkey.patch_all()


def start_data_profiling_adhoc_session():
    logging.info("Start to execute data correcting debug session task")
    last_check_time = time.time()
    last_heatbeat_time = time.time()

    init_codes = []
    for udf_name in PROFILING_REGISTERED_UDFS:
        init_codes.append(
            "spark_session.register_bkdata_udf('{udf_name}')".format(udf_name=udf_name)
        )

    session_manager = SessionManager(
        DATA_PROFILING_SESSION_KEY, MANAGER_SESSION_COUNT, init_codes
    )

    while True:
        now = time.time()
        # TODO 接收框架信号终止进程

        if now - last_check_time > SESSION_STATUS_CHECK_INTERVAL:
            session_manager.check_all_sessions_status()
            last_check_time = now

        if now - last_heatbeat_time > SESSION_HEATBEAT_INTERVAL:
            session_manager.heartbeat_all_sessions()
            last_heatbeat_time = now
