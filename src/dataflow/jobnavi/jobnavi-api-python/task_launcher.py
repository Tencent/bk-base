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
import os
import random
import socket
import sys
import threading
import time
import signal
import subprocess

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.server import TNonblockingServer
from thrift.transport import TSocket
from thrift.transport import TTransport

import jobnavi.settings as settings
from jobnavi.HAProxy import init_ha_proxy
from jobnavi.config import Config
from jobnavi.jobnavi_logging import get_logger, init_logger
from jobnavi.task_event import TaskEventService
from jobnavi.task_event_service_impl import TaskEventServiceImpl
from jobnavi.task_heartbeat import TaskHeartBeatService
from jobnavi.task_heartbeat.ttypes import TaskLauncherInfo

logger = get_logger()


def validate_port_range(min_port, max_port):
    if min_port == 0 or min_port < 1024 or min_port >= 65536:
        raise Exception(settings.JOBNAVI_TASK_PORT_MIN + " config invalid. Available port range is [1024-65535)")

    if max_port == 0 or max_port < 1024 or max_port >= 65536:
        raise Exception(settings.JOBNAVI_TASK_PORT_MAX + " config invalid. Available port range is [1024-65535)")

    if min_port >= max_port:
        raise Exception(settings.JOBNAVI_TASK_PORT_MIN + " must larger than " + settings.JOBNAVI_TASK_PORT_MAX)


def get_start_port(min_port, max_port, max_port_retry):
    for i in range(0, max_port_retry):
        port = random.randint(min_port, max_port)
        if not is_local_port_bind(port):
            return port
    raise Exception("Port bind error after retry " + str(max_port_retry) + " times.")


def start_rpc_service(start_port, execute_id, runner_port):
    handler = TaskEventServiceImpl()
    transport = TSocket.TServerSocket("127.0.0.1", start_port)
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    processor = TaskEventService.Processor(handler)
    server = TNonblockingServer.TNonblockingServer(processor, transport, pfactory, threads=3)
    start_heart_beart(start_port, execute_id, runner_port)
    server.serve()


def start_heart_beart(start_port, execute_id, runner_port):
    tsocket = TSocket.TSocket("127.0.0.1", runner_port)
    tsocket.setTimeout(3000)
    transport = TTransport.TBufferedTransport(tsocket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = TaskHeartBeatService.Client(protocol)
    transport.open()
    task_launcher_info = TaskLauncherInfo(int(execute_id), int(start_port))
    result = client.add(task_launcher_info)
    logger.info(result)
    if result.success:
        transport.close()
        thread = HeartbeatThread(start_port)
        thread.start()
    else:
        logger.error("register task error.")
        transport.close()


def is_local_port_bind(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(("127.0.0.1", port))
    is_bind = result == 0
    sock.close()
    return is_bind


def task_stop(return_value=0):
    try:
        pid = os.getpid()
        logger.info("current pid is: " + str(pid))
        ps_command = subprocess.Popen("ps -o pid --ppid %d --noheaders" % pid, shell=True, stdout=subprocess.PIPE)
        ps_output = ps_command.stdout.read()
        return_value = ps_command.wait()
        if type(ps_output) != str:
            # python 3.5+ ps_output is bytes
            ps_output = ps_output.decode()
        for pid_str in ps_output.strip().split("\n")[:-1]:
            logger.info("kill pid: " + str(pid_str))
            os.kill(int(pid_str), signal.SIGTERM)
    except Exception:
        logger.exception("kill ppid error.")
    sys.exit(return_value)


class HeartbeatThread(threading.Thread):
    def __init__(self, start_port):
        super(HeartbeatThread, self).__init__()
        self.start_port = start_port

    def run(self):
        try:
            while True:
                logger.info("send heartbeat...")
                tsocket = TSocket.TSocket("127.0.0.1", runner_port)
                tsocket.setTimeout(10000)
                transport = TTransport.TBufferedTransport(tsocket)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = TaskHeartBeatService.Client(protocol)
                transport.open()
                task_launcher_info = TaskLauncherInfo(int(execute_id), int(start_port))
                result = client.heartbeat(task_launcher_info)
                logger.info("heartbeat result:" + str(result))
                if not result.success:
                    logger.info("HeartBeat connection loss. terminate.")
                    transport.close()
                    task_stop()
                else:
                    time.sleep(1)
                    transport.close()
        except Exception:
            logger.exception("heart beat error.")
            task_stop()


if __name__ == "__main__":
    execute_id = sys.argv[1]
    runner_port = sys.argv[2]
    root_path = sys.argv[3]
    log_path = sys.argv[4]

    init_logger(log_path)

    config = Config(root_path + "/conf/jobnavi.properties")
    init_ha_proxy(config)

    min_port = int(config.get(settings.JOBNAVI_TASK_PORT_MIN, settings.JOBNAVI_TASK_PORT_MIN_DEFAULT))
    max_port = int(config.get(settings.JOBNAVI_TASK_PORT_MAX, settings.JOBNAVI_TASK_PORT_MAX_DEFAULT))
    max_port_retry = int(config.get(settings.JOBNAVI_TASK_PORT_MAX_RETRY, settings.JOBNAVI_TASK_PORT_MAX_RETRY_DEFAULT))
    max_RPC_retry = int(config.get(settings.JOBNAVI_TASK_RPC_MAX_RETRY, settings.JOBNAVI_TASK_RPC_MAX_RETRY_DEFAULT))
    for i in range(0, max_port_retry):
        try:
            start_port = get_start_port(min_port, max_port, max_port_retry)
            start_rpc_service(start_port, execute_id, runner_port)
        except Exception as e:
            if i == max_port_retry - 1:
                logger.exception(e.message)
                task_stop(1)
            else:
                logger.error("Start Task Launcher error. retry time is " + str(i) + " ...", e)
