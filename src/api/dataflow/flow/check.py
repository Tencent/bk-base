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

import pika
from celery.task.control import inspect
from conf.dataapi_settings import RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST

# 本地测试
# RABBITMQ_HOST = 'localhost'
# RABBITMQ_PORT = '5672'
# RABBITMQ_VHOST = 'bk_data'
# APP_ID = 'guest'
# APP_TOKEN = 'guest'


def flow_check():
    """
    入口函数
    """
    celery_ret = _check_celery()
    return {
        "rabbitmq": _format(_check_rabbitmq()),
        "celery.flow": _format(celery_ret["worker2-1"]),
    }


def _format(ret):
    """
    格式换返回结果
    """
    return {"status": ret[0], "message": ret[1]}


def _check_rabbitmq():
    """
    检查 Celery 队列服务是否正常
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=int(RABBITMQ_PORT),
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
            )
        )
        channel = connection.channel()

        # 尝试申明测试队列
        channel.queue_declare("data_test")
        channel.basic_publish(exchange="", routing_key="data_test", body="Hello DataFlow!")
        connection.close()
    except Exception as e:
        return False, str(e)

    return True, "ok"


def _check_celery():
    """
    检查 Celery 任务是否正常
    """
    # 测试 Worker 状态
    expect_workers = ["worker2-1"]

    ret = {"worker2-1": [False, "Process not exist"]}
    try:
        insp = inspect()
        workers_stats = insp.stats()

        if workers_stats is not None:
            active_workers = [_w.split("@")[0] for _w in list(workers_stats.keys())]
            for _worker in expect_workers:
                if _worker in active_workers:
                    ret[_worker] = [True, "ok"]

    except Exception as e:
        for _w, _v in list(ret.items()):
            _v[0] = False
            _v[1] = str(e)

    return ret
