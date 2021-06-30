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


import redis
import rediscluster
from auth.exceptions import RedisConnectError
from common.log import logger
from django.conf import settings


def create_connection(redis_config):
    redis_cluster = redis_config.get("cluster", False)
    try:
        if redis_cluster:
            _redis = rediscluster.StrictRedisCluster(
                host=redis_config["host"],
                port=redis_config["port"],
                password=redis_config["password"],
                socket_timeout=3,
                decode_responses=True,
            )
        else:
            _redis = redis.StrictRedis(
                host=redis_config["host"],
                port=redis_config["port"],
                password=redis_config["password"],
                socket_timeout=3,
                decode_responses=True,
            )
        logger.info(f"Redis connection OK, config={redis_config}")

        return _redis
    except Exception as e:
        err_message = f"Connect failed, reason={e}, config={redis_config}"
        logger.error(err_message)
        raise RedisConnectError(err_message)


class ConnctionHandler:
    """
    ConnctionHandler build the connection to the target DB and cache it. Redis
    operator method refer to the redis.readthedocs.io document.
    """

    def __init__(self, config):
        self.config = config
        self._connections = {}

    def __getitem__(self, alias):
        if alias in self._connections:
            return self._connections[alias]

        conn = create_connection(self.config[alias])
        self._connections[alias] = conn
        return conn


# 全局唯一，公共维护
connections = ConnctionHandler(settings.REDIS_CONFIG)
