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

from __future__ import absolute_import, print_function, unicode_literals

import logging

import redis
import rediscluster
from dm_engine.base.exceptions import RedisConnectError
from dm_engine.config import settings

logger = logging.getLogger(__name__)

DEFAULT_DB_NAME = "default"


class _RedisClient(object):
    """
    RedisClient build the connection to the target DB and cache it. Common connect
    method, other redis operator method refer to the redis.readthedocs.io document.
    """

    def __init__(self):
        self._redis_cache = {}

    def connect(self, db=DEFAULT_DB_NAME):
        redis_config = settings.REDIS_CONFIG[db]
        redis_cluster = redis_config.get("cluster", False)

        _redis_cache = self._redis_cache

        try:
            if _redis_cache.get(db, False) and _redis_cache[db].ping():
                _redis = _redis_cache[db]
            else:
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
                _redis_cache[db] = _redis
                logger.info("Redis connection OK, config={}".format(redis_config))

            return _redis
        except Exception as e:
            err_message = "Connect {db} failed, reason={err}, config={config}".format(db=db, err=e, config=redis_config)
            logger.exception(err_message)
            raise RedisConnectError(err_message)

    def get(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.get(name)

    def delete(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.delete(name)

    def lpush(self, name, value, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.lpush(name, value)

    def rpop(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.rpop(name)

    def llen(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.llen(name)

    def lrange(self, name, start, end, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.lrange(name, start, end)

    def hget(self, name, key, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.hget(name, key)

    def hset(self, name, key, value, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.hset(name, key, value)

    def hdel(self, name, key, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.hdel(name, key)

    def hgetall(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.hgetall(name)

    def hvals(self, name, db=DEFAULT_DB_NAME):
        _conn = self.connect(db=db)
        return _conn.hvals(name)


RedisClient = _RedisClient()
