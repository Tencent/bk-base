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
from redis.sentinel import Sentinel

from dataflow.stream.settings import REDIS_INFO

meta_rd = None


def get_meta_redis_conn():
    global meta_rd
    try:
        if not meta_rd:
            meta_rd = redis.StrictRedis(**REDIS_INFO)
        return meta_rd
    except BaseException:
        if meta_rd:
            meta_rd.close()
        meta_rd = None


class RedisConnection(object):
    def __init__(self, host, port, password=None, socket_timeout=None):
        # 不需要主动关闭连接，使用完成后会自动被底层连接池回收
        self.__master = redis.StrictRedis(host=host, port=port, password=password, socket_timeout=socket_timeout)

    def ping(self):
        return self.__master.ping()

    def delete(self, *keys):
        # redis 集群模式暂不支持事务操作
        with self.__master.pipeline(transaction=False) as pipe:
            pipe.delete(*keys)
            return pipe.execute()

    def hset(self, *args, **kwargs):
        return self.__master.hset(*args, **kwargs)

    def hgetall(self, *args, **kwargs):
        return self.__master.hgetall(*args, **kwargs)

    def hmset(self, *args, **kwargs):
        return self.__master.hmset(*args, **kwargs)


class RedisSentinelConnection(object):
    def __init__(self, host, port, master_name, password):
        sentinel = Sentinel([(host, port)], socket_timeout=0.1)
        self.__master = sentinel.master_for(master_name, password=password, socket_timeout=0.1)
        self.__slave = sentinel.slave_for(master_name, password=password, socket_timeout=0.1)

    def ping(self):
        """
        仅对 master 进行 ping 测试
        @return:
        """
        return self.__master.ping()

    def delete(self, *keys):
        """
        事务删除
        @param keys:
        @return:
        """
        with self.__master.pipeline() as pipe:
            pipe.delete(*keys)
            return pipe.execute()

    def hset(self, *args, **kwargs):
        return self.__master.hset(*args, **kwargs)

    def hgetall(self, *args, **kwargs):
        return self.__slave.hgetall(*args, **kwargs)

    def hmset(self, *args, **kwargs):
        return self.__master.hmset(*args, **kwargs)
