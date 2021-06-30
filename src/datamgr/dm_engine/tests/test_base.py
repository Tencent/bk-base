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

import attr
from dm_engine.utils.redis import RedisClient


@attr.s
class A(object):
    a = attr.ib(type=str)
    b = attr.ib(type=str)


def test_attr():
    a = {"a": "a", "b": "b"}
    print(A(**a))


def test_redis_ops():
    client = RedisClient
    client.lpush("xxx", "aaa")
    client.lpush("xxx", "bbb")
    client.lpush("xxx", "ccc")

    assert client.rpop("xxx") == "aaa"
    assert client.rpop("xxx") == "bbb"
    assert client.rpop("xxx") == "ccc"
    assert client.rpop("xxx") is None

    client.hset("key1", "aa", "111")
    client.hset("key1", "bb", "111")

    ret = client.hgetall("key1")
    assert type(ret) == dict


# def test_multiprocess_pipe():
#
#     conn1, conn2 = multiprocessing.Pipe()
#
#     def sender(conn1):
#         count = 10
#         while count:
#             conn1.send(f'[{datetime.now()}] ping::{count}')
#             count -= 1
#             time.sleep(1)
#
#     def receider(conn2):
#         count = 10
#         while count:
#             content = conn2.recv()
#             print(f'[{datetime.now()}] {count}, content={content}')
#             count -= 1
#             time.sleep(2)
#
#     p1 = multiprocessing.Process(target=sender, args=(conn1,))
#     p2 = multiprocessing.Process(target=receider, args=(conn2,))
#     p1.start()
#     p2.start()
#
#     p1.join()
#     p2.join()
#
#     conn1.close()
