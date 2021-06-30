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

import gevent
from common.bklanguage import BkLanguage
from common.local import _local, set_local_param
from gevent import monkey

# 不可用 patch_all(patch thread 在 django 关闭数据库连接时会触发 DatabaseError)
monkey.patch_socket()


class FuncInstance(object):
    def __init__(self, func, **params):
        self._func = func
        self._params = params

    def func(self, local, *args, **kwargs):
        for key, value in list(local.items()):
            set_local_param(key, value)
        BkLanguage.set_language(BkLanguage.current_language())
        return self._func(*args, **kwargs)

    @property
    def params(self):
        return self._params


def _concurrent_call_func(func_instances):
    """
    并发调用指定函数
    @param func_instances:
    @return:
    """
    threads = []
    for func_instance in func_instances:
        local = dict(_local.__dict__)
        thread = gevent.spawn(func_instance.func, local, **func_instance.params)
        threads.append(thread)
    gevent.joinall(threads)
    return threads


def concurrent_call_func(func_infos):
    """
    @param func_infos:
        [
            [func1, params]
        ]
    @return:
    """
    func_instances = []
    for _info in func_infos:
        func = _info[0]
        params = _info[1]
        func_instances.append(FuncInstance(func, **params))
    threads_res = _concurrent_call_func(func_instances)
    return [x.value for x in threads_res]


if __name__ == "__main__":

    def func1(x):
        return "func1: %s" % x

    def func2(x, y):
        return "func2: {}, {}".format(x, y)

    params1 = {"x": 1}
    test1 = FuncInstance(func1, **params1)

    params2 = {"x": 1, "y": 2}
    test2 = FuncInstance(func2, **params2)

    # example 1
    print("---1---")
    threads_res = _concurrent_call_func([test1, test2])
    for g in threads_res:
        print(g.value)

    # example 2
    print("---2---")
    func_info = [[func1, params1], [func2, params2]]
    threads_res = concurrent_call_func(func_info)
    print(threads_res)

    # example 3
    print("---3---")

    def func1(x=1):
        return "x: %s" % x

    def func2(x, y=2):
        return "x: {}, y: {}".format(x, y)

    params1 = {"x": 4}
    params2 = {"x": 5}
    func_info = [[func1, params1], [func2, params2]]
    threads_res = concurrent_call_func(func_info)
    print(threads_res)
