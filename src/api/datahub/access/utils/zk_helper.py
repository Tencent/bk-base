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

import contextlib

import kazoo.client
import kazoo.retry


@contextlib.contextmanager
def open_zk(hosts):
    command_retry = kazoo.retry.KazooRetry()
    connection_retry = kazoo.retry.KazooRetry()
    zk = kazoo.client.KazooClient(hosts=hosts, connection_retry=connection_retry, command_retry=command_retry)
    zk.start()
    try:
        yield zk
    finally:
        zk.stop()


def ensure_dir(zk, dir_path):
    parts = dir_path.split("/")
    sofar = ""

    for part in parts:
        sofar += "/%s" % part
        if not zk.exists(sofar):
            zk.create(sofar, "")


def get_node_children(zk_host, node_path):
    # 获取zk上节点的子节点列表
    result = []
    with open_zk(zk_host) as zk:
        if zk.exists(node_path, watch=None):
            result = zk.get_children(node_path)

    return result


def get_node_children_confs(zk_host, node_path):
    result = {}
    with open_zk(zk_host) as zk:
        if zk.exists(node_path, watch=None):
            children = zk.get_children(node_path)
            for child in children:
                # 子节点太多，逐个获取耗时太久
                # data, stat = zk.get("%s/%s" % (node_path, child), watch=None)
                result[child] = "{}"

    return result


def get_zk_conf_content(zk_host, node_path):
    # 从zk中获取节点配置信息
    data = ""
    with open_zk(zk_host) as zk:
        if zk.exists(node_path, watch=None):
            data, stat = zk.get(node_path, watch=None)

    return data


def set_datanode_content(zk_host, node_path, content):
    # 设置zk节点上的值
    with open_zk(zk_host) as zk:
        # 始终使用删除后创建节点的方式设置节点上的配置信息
        if zk.exists(node_path, watch=None):
            zk.delete(node_path)
        zk.create(node_path, content)


def add_sequence_node(zk_host, node_path, content):
    with open_zk(zk_host) as zk:
        # 创建PERSISTENT_SEQUENTIAL节点，并自动创建路径上的parent节点
        return zk.create(node_path, bytes(content), None, False, True, True)


def delete_node(zk_host, node_path):
    """
    删除zk节点, 如果不存在,无影响
    :param zk_host: zk host
    :param node_path: 节点路径
    """
    with open_zk(zk_host) as zk:
        # 始终使用删除后创建节点的方式设置节点上的配置信息
        if zk.exists(node_path, watch=None):
            zk.delete(node_path)
