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
from functools import partial
from threading import RLock

from kazoo.protocol.states import KazooState
from kazoo.recipe.cache import TreeCache, TreeEvent

module_logger = logging.getLogger(__name__)

status_lock = RLock()


def _connection_listener(state):
    if state == KazooState.LOST:
        module_logger.warning('Register somewhere that the session was lost.')
    elif state == KazooState.SUSPENDED:
        module_logger.warning('Handle being disconnected from Zookeeper.')
    else:
        module_logger.info('Handle being connected/reconnected to Zookeeper.')


def join_the_party(zk_client, path, data):
    zk_client.add_listener(_connection_listener)
    party = zk_client.Party(path, data)
    zk_client.start()
    party.join()
    return party


def leave_the_party(party):
    party.leave()


def track_nodes_status(zk_client, path, status_dct, cnt=20):
    t = TreeCache(zk_client, path)
    t.listen(partial(party_status_renew, party_status=status_dct, tree_cache=t, path=path))
    zk_client.start()
    t.start()
    for i in range(cnt):
        if status_dct.get('inited'):
            break
        time.sleep(0.1)
    return t


def get_party_status(tree_cache, path):
    children = tree_cache.get_children(path)
    children_status = [tree_cache.get_data(path + '/' + child) for child in children]
    for status in children_status:
        if not status:
            return [], None
    sorted_status = sorted(children_status, key=lambda k: k.stat.mzxid)
    leader = sorted_status[0] if sorted_status else None
    module_logger.info('Sorted status is {}, leader is {}'.format(sorted_status, leader))
    return sorted_status, leader


def party_status_renew(
    event,
    party_status,
    tree_cache,
    path,
):
    if event.event_type in (
        TreeEvent.NODE_ADDED,
        TreeEvent.NODE_UPDATED,
        TreeEvent.NODE_REMOVED,
        TreeEvent.CONNECTION_RECONNECTED,
        TreeEvent.INITIALIZED,
        TreeEvent.CONNECTION_SUSPENDED,
    ):
        if event.event_type == TreeEvent.INITIALIZED:
            party_status['inited'] = True
        status = get_party_status(
            tree_cache,
            path,
        )
        with status_lock:
            party_status['sorted_status'] = status[0]
            party_status['leader'] = status[1]
