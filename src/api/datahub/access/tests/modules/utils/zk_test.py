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
import datetime
import os

import kazoo
import pytest
from conf import dataapi_settings
from datahub.access.utils import zk_helper

from datahub import pizza_settings


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.skip
def test_update_or_create():
    raw_data_id = 123
    config = str(datetime.datetime.now())
    beacon_node_path = os.path.join(pizza_settings.BEACON_NODE_PATH, str(raw_data_id))

    command_retry = kazoo.retry.KazooRetry()
    connection_retry = kazoo.retry.KazooRetry()
    zk = kazoo.client.KazooClient(
        hosts=dataapi_settings.DATA_ZK_HOST,
        connection_retry=connection_retry,
        command_retry=command_retry,
    )
    zk.start()
    zk_helper.ensure_dir(zk, pizza_settings.BEACON_NODE_PATH)
    if zk.exists(beacon_node_path):
        zk.set(beacon_node_path, config)
    else:
        zk.create(beacon_node_path, config)
    ret = zk.get(beacon_node_path)
    assert ret[0] == config
    zk.stop()
