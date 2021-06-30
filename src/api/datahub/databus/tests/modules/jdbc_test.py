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

import json

import httpretty as hp
import pytest
from datahub.databus.pullers.factory import PullerFactory
from datahub.databus.task.task_utils import get_channel_info_by_id
from datahub.databus.tests.fixture.channel_fixture import add_channel  # noqa
from datahub.databus.tests.fixture.connector_cluster import (  # noqa
    test_connector_cluster,
)
from datahub.databus.tests.fixture.mock_response import Ret
from datahub.databus.tests.fixture.puller_jdbc import *  # noqa

from datahub.databus import model_manager


@pytest.mark.usefixtures("test_puller_jdbc_config")
@pytest.mark.usefixtures("test_connector_cluster")
@pytest.mark.django_db
def test_build_jdbc_conf(mocker):
    """任务提交正常流程"""
    hp.enable()
    hp.register_uri(
        hp.POST,
        "x.x.x.x:10000" + "/connectors/puller-jdbc_123/",
        body=json.dumps({"result": True}),
    )
    mocker.patch("datahub.databus.task.kafka.task.get_task", return_value=None)
    mocker.patch("requests.put", return_value=Ret(200, "OK"))
    mocker.patch("requests.delete", return_value=Ret(200, "OK"))
    mocker.patch("requests.get", return_value=Ret(200, "OK"))

    data_id = 123
    raw_data = model_manager.get_raw_data_by_id(data_id, False)
    storage_channel = get_channel_info_by_id(raw_data.storage_channel_id)
    bk_username = "admin"
    db_puller = PullerFactory.get_puller_factory().get_puller("jdbc")(data_id, raw_data, storage_channel, bk_username)
    db_puller.start()
