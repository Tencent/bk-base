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

import pytest
from common.api.base import DataResponse  # noqa
from datahub.access.tests.utils import delete, get, patch, post  # noqa
from datahub.databus.tests.fixture.access_raw_data import test_access_raw_data  # noqa
from datahub.databus.tests.fixture.access_resource_info import (  # noqa
    test_access_resource_info,
)
from datahub.databus.tests.fixture.channel import test_channel_id  # noqa
from datahub.databus.tests.fixture.clean_fixture import add_clean  # noqa
from datahub.databus.tests.fixture.cluster_fixture import add_cluster  # noqa
from datahub.databus.tests.fixture.connector_cluster import (  # noqa
    test_connector_cluster,
    test_connector_cluster1,
)
from datahub.databus.tests.fixture.databus_connector_task import (  # noqa
    test_databus_connector_task,
    test_databus_connector_task_dup,
)
from datahub.databus.tests.fixture.databus_shipper_info import (  # noqa
    test_databus_shipper_info,
)
from datahub.databus.tests.fixture.databus_transform_processing import (  # noqa
    test_databus_transform_processing,
)
from datahub.databus.tests.mock_api import connector_cluster, result_table


class Ret(object):
    status_code = 200
    text = ""

    @staticmethod
    def json():
        return {
            "name": "xxx",
            "config": {},
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "x.x.x.x:10001"}],
            "connector": {"state": "RUNNING", "worker_id": "x.x.x.x:10001"},
        }


result_table_id = "10000_test_clean_rt"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures(
    "test_channel_id",
    "test_connector_cluster",
    "add_clean",
    "test_access_raw_data",
    "add_cluster",
    "test_databus_shipper_info",
)
@pytest.mark.usefixtures()
def test_create_migrate(mocker):
    result_table.get_storages_ok(result_table_id)
    result_table.get_ok()
    connector_cluster.get_ok()

    params = {
        "result_table_id": result_table_id,
        "source": "mysql",
        "dest": "hdfs",
        "start": "2019-10-01 00:00:00",
        "end": "2019-10-01 01:10:00",
        "overwrite": True,
    }
    res = post("/v3/databus/migrations/", params)
    print(res)
    id1 = res["data"][0]["id"]
    id2 = res["data"][1]["id"]

    params = {"status": "running", "task_id": id1}
    res = post("/v3/databus/migrations/update_task_status/", params)
    print(json.dumps(res))

    params = {"status": "finish", "task_id": id2}
    res = post("/v3/databus/migrations/update_task_status/", params)
    print(json.dumps(res))

    res = get("/v3/databus/migrations/")
    print(json.dumps(res))

    print("get rt_id")
    res = get("/v3/databus/migrations/" + result_table_id + "/")
    print(json.dumps(res))

    print("get task id")
    res = get("/v3/databus/migrations/" + str(id1) + "/")
    print(json.dumps(res))
