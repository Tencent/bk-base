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

import mock
import pytest


@pytest.fixture(scope="module")
def patch_data_sets_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/data_sets.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_data_set_infos",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_data_sets_from_redis():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/data_sets.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_data_set_infos_from_redis",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_data_operations_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/data_operations.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_data_operations",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_data_operations_from_redis():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/data_operations.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_data_operations_from_redis",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_flow_infos_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/flow_infos.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_flow_infos",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_flow_infos_from_redis():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/flow_infos.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_flow_infos_from_redis",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_dataflow_infos_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/dataflow_infos.json", "r") as file:
        dataflow_infos = json.loads(file.read())
        patch_func.return_value = {
            int(flow_id): flow_info for flow_id, flow_info in dataflow_infos.items()
        }

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_dataflow_infos",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_dataflow_infos_from_redis():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/dataflow_infos.json", "r") as file:
        dataflow_infos = json.loads(file.read())
        patch_func.return_value = {
            int(flow_id): flow_info for flow_id, flow_info in dataflow_infos.items()
        }

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_dataflow_infos_from_redis",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_alert_configs_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/alert_configs.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_alert_configs",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_alert_shields_fetch():
    patch_func = mock.Mock()
    patch_func.return_value = []

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_alert_shields",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_bizs_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/biz_infos.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_biz_infos",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_projects_fetch():
    patch_func = mock.Mock()

    with open("tests/dmonitor/data/metadata/project_infos.json", "r") as file:
        patch_func.return_value = json.loads(file.read())

    with mock.patch(
        target="common.mixins.meta_cache_mixin.MetaCacheMixin.fetch_project_infos",
        new=patch_func,
    ):
        yield


@pytest.fixture
def patch_collect_kafka_data(topic):
    from confluent_kafka import Message, TIMESTAMP_CREATE_TIME

    def side_effect(*args, **kwargs):
        messages = []

        with open(f"tests/dmonitor/data/metrics/{topic}.data") as file:
            message_content = file.readline()

            while message_content.strip():
                message = mock.Mock(Message)
                message_data = json.loads(message_content)
                message.value.return_value = message_data["value"].encode("utf-8")
                message.error.return_value = message_data["error"]
                message.partition.return_value = message_data["partition"]
                message.topic.return_value = message_data["topic"]
                message.offset.return_value = message_data["offset"]
                message.timestamp.return_value = (
                    TIMESTAMP_CREATE_TIME,
                    message_data["timestamp"],
                )
                messages.append(message)
                message_content = file.readline()

        messages = sorted(messages, key=lambda x: x.timestamp()[0])

        return messages

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target="common.mixins.consumer_mixin.ConsumerMixin.collect_kafka_data",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_produce_metric():
    from confluent_kafka import Producer

    def side_effect(*args, **kwargs):
        producer = mock.Mock(Producer)
        producer.produce.return_value = 1
        producer.poll.return_value = 0
        return producer

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target="common.kafka.KafkaConnectionHandler.get_confluent_producer",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_redis_conn():
    import redis

    def side_effect(*args, **kwargs):
        redis_conn = mock.Mock(redis.StrictRedis)
        redis_conn.lpush.return_value = 1
        return redis_conn

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target="common.redis.ConnctionHandler.__getitem__",
        new=patch_func,
    ):
        yield


@pytest.fixture
def patch_influxdb_query(influx_metric_name):
    from common.influxdb import InfluxdbConnection

    def side_effect(*args, **kwargs):
        influx_conn = mock.Mock(InfluxdbConnection)
        with open(f"tests/dmonitor/data/metrics/{influx_metric_name}.json") as file:
            influx_conn.query.return_value = json.loads(file.read())
        return influx_conn

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target="common.influxdb.InfluxdbConnectionHandler.__getitem__",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_db_write():
    patch_func = mock.Mock()
    patch_func.return_value = True

    with mock.patch(
        target="common.mixins.db_write_mixin.DbWriteMixin.push_task",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_alert_send():
    from api.base import DataResponse

    patch_func = mock.Mock()
    patch_func.return_value = DataResponse(
        {
            "result": True,
            "message": "ok",
            "error": {},
            "code": "",
            "data": {},
        }
    )

    with mock.patch(
        target="api.datamanage_api.alerts.send",
        new=patch_func,
    ):
        yield


@pytest.fixture(scope="module")
def patch_batch_executions_fetch():
    patch_func = mock.Mock()
    patch_func.return_value = []

    with mock.patch(
        target="dmonitor.mixins.DmonitorMetaCacheMixin.fetch_batch_executions_by_time",
        new=patch_func,
    ):
        yield
