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
import pytest
from datahub.access.tests import db_helper


@pytest.fixture
def test_connector_cluster():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="mysql-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            consumer_bootstrap_servers="",
            consumer_props="",
            module="mysql",
            component="kafka_cluster_name",
            state="RUNNING",
            created_by="",
            updated_by="",
            description="",
        )

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="mysql-kafka_cluster_name-M1",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="mysql",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="puller-datanode-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel2",
            module="puller",
            component="datanode",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="puller-tdwhdfs-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="puller",
            component="tdwhdfs",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="puller-jdbc-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_kafka",
            cluster_type="kafka",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="puller",
            component="jdbc",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="hdfs-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="hdfs",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="tredis-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="tredis",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="es-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            module="es",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="eslog-kafka_cluster_name4-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="es",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="druid-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            channel_name="test_channel",
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            module="druid",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="hermes-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            module="hermes",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="tsdb-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            module="tsdb",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="mysql-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            module="mysql",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )
        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="queue-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            channel_name="test_channel",
            module="queue",
            component="kafka_cluster_name",
            consumer_bootstrap_servers="",
            consumer_props="",
            description="",
            state="RUNNING,",
        )

    try:
        yield
    finally:
        delete_channel_id()


@pytest.fixture
def test_connector_cluster1():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_connector_cluster_config",
            cluster_name="mysql-kafka_cluster_name-M",
            cluster_rest_domain="x.x.x.x",
            cluster_rest_port=10000,
            cluster_bootstrap_servers="kafka.domain:9092",
            cluster_props="",
            consumer_bootstrap_servers="",
            consumer_props="",
            module="mysql",
            component="kafka_cluster_name",
            state="RUNNING",
            created_by="",
            updated_by="",
            description="",
        )

    try:
        yield
    finally:
        delete_channel_id()


def delete_channel_id():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_connector_cluster_config
        """,
        )
