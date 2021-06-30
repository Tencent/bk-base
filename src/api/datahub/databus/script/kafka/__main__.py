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
import time

from common.log import logger
from conf.dataapi_settings import DEFAULT_GEOG_AREA_TAG, KAFKA_CONFIG_HOST
from kafka.client_async import KafkaClient
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Int16, Int32, Schema, String

from datahub.databus import settings, zk_helper

EXPIRE_TIME = "315360000000"


class CreateTopicsResponse_v0(Struct):
    API_KEY = 19
    API_VERSION = 0
    SCHEMA = Schema(("topic_error_codes", Array(("topic", String("utf-8")), ("error_code", Int16))))


class CreateTopicsRequest_v0(Struct):
    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = Schema(
        (
            "create_topic_requests",
            Array(
                ("topic", String("utf-8")),
                ("num_partitions", Int32),
                ("replication_factor", Int16),
                (
                    "replica_assignment",
                    Array(("partition_id", Int32), ("replicas", Array(Int32))),
                ),
                (
                    "configs",
                    Array(
                        ("config_key", String("utf-8")),
                        ("config_value", String("utf-8")),
                    ),
                ),
            ),
        ),
        ("timeout", Int32),
    )

    def expect_response(self):
        return True


def ensure_topic(topic, num_partitions=1, replication_factor=1, configs={}):
    timeout_ms = 3000
    client = KafkaClient(
        bootstrap_servers=KAFKA_CONFIG_HOST[settings.DEFAULT_GEOG_AREA_TAG],
        api_version=(0, 10),
    )
    logger.info("init_databus_topic_config ensure_topic topic:{} config:{}".format(topic, configs))
    if topic not in client.cluster.topics(exclude_internal_topics=True):

        request = CreateTopicsRequest_v0(
            create_topic_requests=[
                (
                    topic,
                    num_partitions,
                    replication_factor,
                    [],  # Partition assignment
                    [(key, value) for key, value in configs.items()],  # Configs
                )
            ],
            timeout=timeout_ms,
        )
        node_id = client.cluster.controller.nodeId

        retry_num = 5
        while not client.ready(node_id):
            if retry_num <= 0:
                raise Exception("controller node not ready")
            time.sleep(1)
            logger.info("[%s] waiting node ready" % topic)
            retry_num -= 1

        future = client.send(node_id, request)
        client.poll(timeout_ms=timeout_ms, future=future)
        result = future.value
        error_code = result.topic_error_codes[0][1]
        # 0: success
        if error_code == 0:
            return
        elif error_code == 38:
            # 节点数量小于 replication_factor数量
            logger.warning(
                "config kafka broker nms less than replication_factor, " "try to create 1 replication_factor topic"
            )
            ensure_topic(topic, num_partitions, 1, configs)
        elif error_code != 36:
            # 36: already exists, check topic
            raise Exception("Unknown error code during creation of topic `{}`: {}".format(topic, error_code))


def get_config(topic):
    with zk_helper.open_zk(settings.CONFIG_ZK_ADDR[settings.DEFAULT_GEOG_AREA_TAG]) as zk:
        ret = zk.get("/config/topics/%s" % topic)
        ret = json.loads(ret[0])
        return ret.get("config", {}) or {}


def set_config(topic, new_config):
    logger.info("init_databus_topic_config ensure_topic topic:{} config:{}".format(topic, new_config))

    old_config = get_config(topic)
    old_config.update(new_config)

    config = {"version": 1, "config": old_config}

    with zk_helper.open_zk(settings.CONFIG_ZK_ADDR[settings.DEFAULT_GEOG_AREA_TAG]) as zk:
        logger.info(json.dumps(config))
        zk.set("/config/topics/%s" % topic, json.dumps(config))

    set_changes(get_change_path(), topic)


def set_changes(path, topic):
    config = {"version": 2, "entity_path": "topics/%s" % topic}

    with zk_helper.open_zk(settings.CONFIG_ZK_ADDR[settings.DEFAULT_GEOG_AREA_TAG]) as zk:
        zk.create(path, json.dumps(config))


def get_change_path():
    with zk_helper.open_zk(settings.CONFIG_ZK_ADDR[settings.DEFAULT_GEOG_AREA_TAG]) as zk:

        children = zk.get_children("/config/changes")
        children.sort()
        if children:
            last_name = children[-1]
            return "/config/changes/config_change_%010d" % (int(last_name.split("_")[2]) + 1)
        else:
            return "/config/changes/config_change_0000000002"


def need_fix(cluster):
    topic = "connect-configs." + cluster
    retention_ms = get_config(topic).get("retention.ms", "")
    logger.info("topic:{} retention_ms: {}, need_fix: {}".format(topic, retention_ms, str(retention_ms != EXPIRE_TIME)))
    return retention_ms != EXPIRE_TIME


def main_method():

    topic_configs = {"retention.ms": EXPIRE_TIME}
    logger.info("init_databus_topic_config config:%s" % topic_configs)
    for m in [
        "clean-outer-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "eslog-outer-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "es-inner-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "hdfs-inner-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "mysql-inner-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "tredis-inner-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "tsdb-inner-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "puller-datanode-%s-M" % DEFAULT_GEOG_AREA_TAG,
        "puller-bkhdfs-%s-M" % DEFAULT_GEOG_AREA_TAG,
    ]:
        ensure_topic("connect-offsets." + m, 5, replication_factor=2, configs=topic_configs)
        ensure_topic("connect-configs." + m, 1, replication_factor=2, configs=topic_configs)
        ensure_topic("connect-status." + m, 5, replication_factor=2, configs=topic_configs)

        if need_fix(m):
            logger.info(
                "WARNING: The configuration of databus cluster [databus_%s] is wrong, and we'll fix it in this "
                "script. Please make sure your data service is working after this procedure." % m
            )
            set_config("connect-offsets." + m, topic_configs)
            set_config("connect-configs." + m, topic_configs)
            set_config("connect-status." + m, topic_configs)
