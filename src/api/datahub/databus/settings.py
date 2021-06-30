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

import os

from conf import dataapi_settings
from conf.dataapi_settings import (
    ACCESS_API_HOST,
    ACCESS_API_PORT,
    AUTH_API_HOST,
    AUTH_API_PORT,
    DATABUS_API_HOST,
    DATABUS_API_PORT,
    DATAFLOW_API_HOST,
    DATAFLOW_API_PORT,
    DATAMANAGE_API_HOST,
    DATAMANAGE_API_PORT,
    HUB_MANAGER_API_HOST,
    HUB_MANAGER_API_PORT,
    META_API_HOST,
    META_API_PORT,
    STOREKIT_API_HOST,
    STOREKIT_API_PORT,
)
from datahub.common.const import CLICKHOUSE, HDFS, ICEBERG, IGNITE

RUN_VERSION = dataapi_settings.RUN_VERSION


HUB_MANAGER_API_URL = "http://{host}:{port}/v3/hubmanager".format(host=HUB_MANAGER_API_HOST, port=HUB_MANAGER_API_PORT)
AUTH_API_URL = "http://{host}:{port}/v3/auth/".format(host=AUTH_API_HOST, port=AUTH_API_PORT)
META_API_URL = "http://{host}:{port}/v3/meta/".format(host=META_API_HOST, port=META_API_PORT)
STOREKIT_API_URL = "http://{host}:{port}/v3/storekit/".format(host=STOREKIT_API_HOST, port=STOREKIT_API_PORT)
DATAFLOW_API_URL = "http://{host}:{port}/v3/dataflow/".format(host=DATAFLOW_API_HOST, port=DATAFLOW_API_PORT)
ACCESS_API_URL = "http://{host}:{port}/v3/access/".format(host=ACCESS_API_HOST, port=ACCESS_API_PORT)
DATABUS_API_URL = "http://{host}:{port}/v3/databus/".format(host=DATABUS_API_HOST, port=DATABUS_API_PORT)
DATAMANAGE_API_URL = "http://{host}:{port}/v3/datamanage/".format(host=DATAMANAGE_API_HOST, port=DATAMANAGE_API_PORT)

LHOTSE_API_URL = dataapi_settings.LHOTSE_API_URL if hasattr(dataapi_settings, "LHOTSE_API_URL") else "http://127.0.0.1"
TUBE_TASK_CONFIG_ZK = (
    dataapi_settings.TUBE_TASK_CONFIG_ZK if hasattr(dataapi_settings, "TUBE_TASK_CONFIG_ZK") else "127.0.0.1:2181"
)
TUBE_MATER_DOMAIN = dataapi_settings.TUBE_MATER_DOMAIN if hasattr(dataapi_settings, "TUBE_MATER_DOMAIN") else ""
TUBE_TASK_TID_CONFIG = "/databus/config/tube"
TUBE_PULSAR_TASK_TID_CONFIG = "/databus/config/pulsar/tdbank"
TUBE_TASK_TASKS_MAX = 3
TUBE_TASK_PROCESSOR_NUM = 3

TYPE_KAFKA = "kafka"
TYPE_PULSAR = "pulsar"

TYPE_SINK = "sink"
TYPE_SOURCE = "source"
TYPE_TRANSPORT = "transport"

LZ_TIMEOUT_HOUR = dataapi_settings.LZ_TIMEOUT_HOUR if hasattr(dataapi_settings, "LZ_TIMEOUT_HOUR") else 1
LZ_TIMEOUT_DAY = dataapi_settings.LZ_TIMEOUT_DAY if hasattr(dataapi_settings, "LZ_TIMEOUT_DAY") else 1

CHANNEL_VERSION = dataapi_settings.CHANNEL_VERSION

TRANSPORT_STORAGE_SOURCE_SET = [HDFS]
TRANSPORT_STORAGE_SINK_SET = [CLICKHOUSE, HDFS, ICEBERG, IGNITE]

JAVA_CMD = "%s/bin/java" % os.environ["JAVA_HOME"] if "JAVA_HOME" in os.environ else "java"

ETL_HINTS = {
    "HashMap": ["items", "access_obj", "assign_obj", "assign_json", "zip"],
    "LinkedHashMap": ["items", "access_obj", "assign_obj", "assign_json", "zip"],
    "ArrayList": ["iterate", "access_pos", "assign_pos", "pop"],
    "String": ["assign_value", "csvline", "from_json", "replace", "split", "splitkv"],
    "Integer": ["assign_value"],
    "Long": ["assign_value"],
    "Double": ["assign_value"],
    "Other": ["assign_value"],
}

ETL_TEMPALTE = {
    "from_json": {"type": "fun", "method": "from_json", "args": []},
    "csvline": {"type": "fun", "method": "csvline", "args": []},
    "replace": {"type": "fun", "method": "replace", "args": ["", ""]},
    "split": {"type": "fun", "method": "split", "args": ["|"]},
    "iterate": {"args": [], "type": "fun", "method": "iterate"},
    "access_obj": {"subtype": "access_obj", "key": "", "type": "access", "keys": []},
    "access_pos": {"index": 0, "type": "access", "subtype": "access_pos"},
    "assign_value": {
        "type": "assign",
        "subtype": "assign_value",
        "assign": {"assign_to": "", "type": "string"},
    },
    "assign_obj": {"subtype": "assign_obj", "type": "assign", "assign": [], "keys": []},
    "assign_pos": {"assign": [], "subtype": "assign_pos", "type": "assign"},
    "assign_json": {"assign": [], "subtype": "assign_json", "type": "assign"},
    "pop": {"type": "fun", "method": "pop"},
    "splitkv": {"type": "fun", "method": "splitkv", "args": ["", ""]},
    "zip": {"type": "fun", "method": "zip", "args": ["", ""]},
    "items": {"type": "fun", "method": "items", "args": []},
}

KAFKA_ACL_SCRIPT = ""
KAFKA_CONFIG_SCRIPT = ""

ZK_RT_CONFIG_CHANGE_PATH = "/config/databus/rt_config_change/rt"

# 专用的Kafka配置集群的ZK地址，用于知会总线集群，其RT的配置发生了变化
CONFIG_ZK_ADDR = dataapi_settings.CONFIG_ZK_ADDR
# kafka op 的地址，用于读取清洗失败的采样数据
KAFKA_OP_HOST = dataapi_settings.KAFKA_OP_HOST
CLEAN_BAD_MSG_TOPIC_PREFIX = "bkdata_bad_msg"

DATANODE_CLUSTER_NAME = (
    dataapi_settings.DATANODE_CLUSTER_NAME
    if hasattr(dataapi_settings, "DATANODE_CLUSTER_NAME")
    else {"": "puller-datanode-M"}
)
BKHDFS_CLUSTER_NAME = (
    dataapi_settings.BKHDFS_CLUSTER_NAME if hasattr(dataapi_settings, "BKHDFS_CLUSTER_NAME") else ["puller-bkhdfs-M"]
)
BKHDFS_PULSAR_CLUSTER_NAME = (
    dataapi_settings.BKHDFS_PUSLAR_CLUSTER_NAME
    if hasattr(dataapi_settings, "BKHDFS_PUSLAR_CLUSTER_NAME")
    else ["puller-bkhdfs-pulsarinner-3M"]
)

PULLER_BKHDFS_WORKERS = getattr(dataapi_settings, "PULLER_BKHDFS_WORKERS", 5)

# 默认24个worker，对应每天24个小时目录
PULLER_HDFSICEBERG_WORKERS = getattr(dataapi_settings, "PULLER_HDFSICEBERG_WORKERS", 24)

LHOTSE_CLUSTER = "tdw-lhotse"

MODULE_CLEAN = "clean"
MODULE_SHIPPER = "shipper"
MODULE_PULLER = "puller"
MODULE_TRANSPORT = "transport"

COMPONENT_CLEAN = "clean"
COMPONENT_DATANODE = "datanode"
COMPONENT_JDBC = "jdbc"
COMPONENT_KAFKA = "kafka"
COMPONENT_QUEUE = "queue"
COMPONENT_PULSAR_QUEUE = "queue_pulsar"
COMPONENT_BKHDFS = "bkhdfs"
COMPONENT_PGSQL = "postgresql"
COMPONENT_DRUID = "druid"
COMPONENT_ESLOG = "eslog"
COMPONENT_ES = "es"
COMPONENT_HDFS = "hdfs"
COMPONENT_HERMES = "hermes"
COMPONENT_TDBANK = "tdbank"
COMPONENT_TDW = "tdw"
COMPONENT_HTTP = "http"
COMPONENT_TDWHDFS = "tdwhdfs"
COMPONENT_TSDB = "tsdb"
COMPONENT_TPG = "tpg"
COMPONENT_TREDIS = "tredis"
COMPONENT_TSPIDER = "tspider"
COMPONENT_TRANSPORT = "transport"

# 清洗RT的类型
CLEAN_TRT_CODE = "clean"
# 固化RT的类型
DATANODE_TRT_CODE = "transform"

# 清洗归属的项目ID
CLEAN_PROJECT_ID = 4

# 可用
AVAILABLE_STORAGE_LIST = ["es", "eslog", "mysql", "tspider", "tsdb"]
STORAGE_EXPIRE_DAYS = [7, 15, 30]

SHIPPER_TARGET = [
    "es",
    "hdfs",
    "hermes",
    "tsdb",
    "kafka",
    "tredis",
    "tspider",
    "queue",
    "druid",
    "mysql",
    CLICKHOUSE,
]

BATCH_STORAGE_CHANNEL = ["kafka", "pulsar", "hdfs"]
STREAM_STORAGE_CHANNEL = ["kafka", "pulsar"]

CLUSTER_PROPS = "cluster.props"
OTHER_PROPS = "other.props"

# 用于定义总线任务集群的一些配置
PULSAR_CLUSTER_BASIC_CONF = {
    "numFunctionPackageReplicas": 1,
    "failureCheckFreqMs": 30000,
    "rescheduleTimeoutMs": 60000,
    "initialBrokerReconnectMaxRetries": 60,
    "assignmentWriteMaxRetries": 60,
    "instanceLivenessCheckFreqMs": 30000,
    "schedulerClassName": "org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler",
}

KAFKA_CLUSTER_BASE_CONF = {
    "rebalance.timeout.ms": 120000,
    "task.shutdown.graceful.timeout.ms": 30000,
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": False,
    "value.converter.schemas.enable": False,
    "internal.key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "internal.value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "internal.key.converter.schemas.enable": False,
    "internal.value.converter.schemas.enable": False,
}

DATABUS_CLUSTER_BASE_CONF = {
    TYPE_PULSAR: PULSAR_CLUSTER_BASIC_CONF,
    TYPE_KAFKA: KAFKA_CLUSTER_BASE_CONF,
}

DATABUS_CONSUMER_CONF = {
    "auto.offset.reset": "earliest",
    "max.poll.records": 500,
    "fetch.min.bytes": 102400,
    "fetch.max.wait.ms": 5000,
    "fetch.message.max.bytes": 5242880,
    "max.partition.fetch.bytes": 5242880,
    "heartbeat.interval.ms": 15000,
    "session.timeout.ms": 120000,
    "request.timeout.ms": 130000,
    "reconnect.backoff.ms": 500,
    "retry.backoff.ms": 1000,
}

DATABUS_MONITOR_CONF = {
    "badmsg.topic": "bkdata_bad_msg",
    "producer.topic": "bkdata_data_monitor_databus_metrics591",
    "producer.request.timeout.ms": 60000,
    "producer.retries": 5,
    "producer.max.block.ms": 60000,
    "producer.acks": "all",
    "producer.max.request.size": 3145728,
    "producer.compression.type": "snappy",
    "producer.max.in.flight.requests.per.connection": 1,
}

DATABUS_CONNECTOR_CLUSTER_CONF = {
    TYPE_PULSAR: {
        "clean": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.linger.ms": 5000,
                "producer.batch.size": 1048576,
                "producer.max.request.size": 3145728,
                "producer.send.buffer.bytes": 1048576,
                "producer.max.in.flight.requests.per.connection": 1,
                "producer.retries": 0,
            }
        },
        "eslog": {OTHER_PROPS: {"connector.msg.type": "etl"}},
        "hdfs": {
            OTHER_PROPS: {
                "connector.flush.size": 20000,
                "connector.rotate.interval.ms": 300000,
                "connector.multi.paths.rotate.interval.ms": 240000,
            }
        },
        "mysql": {OTHER_PROPS: {"connector.batch.timeout.ms": 180000}},
        "queue": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 1,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
            }
        },
        "queue_pulsar": {
            OTHER_PROPS: {
                "producer.sendTimeoutMs": 10000,
                "producer.batchingEnabled": True,
                "producer.maxPendingMessages": 1000,
                "producer.batchingMaxPublishDelayMicros": 3000,
                "producer.batchingMaxMessages": 1000,
                "producer.blockIfQueueFull": True,
            }
        },
        "tspider": {OTHER_PROPS: {"connector.batch.timeout.ms": 180000}},
        "postgresql": {OTHER_PROPS: {"connector.batch.timeout.ms": 180000}},
        "beacon": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            }
        },
        "bkhdfs": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
                "connector.worker.num": PULLER_BKHDFS_WORKERS,
            }
        },
        "datanode": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            }
        },
        "tdwhdfs": {
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            }
        },
    },
    TYPE_KAFKA: {
        "clean": {
            CLUSTER_PROPS: {"value.converter": "com.tencent.bkdata.connect.common.convert.RawConverter"},
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.linger.ms": 5000,
                "producer.batch.size": 1048576,
                "producer.max.request.size": 3145728,
                "producer.send.buffer.bytes": 1048576,
                "producer.max.in.flight.requests.per.connection": 1,
                "producer.retries": 0,
            },
        },
        "druid": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
        "clickhouse": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
        "es": {
            CLUSTER_PROPS: {
                "transport.batch.record.count": 30000,
                "transport.bulk.size.mb": 20,
                "transport.flush.interval.ms": 60000,
                "transport.max.in.flight.requests": 10,
                "transport.retry.backoff.ms": 500,
                "transport.max.retries": 5,
                "es.min.free.memory.mb": 256,
            },
            OTHER_PROPS: {},
        },
        "eslog": {
            CLUSTER_PROPS: {
                "transport.batch.record.count": 30000,
                "transport.bulk.size.mb": 20,
                "transport.flush.interval.ms": 60000,
                "transport.max.in.flight.requests": 10,
                "transport.retry.backoff.ms": 500,
                "transport.max.retries": 5,
                "es.min.free.memory.mb": 256,
            },
            OTHER_PROPS: {"connector.msg.type": "etl"},
        },
        "hdfs": {
            CLUSTER_PROPS: {},
            OTHER_PROPS: {
                "connector.flush.size": 20000,
                "connector.rotate.interval.ms": 300000,
                "connector.multi.paths.rotate.interval.ms": 240000,
            },
        },
        "hermes": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
        "mysql": {
            CLUSTER_PROPS: {"conn.pool.size": 50},
            OTHER_PROPS: {"connector.batch.timeout.ms": 180000},
        },
        "queue": {
            CLUSTER_PROPS: {},
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 1,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
            },
        },
        "queue_pulsar": {
            CLUSTER_PROPS: {},
            OTHER_PROPS: {
                "producer.sendTimeoutMs": 10000,
                "producer.batchingEnabled": True,
                "producer.maxPendingMessages": 1000,
                "producer.batchingMaxPublishDelayMicros": 3000,
                "producer.batchingMaxMessages": 1000,
                "producer.blockIfQueueFull": True,
            },
        },
        "tredis": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
        "tsdb": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
        "tspider": {
            CLUSTER_PROPS: {"conn.pool.size": 50},
            OTHER_PROPS: {"connector.batch.timeout.ms": 180000},
        },
        "postgresql": {
            CLUSTER_PROPS: {"conn.pool.size": 50},
            OTHER_PROPS: {"connector.batch.timeout.ms": 180000},
        },
        "beacon": {
            CLUSTER_PROPS: {
                "cache.length": 5000,
                "thread.block.time.ms": 100,
                "worker.thread.num": 20,
                "worker.max.in.progress.request": 50,
                "worker.max.pending.request.per.partition": 1,
            },
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            },
        },
        "bkhdfs": {
            CLUSTER_PROPS: {
                "read.max.records": 50000000,
                "read.parquet.max.records": 80000000,
            },
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
                "connector.worker.num": PULLER_BKHDFS_WORKERS,
            },
        },
        "datanode": {
            CLUSTER_PROPS: {},
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            },
        },
        "tdwhdfs": {
            CLUSTER_PROPS: {},
            OTHER_PROPS: {
                "producer.acks": "all",
                "producer.retries": 5,
                "producer.max.in.flight.requests.per.connection": 5,
                "producer.batch.size": 1048576,
                "producer.linger.ms": 500,
                "producer.buffer.memory": 52428800,
            },
        },
        "tdbank": {
            CLUSTER_PROPS: {"tube.zookeeper": TUBE_TASK_CONFIG_ZK},
            OTHER_PROPS: {},
        },
        "tdw": {CLUSTER_PROPS: {}, OTHER_PROPS: {}},
    },
}

QUEUE_API_URL = dataapi_settings.QUEUE_API_URL if hasattr(dataapi_settings, "QUEUE_API_URL") else ""
QUEUE_SASL_ENABLE = dataapi_settings.QUEUE_SASL_ENABLE if hasattr(dataapi_settings, "QUEUE_SASL_ENABLE") else False
QUEUE_SASL_USER = dataapi_settings.QUEUE_SASL_USER if hasattr(dataapi_settings, "QUEUE_SASL_USER") else "bkdata_admin"
QUEUE_SASL_PASS = dataapi_settings.QUEUE_SASL_PASS if hasattr(dataapi_settings, "QUEUE_SASL_PASS") else ""
CRYPT_INSTANCE_KEY = dataapi_settings.CRYPT_INSTANCE_KEY

default_time_format = "%Y-%m-%d %H:%M:%S"

log_item_format = "log-%s-table_%s"

DEFAULT_GEOG_AREA_TAG = dataapi_settings.DEFAULT_GEOG_AREA_TAG
TAG_TARGET_LIMIT = 200
pulsar_tenant = "public"
pulsar_namespace = "migration"
pulsar_admin_url = (
    dataapi_settings.PULSAR_ADMIN_URL
    if hasattr(dataapi_settings, "PULSAR_ADMIN_URL")
    else {dataapi_settings.DEFAULT_GEOG_AREA_TAG: ""}
)
pulsar_io_url = (
    dataapi_settings.PULSAR_IO_URL
    if hasattr(dataapi_settings, "PULSAR_IO_URL")
    else {dataapi_settings.DEFAULT_GEOG_AREA_TAG: ""}
)
task_start_interval = 5
migration_source_supported = ["tspider"]
migration_dest_supported = ["hdfs"]

pulsar_queue_token = dataapi_settings.PULSAR_QUEUE_TOKEN if hasattr(dataapi_settings, "PULSAR_QUEUE_TOKEN") else ""

CONNECTOR_STATE_RUNNING = "RUNNING"

TGPInCharge = dataapi_settings.TGPInCharge if hasattr(dataapi_settings, "TGPInCharge") else ""
CONSUMER_TIMEOUT = dataapi_settings.CONSUMER_TIMEOUT if hasattr(dataapi_settings, "CONSUMER_TIMEOUT") else 1500

need_auth_rt = dataapi_settings.NEED_AUTH_RT if hasattr(dataapi_settings, "NEED_AUTH_RT") else []

PULSAR_IO_URL_TPL = "http://%s/admin/v3/%ss/%s/%s/%s"
PULSAR_PARTITION_TOPIC_URL_TPL = "http://%s/admin/v2/persistent/%s/%s/%s"
PULSAR_DEFAULT_PARTITION = (
    dataapi_settings.PULSAR_DEFAULT_PARTITION if hasattr(dataapi_settings, "PULSAR_DEFAULT_PARTITION") else 3
)
PULSAR_SCHEMA_URL_TPL = "http://%s/admin/v2/schemas/%s/%s/%s/schema"

CONSUL_HOST = dataapi_settings.CONSUL_HOST if hasattr(dataapi_settings, "CONSUL_HOST") else "localhost"
CONSUL_PORT = dataapi_settings.CONSUL_PORT if hasattr(dataapi_settings, "CONSUL_PORT") else 8081

# pulsar tenant namespace
PULSAR_OUTER_TENANT = (
    dataapi_settings.PULSAR_OUTER_TENANT if hasattr(dataapi_settings, "PULSAR_OUTER_TENANT") else "public"
)
PULSAR_OUTER_NAMESPACE = (
    dataapi_settings.PULSAR_OUTER_NAMESPACE if hasattr(dataapi_settings, "PULSAR_OUTER_NAMESPACE") else "data"
)
PULSAR_TASK_TENANT = (
    dataapi_settings.PULSAR_TASK_TENANT if hasattr(dataapi_settings, "PULSAR_TASK_TENANT") else "datahub"
)
PULSAR_TASK_NAMESPACE = (
    dataapi_settings.PULSAR_TASK_NAMESPACE if hasattr(dataapi_settings, "PULSAR_TASK_NAMESPACE") else "databus-cluster"
)
PULSAR_TASK_STATUS_NAMESPACE = "stream"


DEFAULT_MAX_RECORDS = 10000000
CLEAN_TABLE_TOPIC_TEMPLATE = "table_%s"
DEFAULT_THIN_CLIENT_THRESHOLD = 5000000

ES_DEFAULT_VERSION = dataapi_settings.ES_DEFAULT_VERSION if hasattr(dataapi_settings, "ES_DEFAULT_VERSION") else "5.4.0"
AUTH_TDW_URL = dataapi_settings.AUTH_TDW_URL if hasattr(dataapi_settings, "AUTH_TDW_URL") else ""
DNS_SERVICE = dataapi_settings.DNS_SERVICE if hasattr(dataapi_settings, "DNS_SERVICE") else ""

PULSAR_READER_URL = HUB_MANAGER_API_URL + "/pulsar/tail/"
CLEAN_RESTFUL_READER_URL = HUB_MANAGER_API_URL + "/clean/verify/"
# 是否使用data_route 从gse获取data_id
SYNC_GSE_ROUTE_DATA_USE_API = (
    dataapi_settings.SYNC_GSE_ROUTE_DATA_USE_API if hasattr(dataapi_settings, "SYNC_GSE_ROUTE_DATA_USE_API") else False
)
DATA_ROUTE_NAME_TEMPLATE = "bkdata.%s.%s"

# transport相关
QUERY_FIELDS_SQL = "select name, type from system.columns where database = '{}' and table = '{}' order by position asc"
PULSAR_STREAM_TOPIC = "persistent://datahub/stream/{}"
METAAPI_NOT_EXIST_CODE = "1521050"

# tcaplus相关的
TCA_CREATE_TOKEN_URL = getattr(dataapi_settings, "TCA_CREATE_TOKEN_URL", "")
TCA_API_GET_SERVER_LIST_URL = getattr(dataapi_settings, "TCA_API_GET_SERVER_LIST_URL", "")
TCA_TOKEN_CMD = 10000
TCA_IP_TYPE = "webservicerest"
REGISTER_TYPE = "register_type"
AUTH_TYPE = "authtype"
TCA_WEBSERVICE = "tca_webservice"
SCRAM_SHA_512 = "SCRAM-SHA-512"
SASL_PLAINTEXT = "SASL_PLAINTEXT"
CHANNEL_CLUSTER_NAME_DEFAULT_VALUE = getattr(dataapi_settings, "CHANNEL_CLUSTER_NAME_DEFAULT_VALUE", "")

DATABUS_SHIPPERS_DICT = getattr(dataapi_settings, "DATABUS_SHIPPERS_DICT", {})
DATABUS_PULLERS_DICT = getattr(dataapi_settings, "DATABUS_PULLERS_DICT", {})
CONFIG_SERVER_API_URL = dataapi_settings.GSE_API_URL
SUCCESS_CODE = "1500200"
