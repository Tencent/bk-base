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

from conf import dataapi_settings
from conf.dataapi_settings import (
    AUTH_API_HOST,
    AUTH_API_PORT,
    DATABUS_API_HOST,
    DATABUS_API_PORT,
    DATAFLOW_API_HOST,
    DATAFLOW_API_PORT,
    DATAMANAGE_API_HOST,
    DATAMANAGE_API_PORT,
    DATAQUERY_API_HOST,
    DATAQUERY_API_PORT,
    GCS_API_HOST,
    GCS_API_PORT,
    HUB_MANAGER_API_HOST,
    HUB_MANAGER_API_PORT,
    JOBNAVI_API_HOST,
    JOBNAVI_API_PORT,
    META_API_HOST,
    META_API_PORT,
    RESOURCE_CENTER_API_HOST,
    RESOURCE_CENTER_API_PORT,
    RUN_VERSION,
    STOREKIT_API_HOST,
    STOREKIT_API_PORT,
)
from datahub.common.const import (
    ARGS,
    BATCH,
    BATCH_MODEL,
    BIGINT,
    BOOLEAN,
    CHANNEL_CLUSTER,
    CLEAN,
    CLICKHOUSE,
    DATETIME,
    DOUBLE,
    DRUID,
    DTEVENTTIME,
    DTEVENTTIMESTAMP,
    ES,
    ET,
    FIELD,
    FLOAT,
    HDFS,
    HOUR,
    ICEBERG,
    IGNITE,
    INT,
    INTEGER,
    JSON,
    KAFKA,
    LOCALHOST,
    LOCALTIME,
    LONG,
    LONGTEXT,
    METHOD,
    MINUTE,
    MODEL,
    MYSQL,
    OFFSET,
    PARQUET,
    PARTITION_TIME,
    POSTGRESQL,
    PULSAR,
    QUERYSET,
    QUEUE,
    QUEUE_PULSAR,
    REAL,
    RESULT_TABLE,
    ROOT,
    SNAPSHOT,
    STORAGE,
    STORAGE_CLUSTER,
    STREAM,
    STREAM_MODEL,
    STRING,
    TEXT,
    THEDATE,
    TIMESTAMP,
    TINYINT,
    TRANSFORM,
    TSDB,
    VARCHAR,
    VIEW,
)
from django.utils.translation import ugettext as _

META_API_URL = "http://{host}:{port}/v3/meta".format(host=META_API_HOST, port=META_API_PORT)
DATABUS_API_URL = "http://{host}:{port}/v3/databus".format(host=DATABUS_API_HOST, port=DATABUS_API_PORT)
STOREKIT_API_URL = "http://{host}:{port}/v3/storekit".format(host=STOREKIT_API_HOST, port=STOREKIT_API_PORT)
DATAMANAGE_API_URL = "http://{host}:{port}/v3/datamanage/dmonitor".format(
    host=DATAMANAGE_API_HOST, port=DATAMANAGE_API_PORT
)
DATAFLOW_API_URL = "http://{host}:{port}/v3/dataflow".format(host=DATAFLOW_API_HOST, port=DATAFLOW_API_PORT)
AUTH_API_URL = "http://{host}:{port}/v3/auth".format(host=AUTH_API_HOST, port=AUTH_API_PORT)
GCS_API_URL = "http://{host}:{port}/".format(host=GCS_API_HOST, port=GCS_API_PORT)
HUB_MANAGER_API_URL_DEFAULT = "http://{host}:{port}/v3/hubmanager".format(
    host=HUB_MANAGER_API_HOST, port=HUB_MANAGER_API_PORT
)
HUB_MANAGER_API_URL = getattr(dataapi_settings, "HUB_MANAGER_API_URL", HUB_MANAGER_API_URL_DEFAULT)
RESOURCE_CENTER_API_URL = "http://{host}:{port}/v3/resourcecenter".format(
    host=RESOURCE_CENTER_API_HOST, port=RESOURCE_CENTER_API_PORT
)
DATAQUERY_API_URL = "http://{host}:{port}/v3/queryengine".format(host=DATAQUERY_API_HOST, port=DATAQUERY_API_PORT)
JOBNAVI_API_URL = "http://{host}:{port}/v3/jobnavi".format(host=JOBNAVI_API_HOST, port=JOBNAVI_API_PORT)

APP_ID = dataapi_settings.APP_ID
APP_TOKEN = dataapi_settings.APP_TOKEN

SUCCESS_CODE = "1500200"

# 标识表storage_result_table中的记录是否被删除
STORAGE_RT_ACTIVE_TRUE = 1
STORAGE_RT_ACTIVE_FALSE = 0
# 是否启用当删除存储关联关系时同时删除存储中的数据
ENABLE_DELETE_STORAGE_DATA = False

INNER_TIME_FIELDS = [DTEVENTTIME, DTEVENTTIMESTAMP, THEDATE, LOCALTIME]

# 目前支持的PROCESSING_TYPE
PROCESSING_TYPE_LIST = [
    BATCH,
    STREAM,
    CLEAN,
    TRANSFORM,
    MODEL,
    STREAM_MODEL,
    BATCH_MODEL,
    STORAGE,
    VIEW,
    QUERYSET,
    SNAPSHOT,
]

MYSQL_TYPE_MAPPING = {
    "string": "text",
    "int": "int",
    "boolean": "tinyint",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "text": "text",
    "longtext": "longtext",
    "short": "int",
    "datetime": "datetime",
    "bigint": "bigint",
    "bigdecimal": "decimal",
}

# PROCESSING_TYPE_SAME中的元素，获取physical_table_name时，都转换为和stream的一致
PROCESSING_TYPE_SAME = [STREAM, CLEAN, TRANSFORM, MODEL, STORAGE, VIEW]
# 数据格式的映射关系
DATA_TYPE_MAPPING = {
    HDFS: {
        BATCH: PARQUET,
        STREAM: PARQUET,
        CLEAN: PARQUET,
        TRANSFORM: PARQUET,
        MODEL: PARQUET,
        STREAM_MODEL: PARQUET,
        BATCH_MODEL: PARQUET,
    }
}

DATABUS_CHANNEL_TYPE = [KAFKA, PULSAR]
TABLE_NAME_SPLIT_BY_POINT = [MYSQL, TSDB]

# tspider和MySQL生成建表语句时，忽略的字段
CREATE_TABLE_SQL_IGNORE_FIELDS = [TIMESTAMP, OFFSET, DTEVENTTIMESTAMP, THEDATE]


STORAGE_DELETE = [ES, MYSQL]

# 不对物理表名做+1处理，返回默认规则表名
STORAGE_EXCLUDE_TABLE_NAME_MODIFY = [KAFKA, QUEUE, TSDB, ES, PULSAR]

# 存储维护任务使用
MAINTAIN_TASK_QUEUE = "storekitapi"
ES_MAINTAIN_TIMEOUT = 2 * 60 * 60
HDFS_MAINTAIN_TIMEOUT = 12 * 60 * 60
DRUID_MAINTAIN_TIMEOUT = 8 * 60 * 60
CLICKHOUSE_MAINTAIN_TIMEOUT = 1 * 60 * 60
ICEBERG_MAINTAIN_TIMEOUT = 2 * 60 * 60
ICEBERG_COMPACT_TIMEOUT = 24 * 60 * 60
ICEBERG_COMPACT_PERIOD = "7d"
MYSQL_MAINTAIN_TIMEOUT = 8 * 60 * 60
HTTP_REQUEST_TIMEOUT = 60

# 不同版本名称
VERSION_ENTERPRISE_NAME = "ee"  # enterprise -> ee
VERSION_COMMUNITY_NAME = "ce"  # community -> ce
VERSION_IEOD_NAME = "tencent"  # ieod -> tencent
VERSION_TGDP_NAME = "tgdp"  # 海外版
SUPPORT_HIVE_VERSION = (
    dataapi_settings.SUPPORT_HIVE_VERSION
    if hasattr(dataapi_settings, "SUPPORT_HIVE_VERSION")
    else [VERSION_IEOD_NAME, VERSION_TGDP_NAME]
)
BKDATA_VERSION_TYPE = (
    dataapi_settings.BKDATA_VERSION_TYPE if hasattr(dataapi_settings, "BKDATA_VERSION_TYPE") else VERSION_IEOD_NAME
)

# 开源版本
STORAGE_QUERY_ORDER = (
    dataapi_settings.STORAGE_QUERY_ORDER
    if hasattr(dataapi_settings, "STORAGE_QUERY_ORDER")
    else {"mysql": 2, "tsdb": 4, "es": 6}
)
STORAGE_EXCLUDE_QUERY = (
    dataapi_settings.BKDATA_STORAGE_EXCLUDE_QUERY
    if hasattr(dataapi_settings, "BKDATA_STORAGE_EXCLUDE_QUERY")
    else ["queue", "kafka", "hdfs"]
)


# 支持的过期时间
EXPIRES_SUPPORT = [1, 3, 7, 14, 21, 30, 60, 90, 120, 180, 360, 720, 1080, 1800]

# storage default sql template
DEFAULT_SQL_TEMPLATE = """SELECT thedate, dtEventTime, dtEventTimeStamp, localTime, {rt_fields}
             FROM {rt_id_storage}
             WHERE thedate>='{thedate}' AND thedate<='{thedate}'
             ORDER BY dtEventTime DESC LIMIT 10"""

DEFAULT_DRUID_SQL_TEMPLATE = """SELECT thedate, dtEventTime, dtEventTimeStamp, {rt_fields}
             FROM {rt_id_storage}
             WHERE thedate>='{thedate}' AND thedate<='{thedate}'
             ORDER BY dtEventTime DESC LIMIT 10"""

DEFAULT_IGNITE_SQL_TEMPLATE = """SELECT dtEventTime, dtEventTimeStamp, localTime, {rt_fields}
             FROM {rt_id_storage}
             WHERE dtEventTimeStamp>='{dtEventTimeStamp}' AND dtEventTimeStamp<='{dtEventTimeStamp}'
             ORDER BY dtEventTimeStamp DESC LIMIT 10"""

# druid/tsdb sql template
DRUID_TSDB_SQL_TEMPLATE = """SELECT {rt_fields}, time
FROM {rt_id_storage}
WHERE time > '1h'
ORDER BY time DESC LIMIT 10"""

# es dsl template
ES_DSL_TEMPLATE = """{{\"body\": {{\"sort\": [{{\"dtEventTimeStamp\": {{\"order\": \"desc\"}}}}],
                     \"query\": {{\"bool\": {{\"filter\": [{{\"range\": {{\"dtEventTimeStamp\": {{\"gte\": {gte},
                     \"lte\": {lte}, \"format\": \"epoch_millis\"}}}}}}], \"must\": [{{\"query_string\":
                     {{\"query\": \"*\", \"analyze_wildcard\": true}}}}]}}}}, \"from\": 0, \"size\": 10}},
                     \"index\": \"{rt_id}_*\"}}"""

# query hdfs template
HDFS_SQL_TEMPLATE = """SELECT {rt_fields}, dtEventTime, dtEventTimeStamp, thedate, localTime
             FROM {rt_id_storage}
             WHERE thedate='{thedate}'
             LIMIT 10"""


DATALAB_HDFS_SQL_TEMPLATE = """SELECT {rt_fields}
             FROM {rt_id_storage}
             LIMIT 10"""

# 容量相关
MONITOR_TOPIC = "bkdata_monitor_metrics591"
STAT_DATABASE = "monitor_performance_metrics"

# gsc, 定时任务调用监控数据
CALL_MONITOR_DATABASE = "monitor_custom_metrics"
CALL_MONITOR_MEASUREMENT = "storage_call_metrics"
MAINTAIN_DELTA_DAY = 7
ICEBERG_COMPACT_DELTA_DAY = 3
MERGE_DAYS_DEFAULT = 1
CLEAN_DELTA_DAY = 7
CLEAN_BEGIN_DAY = 1460
EXECUTE_TIMEOUT = 60
DEFAULT_RECORD_NUM = 10

# iceberg入库时添加的字段
ICEBERG_ADDITIONAL_FIELDS = {
    ET: TIMESTAMP,
    DTEVENTTIME: STRING,
    DTEVENTTIMESTAMP: LONG,
    LOCALTIME: STRING,
    THEDATE: INT,
}
# iceberg表默认的分区方式，使用隐藏的时间字段按小时分区
ICEBERG_DEFAULT_PARTITION = [{FIELD: ET, METHOD: HOUR, ARGS: 0}]
# 使用不分区iceberg表的processing_type
PROCESSING_TYPES_NOT_ADDING_FIELDS = [QUERYSET, SNAPSHOT]
# iceberg默认配置
HIVE_METASTORE_SERVER = getattr(dataapi_settings, "HIVE_METASTORE_SERVER", {})
ICEBERG_DEFAULT_CONF = {
    "hive.metastore.warehouse.dir": "/data/iceberg/warehouse",
    "table.commit.retry.min-wait-ms": 100,
    "table.commit.retry.max-wait-ms": 5000,
    "table.commit.retry.total-timeout-ms": 600000,
    "table.commit.retry.num-retries": 2000,
    "table.preserve.snapshot.nums": 10,
    "table.preserve.snapshot.days": 3,
    "table.max.rewrite.files": 2000,
    "table.max.compact.file.size": 16777216,
    "interval": 300000,
    "flush.size": 1000000,
}

# hdfs 相关的配置项
HIVE_CREATE_DB = "CREATE DATABASE IF NOT EXISTS mapleleaf_{biz_id}"
HIVE_CREATE_TABLE_BASIC_SQL = "CREATE EXTERNAL TABLE IF NOT EXISTS mapleleaf_{biz_id}.{table_name} ({columns})"
# PARQUET/JSON格式使用不同的sql
HIVE_LAST_SQL = {
    PARQUET: " PARTITIONED BY (dt_par_unit int) STORED AS PARQUET LOCATION '{hdfs_url}{physical_table_name}/'",
    JSON: " PARTITIONED BY (dt_par_unit int) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
    " LOCATION '{hdfs_url}{physical_table_name}/'",
}

HIVE_LAST_SQL_NO_PART = {
    PARQUET: " STORED AS PARQUET LOCATION '{hdfs_url}{physical_table_name}/'",
    JSON: " ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' LOCATION '{hdfs_url}{physical_table_name}/'",
}

HIVE_ADD_PARTITION_SQL = "ALTER TABLE mapleleaf_{biz_id}.{table_name} ADD IF NOT EXISTS {partitions}"
HIVE_ONE_PARTITION_SQL = (
    "PARTITION(dt_par_unit={datehour}) LOCATION '{physical_table_name}/{year}/{month}/{day}/{hour}'"
)
HIVE_DROP_PARTITION_SQL = "ALTER TABLE mapleleaf_{biz_id}.{table_name} DROP IF EXISTS {partitions}"
HIVE_TABLE_EXISTS_SQL = "SHOW TABLES IN mapleleaf_{biz_id} LIKE '{table_name}'"
HIVE_DESC_TABLE_SQL = "DESC mapleleaf_{biz_id}.{table_name}"
HIVE_ADD_COLUMNS_SQL = "ALTER TABLE mapleleaf_{biz_id}.{table_name} ADD COLUMNS ({columns})"
HIVE_DROP_TABLE_SQL = "DROP TABLE IF EXISTS mapleleaf_{biz_id}.{table_name}"
HIVE_SHOW_TABLE_SQL = "SHOW CREATE TABLE mapleleaf_{biz_id}.{table_name}"
HIVE_SHOW_PARTITION_SQL = "SHOW PARTITIONS mapleleaf_{biz_id}.{table_name}"
EXCEPT_FIELDS = [TIMESTAMP, OFFSET, THEDATE, DTEVENTTIME.lower(), DTEVENTTIMESTAMP.lower(), LOCALTIME.lower()]
HDFS_DEFAULT_COLUMNS = [
    "`{}` {}".format(THEDATE, INT),
    "`{}` {}".format(DTEVENTTIME, STRING),
    "`{}` {}".format(DTEVENTTIMESTAMP, BIGINT),
    "`{}` {}".format(LOCALTIME, STRING),
]
# 默认存储的过期时间
DEFAULT_EXPIRE_DAYS = 7
# 连接hive server的配置信息
HIVE_SERVER_HOST = dataapi_settings.HIVE_SERVER_HOST if hasattr(dataapi_settings, "HIVE_SERVER_HOST") else LOCALHOST
HIVE_SERVER_PORT = dataapi_settings.HIVE_SERVER_PORT if hasattr(dataapi_settings, "HIVE_SERVER_PORT") else 10000
HIVE_SERVER_USER = dataapi_settings.HIVE_SERVER_USER if hasattr(dataapi_settings, "HIVE_SERVER_USER") else ROOT
HIVE_SERVER_PASS = dataapi_settings.HIVE_SERVER_PASS if hasattr(dataapi_settings, "HIVE_SERVER_PASS") else ""

# 连接hive server的配置信息
HIVE_SERVER = dataapi_settings.HIVE_SERVER if hasattr(dataapi_settings, "HIVE_SERVER") else {}
# webhdfs账户
WEBHDFS_USER = dataapi_settings.WEBHDFS_USER if hasattr(dataapi_settings, "WEBHDFS_USER") else ROOT
DAY_HOURS = [
    "00",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
]

# es 相关的配置项
SKIP_ES_INDEX_PREFIX = [
    ".",
    "kibana",
    "fta_relation",
    "fta_event",
    "pizza",
    "test_fta_relation",
    "searchguard",
    "tencentgamedbateam",
    "elastalert_status",
]
SKIP_RT_FIELDS = [TIMESTAMP, OFFSET]
AUTO_CREATE_FIELD = ["_iteration_idx", "gseindex", "ip", "path"]  # 清洗直接入库场景
ES_SIX_VERSION = "6."
EXCLUDE_ES_CLUSTER = ["es-paas", "es-datalog"]  # applog-2018.06.25, bkdata_algorithm_log-2018.07.05
REPLICA_CLUSTER = ["eslog-security", "eslog-sh-fifa"]
TAG_COLD = "cold"
TAG_HOT = "hot" if RUN_VERSION in [VERSION_IEOD_NAME, VERSION_TGDP_NAME] else TAG_COLD
NODE_HAS_TAG = dataapi_settings.NODE_HAS_TAG if hasattr(dataapi_settings, "NODE_HAS_TAG") else True
INITIAL_SHARD_NUM = 3
HOT_INDEX_SAVE_DAYS = 1
MAX_SHARD_NUM = 20
RESERVED_INDEX_NUM = 1

REPLICA_NUM = 1  # 副本数量
TOTAL_SHARDS_PER_NODE = 2  # 每个节点索引最大分片数

INDEX_SPLIT_THRESHOLD_IN_BYTE = 536870912000  # 500gb
INDEX_NOT_SPLIT_SIZE_IN_BYTE = 10737418240  # 10gb
INITIAL_SHARD_MAX_SIZE_IN_BYTE = 107374182400  # 100gb
FORCE_SPLIT_DAYS = 30  # 强制30天分裂一次es索引
DOCS_LIMIT_PER_SHARD = 10 * 10000 * 10000  # lucene 单分片docs限制为20亿,此处限制10亿条

HAS_COLD_NODES = dataapi_settings.HAS_COLD_NODES if hasattr(dataapi_settings, "HAS_COLD_NODES") else True

IGNITE_JOIN_SCENARIO_TYPE = "join"

IGNITE_IP_LIB_SCENARIO_TYPE = "iplib"

DATA_REGION = {IGNITE_JOIN_SCENARIO_TYPE: "join", IGNITE_IP_LIB_SCENARIO_TYPE: "iplib"}

IGNITE_CONN = "%s:%s;user=%s;password=%s;"

IGNITE_CREATE_TABLE_SQL_PREFIX = (
    "CREATE TABLE if NOT EXISTS {TABLE_NAME} (dtEventTime varchar(32) NOT NULL, "
    "dtEventTimeStamp bigint(20) NOT NULL, localTime varchar(32), "
)

IGNITE_CREATE_DATALAB_TABLE_SQL_PREFIX = "CREATE TABLE if NOT EXISTS {TABLE_NAME} ("


REPLICATED_TEMPLATE = "replicated"
REPLICATED_BACKUPS = "0"

PARTITION_TEMPLATE = "partitioned"
PARTITION_BACKUPS = "2"

# 目前只有iplib为复制模式，暂时不考虑过期
PAR_EXPIRY_TEMP = {3: "3d_partitioned", 7: "7d_partitioned", 30: "30d_partitioned"}

IGNITE_CREATE_TABLE_SQL_SUFFIX = (
    'PRIMARY KEY ({PRIMARY_KEYS})) WITH "template={TEMPLATE}, CACHE_NAME={TABLE_NAME}, '
    "DATA_REGION={DATA_REGION}, BACKUPS={BACKUPS}, KEY_TYPE=java.lang.String, "
    'VALUE_TYPE={TABLE_NAME}"'
)

IGNITE_CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS {TABLE_NAME}_idx ON {TABLE_NAME} ({FIELDS})"

IGNITE_ALTER_TABLE_SQL = "ALTER TABLE {TABLE_SCHEMA}.{TABLE_NAME} ADD COLUMN ({FIELDS})"

IGNITE_SHOW_COLUMN_SQL = (
    "SELECT table_schema, table_name, column_name, type_name  FROM INFORMATION_SCHEMA.COLUMNS "
    "where table_schema='{TABLE_SCHEMA}' and table_name='{TABLE_NAME}';"
)

COLUMN_FIELDS = ["table_schema", "table_name", "column_name", "type_name"]

IGNITE_SHOW_TABLE_SQL = (
    "SELECT table_schema, table_name FROM INFORMATION_SCHEMA.TABLES "
    "where table_schema='{TABLE_SCHEMA}' and table_name='{TABLE_NAME}'"
)

TABLE_FIELDS = ["table_schema", "table_name"]

IGNITE_SYSTEM_FIELDS = ["_key", "_val", "_ver"]


# 敏感信息变量
# ES相关
RTX_RECEIVER = dataapi_settings.RTX_RECEIVER if hasattr(dataapi_settings, "RTX_RECEIVER") else ""

RT_TYPE_TO_ICEBERG_MAPPING = {
    LONG: LONG,
    INT: INTEGER,
    STRING: STRING,
    DOUBLE: DOUBLE,
    TEXT: STRING,
    TIMESTAMP: TIMESTAMP,
}

# clickhouse相关
RT_TYPE_TO_CLICKHOUSE_MAPPING = {
    LONG: "Int64",
    INT: "Int32",
    STRING: "String",
    DOUBLE: "Float64",
    TEXT: "String",
    TIMESTAMP: "DateTime",
}
ClICKHOUSE_EXCEPT_FIELDS = [
    PARTITION_TIME,
    DTEVENTTIME.lower(),
    DTEVENTTIMESTAMP.lower(),
    THEDATE.lower(),
    LOCALTIME.lower(),
    TIMESTAMP,
]
PARTITION_COLUMN = "__time DateTime"
ClICKHOUSE_DEFAULT_COLUMNS = [
    PARTITION_COLUMN,
    "{} {}".format(DTEVENTTIME.lower(), "String"),
    "{} {}".format(DTEVENTTIMESTAMP.lower(), "Int64"),
    "{} {}".format(THEDATE.lower(), "Int32"),
    "{} {}".format(LOCALTIME.lower(), "String"),
]
QUERY_CLUSTER_SQL = "select host_address, port from system.clusters"
QUERY_CAPACITY_SQL = (
    "select formatReadableSize(sum(bytes)) from system.parts where database = '{}' and table = '{}' and active"
)
FORMAT_READABLE_SQL = "select formatReadableSize({})"
QUERY_CAPACITY_BYTES_SQL = "select sum(bytes) from system.parts where database = '{}' and table = '{}' and active"
QUERY_ALL_CAPACITY_SQL = (
    "select database, name, total_bytes, total_rows from system.tables where "
    "name like '%_local' order by total_bytes, total_rows"
)
QUERY_CLUSTER_CAPACITY_SQL = "select free_space, total_space from system.disks"
QUERY_PROCESSLIST_SQL = (
    "select query_id, query, elapsed from system.processes where is_initial_query = 1 order by elapsed  limit 10 ;"
)
QUERY_DISTINCT_PARTITIONS_SQL = (
    "select count(distinct partition) from system.parts where database = '{}' and table = '{}' and active"
)
QUERY_TOTAL_PARTITIONS_SQL = (
    "select count(partition) from system.parts where database = '{}' and table = '{}' and active"
)
QUERY_TOP_PARTITIONS_SQL = (
    "select partition, count(partition) as count, database, table from system.parts "
    "where database = '{}' and table = '{}' and active group by database, table, partition "
    "order by count desc limit 10"
)
QUERY_CLUSTER_TOP_PARTITIONS_SQL = (
    "select partition, count(partition) as count, database, table from system.parts "
    "where active = 1 group by database, table, partition order by count desc limit 20"
)
CREATE_DB_SQL = "create database if not exists {} on cluster {}"
CREATE_REPLICATED_TABLE_SQL = (
    "create table if not exists {}.{} on cluster {} ({}) "
    "engine = ReplicatedMergeTree('/clickhouse/tables/{{layer}}-{{shard}}/{}.{}', '{{replica}}') {} {} {} {}"
)
CREATE_DISTRIBUTED_TABLE_SQL = (
    "create table if not exists {}.{} on cluster {} ({}) engine = Distributed('{}', '{}', '{}', rand())"
)
CHECK_TABLE_EXIST_SQL = "select name from system.tables where database = '{}' and name = '{}' or name = '{}' "
DROP_TABLE_SQL = "drop table if exists {}.{} on cluster {}"
TRUNCATE_TABLE_SQL = "truncate table if exists {}.{} on cluster {}"
ALTER_INDEX_SQL = "alter table {}.{} on cluster {} add index {} {} TYPE {} GRANULARITY {}"
DROP_INDEX_SQL = "alter table {}.{} on cluster {} drop index {}"

QUERY_FIELDS_SQL = "select name, type from system.columns where database = '{}' and table = '{}' order by position asc"
SHOW_SCHEMA_SQL = "show create table {}.{}"
QUERY_SAMPLE_SQL = "select * from {}.{} limit 10"
QUERY_COUNT_SQL = "select count(*) from {}.{}"
ORDER_BY_SQL = "order by ({})"
SAMPLE_BY_SQL = "sample by __time"
PARTITION_BY_SQL = "partition by toStartOfHour(__time)"
TTL_BY_SQL = "TTL __time + toIntervalDay({})"
ALTER_TABLE_SQL = "alter table {}.{} on cluster {} {}"
ADD_COLUMN_SQL = "add column if not exists {}"
DROP_COLUMN_SQL = "drop column if exists {}"
ALTER_TTL_SQL = "alter table {}.{} on cluster {} MODIFY TTL __time + toIntervalDay({})"
ENGINE_FULL_SQL = "select engine_full from system.tables where database = '{}' and name = '{}'"

CONN_POOL = {}
CREATE_TABLE_BASIC_SQL = (
    "create table if not exists `<TABLE_NAME>` (`thedate` int(11) NOT NULL, `dtEventTime` varchar(32) NOT NULL, "
    "`dtEventTimeStamp` bigint(20) NOT NULL, `localTime` varchar(32) DEFAULT NULL, "
)

CREATE_TABLE_LAST_SQL = (
    "KEY ind_dtEventTimeStamp (dtEventTimeStamp)) DEFAULT CHARSET=utf8mb4 COMMENT='shard_key \"dtEventTimeStamp\"' "
)

SHOW_TABLE_SQL = "SHOW CREATE TABLE %s"
SAMPLE_DATA_SQL = "SELECT * FROM %s ORDER BY dtEventTimeStamp DESC LIMIT 10"

# mysql相关配置
MYQL_PARTITION = " PARTITION BY RANGE (`thedate`) ("
MYSQL_TYPE_MAPPING = {
    STRING: TEXT,
    INT: INT,
    BOOLEAN: TINYINT,
    LONG: BIGINT,
    FLOAT: FLOAT,
    DOUBLE: DOUBLE,
    TEXT: TEXT,
    DATETIME: DATETIME,
}
# 通过测试 ignite varchar 可以存放 65535 长度
IGNITE_TYPE_MAPPING = {
    STRING: VARCHAR,
    INT: INTEGER,
    BOOLEAN: BOOLEAN,
    LONG: BIGINT,
    FLOAT: REAL,
    DOUBLE: DOUBLE,
    TEXT: VARCHAR,
}

MYSQL_TYPE_META = {BIGINT: "1_12", INT: "1_11", DOUBLE: "2_22", FLOAT: "2_21", LONGTEXT: "3_32", TEXT: "3_31"}
PARTITION_DAYS = 7
MAX_PARTITION = "30000101"  # 假定3000-01-01作为max的分区值
REORGANIZE_PARTITION = (
    "ALTER TABLE %s REORGANIZE PARTITION `p%s` INTO (PARTITION `p%s` VALUES LESS THAN (%s), "
    "PARTITION `p%s` VALUES LESS THAN MAXVALUE)"
)
ADD_PARTITION = (
    "ALTER TABLE %s ADD PARTITION (PARTITION `p%s` VALUES LESS THAN (%s), PARTITION `p%s` VALUES LESS THAN MAXVALUE)"
)
DROP_PARTITION = "ALTER TABLE %s DROP PARTITION %s"

DISABLE_TAG = "disable"

MAX_RETRIES = 10


# 存储建议使用场景
STORAGE_SCENARIOS = {
    MYSQL: _("建议使用场景：\n\t关系型数据库及关联分析\n\t建议日数据量[单表]：\n\tGB/千万级以内\n\t查询模式：\n\t点查询/关联查询/聚合查询"),
    ES: _("建议使用场景：\n\t数据分析及日志检索\n\t建议日数据量[单表]：\n\tGB/TB级\n\t查询模式：\n\t检索查询【支持分词】"),
    HDFS: _("建议使用场景：\n\t海量数据离线分析，对查询时延要求不高\n\t建议日数据量[单表]：\n\tTB/PB\n\t查询默认：\n\t可对接离线分析/即席查询/数据分析等方式"),
    DRUID: _("建议使用场景：\n\t时序数据分析\n\t建议日数据量[单表]：\n\tGB/千万级以上\n\t查询模式：\n\t明细查询/聚合查询"),
    TSDB: _("建议使用场景：\n\t监控场景\n\t建议日数据量[单表]：\n\tGB\n\t查询模式：\n\t明细查询/聚合查询"),
    QUEUE: _("建议使用场景：\n\t数据订阅\n\t建议日数据量[单表]：\n\tGB/TB\n\t查询模式：\n\t客户端连接消费"),
    QUEUE_PULSAR: _("建议使用场景：\n\t数据订阅\n\t建议日数据量[单表]：\n\tGB/TB\n\t查询模式：\n\t客户端连接消费"),
    IGNITE: _("建议使用场景：\n\t静态关联；数据共享；\n\t建议日数据量[单表]：\n\tGB\n\t查询模式：\n\t点查询/关联查询/聚合查询"),
    ICEBERG: _("建议使用场景：\n\t海量结构化或者非结构化数据，可用于离线分析等；\n\tt建议日数据量[单表]：\n\tTB/PB\n\t查询默认：\n\t可对接离线分析/数据分析等方式"),
    CLICKHOUSE: _("建议使用场景：\n\t分析型数据库，实时数仓，适合宽表，大数据量和复杂查询场景，建议申请专用ClickHouse 集群"),
}


# 存储组件规范名称
FORMAL_NAME = {
    MYSQL: "MySQL",
    ES: "Elasticsearch",
    HDFS: "HDFS",
    DRUID: "Druid",
    TSDB: "TSDB",
    QUEUE: "Queue",
    IGNITE: "Ignite",
    POSTGRESQL: "PostgreSQL",
    QUEUE_PULSAR: "QueuePulsar",
    ICEBERG: "Iceberg",
    CLICKHOUSE: "ClickHouse",
}


# admin注册存储集群时的说明
STORAGE_ADMIN = {
    TSDB: [
        {
            "host": {"tips": _("tsdb proxy连接域名"), "check": _("不能为空，长度小于50")},
            "port": {"tips": _("tsdb proxy连接端口"), "check": _("不能为空，取值范围1~65530"), "example": 7085},
            "user": {"tips": _("tsdb proxy连接用户名"), "check": _("可以为空，长度小于30")},
            "password": {"tips": _("tsdb proxy连接密码"), "check": _("可以为空，长度小于30")},
            "user_backend": {"tips": _("tsdb node连接用户名"), "check": _("可以为空，长度小于30")},
            "password_backend": {"tips": _("tsdb node连接密码"), "check": _("可以为空，长度小于30")},
            "etcd": {
                "tips": _("etcd集群信息"),
                "check": _("不能为空，长度小于100"),
                "example": _('"etcd": ["(\'x.x.x.x\', 2379)"])'),
            },
        }
    ],
    MYSQL: [
        {
            "host": {"tips": _("MySQL连接域名"), "check": _("不能为空，长度小于50")},
            "port": {"tips": _("MySQL连接端口"), "check": _("不能为空，取值范围1~65530"), "example": 3306},
            "user": {"tips": _("MySQL连接用户名"), "check": _("不能为空，长度小于30")},
            "password": {"tips": _("MySQL连接密码"), "check": _("可以为空，长度小于30")},
        }
    ],
    ES: [
        {
            "host": {"tips": _("ES集群连接域名"), "check": _("不能为空，长度小于50")},
            "port": {"tips": _("ES集群连接http端口"), "check": _("不能为空，取值范围1~65530"), "example": 8080},
            "transport": {"tips": _("ES集群连接tcp端口"), "check": _("不能为空，取值范围1~65530"), "example": 9300},
            "enable_auth": {"tips": _("ES集群是否开启searchguard认证"), "check": _("不能为空，只能取值True或False")},
            "user": {"tips": _("ES集群连接用户名"), "check": _("可以为空，长度小于30")},
            "password": {"tips": _("ES集群连接密码"), "check": _("可以为空，长度小于30")},
        }
    ],
}

PAGE_SIZE_DEFAULT = 1000
PAGE_DEFAULT = 1

K_STORAGE = {STORAGE_CLUSTER: "id", CHANNEL_CLUSTER: "id", RESULT_TABLE: "result_table_id"}
# Druid 版本

DRUID_VERSION_V1 = "0.11"
DRUID_VERSION_V2 = "0.16"

# Druid注册在Zookeeper中的路径
ZK_DRUID_PATH = "/druid/internal-discovery"
ZK_OVERLORD_PATH = "%s/overlord" % (ZK_DRUID_PATH)
OVERLORD = "overlord"
COORDINATOR = "coordinator"
ZK_COORDINATOR_PATH = "%s/coordinator" % (ZK_DRUID_PATH)

# druid任务配置
DEFAULT_TASK_MEMORY = 1024
DEFAULT_SEGMENT_GRANULARITY = "FIVE_MINUTE"
DEFAULT_TIMESTAMP_COLUMN = "__druid_reserved_ingestion_time"
DEFAULT_MAX_IDLE_TIME = 600000
DEFAULT_WINDOW_PERIOD = "PT3M"
INT_MAX_VALUE = 2 ** 31 - 1
MERGE_BYTES_LIMIT = 1000000000

DEFAULT_DRUID_EXPIRES = "360d"

ENDPOINT_RUN_TASK = "/druid/indexer/v1/task"
ENDPOINT_SHUTDOWN_TASK = "/druid/worker/v1/chat/receiver/shutdown"
ENDPOINT_GET_RUNNING_WORKERS = "/druid/indexer/v1/workers"
ENDPOINT_GET_RUNNING_TASKS = "/druid/indexer/v1/runningTasks"
ENDPOINT_GET_PENDING_TASKS = "/druid/indexer/v1/pendingTasks"
ENDPOINT_PUSH_EVENTS = "/druid/worker/v1/chat/receiver/push-events"
ENDPOINT_GET_DATASOURCES = "/druid/coordinator/v1/metadata/datasources"
ENDPOINT_GET_ALL_DATASOURCES = "/druid/coordinator/v1/metadata/datasources?includeUnused"
ENDPOINT_DATASOURCE_RULE = "/druid/coordinator/v1/rules"
ENDPOINT_HISTORICAL_SIZES = "/druid/coordinator/v1/servers?simple"
ENDPOINT_GET_OVERLORD_LEADER = "/druid/indexer/v1/leader"
ENDPOINT_GET_COORDINATOR_LEADER = "/druid/coordinator/v1/leader"

TASK_TYPE_RUNNING = "running"
TASK_TYPE_PENDING = "pending"
TASK_CONFIG_TEMPLATE = """
{{
  "type": "index_realtime",
  "resource": {{
    "availabilityGroup": "{availability_group}",
    "requiredCapacity": {required_capacity}
  }},
  "spec": {{
    "dataSchema": {{
      "dataSource": "{data_source}",
      "metricsSpec": {metrics_spec},
      "granularitySpec": {{
        "segmentGranularity": "{segment_granularity}",
        "queryGranularity": "none",
        "rollup": false
      }},
      "parser": {{
        "type": "map",
        "parseSpec": {{
          "format": "json",
          "timestampSpec": {{
            "column": "{timestamp_column}",
            "format": "millis",
            "missingValue": null
          }},
          "dimensionsSpec": {{
            "dimensions": {dimensions_spec},
            "dimensionExclusions": {dimension_exclusions}
          }}
        }}
      }}
    }},
    "ioConfig": {{
      "type": "realtime",
      "firehose": {{
        "type": "receiver",
        "serviceName": "receiver",
        "bufferSize": 50000,
        "maxIdleTime": {max_idle_time}
      }}
    }},
    "tuningConfig": {{
      "type": "realtime",
      "maxRowsInMemory": 50000,
      "intermediatePersistPeriod": "PT1M",
      "windowPeriod": "{window_period}",
      "rejectionPolicy": {{
        "type": "serverTime"
      }},
      "shardSpec": {{
        "type": "linear",
        "partitionNum": "{partition_num}"
      }}
    }}
  }},
  "context": {context}
}}
"""

# Druid默认数据过期时间规则
DEFAULT_EXPIRES_RULE = [
    {"period": "P720D", "tieredReplicants": {"tier_hot": 2}, "type": "loadByPeriod"},
    {"type": "dropForever"},
]

DJANGO_BATCH_CREATE_OBJS = 1000

UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
UTC_BEGIN_TIME = "1970-01-01T00:00:00.000Z"
TIME_ZONE_DIFF = 8
DEFAULT_TIME_ZONE = "UTC+8"

EXPIRES_ZH_DAY_UNIT = "天"
EXPIRES_ZH_FOREVER = "永久保存"
IGNITE_DEFAULT_MAX_RECORDS = 10000000

DRUID_MERGE_SEGMENTS_TASK_CONFIG_TEMPLATE = """
{
  "type": "index_parallel",
  "resource": {
    "requiredCapacity": 1
  },
  "spec": {
    "dataSchema": {
      "dataSource": "{{ datasource }}",
      "parser": {
        "type": "string",
        "parseSpec": {
          "format": "timeAndDims",
          "timestampSpec": {
            "column": "__time",
            "format": "auto"
          }
        }
      },
      "metricsSpec": [
        {
          "type": "longSum",
          "name": "__druid_reserved_count",
          "fieldName": "__druid_reserved_count",
          "expression": null
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": {
          "type": "none"
        },
        "rollup": false,
        "intervals": null
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "firehose": {
        "type": "ingestSegment",
        "dataSource": "{{ datasource }}",
        "interval": "{{ interval }}",
        "segments": null,
        "filter": null,
        "dimensions": null,
        "metrics": null,
        "maxInputSegmentBytesPerTask": 157286400
      },
      "appendToExisting": false
    },
    "tuningConfig": {
      "type": "index_parallel",
      "maxRowsPerSegment": null,
      "maxRowsInMemory": 1000000,
      "maxBytesInMemory": 0,
      "maxTotalRows": null,
      "numShards": null,
      "partitionsSpec": null,
      "indexSpec": {
        "bitmap": {
          "type": "concise"
        },
        "dimensionCompression": "lz4",
        "metricCompression": "lz4",
        "longEncoding": "longs"
      },
      "indexSpecForIntermediatePersists": {
        "bitmap": {
          "type": "concise"
        },
        "dimensionCompression": "lz4",
        "metricCompression": "lz4",
        "longEncoding": "longs"
      },
      "maxPendingPersists": 0,
      "forceGuaranteedRollup": false,
      "reportParseExceptions": false,
      "pushTimeout": 0,
      "segmentWriteOutMediumFactory": null,
      "maxNumConcurrentSubTasks": 1,
      "maxRetry": 3,
      "taskStatusCheckPeriodMs": 1000,
      "chatHandlerTimeout": "PT10S",
      "chatHandlerNumRetries": 5,
      "maxNumSegmentsToMerge": 100,
      "totalNumMergeTasks": 10,
      "logParseExceptions": false,
      "maxParseExceptions": 2147483647,
      "maxSavedParseExceptions": 0,
      "buildV9Directly": true,
      "partitionDimensions": []
    }
  },
  "context": {
    "forceTimeChunkLock": true,
    "priority": 25
  },
  "dataSource": "{{ datasource }}"
}
"""

DRUID_CLEAN_DEEPSTORAGE_TASK_CONFIG_TEMPLATE = """
{
  "type": "kill",
  "dataSource": "{{ datasource }}",
  "interval": "{{ interval }}",
  "context": {
    "forceTimeChunkLock": true,
    "priority": 25
  },
  "resource": {
    "requiredCapacity": 1
  }
}
"""

DRUID_COMPACT_SEGMENTS_TASK_CONFIG_TEMPLATE = """
{
  "type": "compact",
  "dataSource": "{{ datasource }}",
  "interval": "{{ interval }}",
  "segmentGranularity": "DAY",
  "tuningConfig" : {
    "type" : "index",
    "maxRowsPerSegment" : 5000000,
    "maxRowsInMemory" : 25000
  },
  "context": {
    "forceTimeChunkLock": true,
    "priority": 25
  }
}
"""
SUPPORT_STORAGE_TYPE = (
    dataapi_settings.SUPPORT_STORAGE_TYPE
    if hasattr(dataapi_settings, "SUPPORT_STORAGE_TYPE")
    else ["es", "mysql", "hdfs", "tsdb"]
)

FOREVER_START_DIFF_DAY = -3600
DEF_END_DIFF_DAY = -1

START_BATCH_TASK = "start"
STOP_BATCH_TASK = "stop"
DEF_KEY_SEPARATOR = ":"

FUSING_THRESHOLD = 200

CK_NODE_ZK_PATH_FORMAT = "/bk/clickhouse/%s/nodes/%s"
CK_TB_ZK_PATH_FORMAT = "/bk/clickhouse/%s/tables/%s"

CRONTAB_ES_MAINTAIN = (
    dataapi_settings.BKDATA_STORAGE_CRONTAB_ES_MAINTAIN
    if hasattr(dataapi_settings, "BKDATA_STORAGE_CRONTAB_ES_MAINTAIN")
    else {MINUTE: "5,35", HOUR: "*"}
)

CRONTAB_DRUID_MAINTAIN = (
    dataapi_settings.BKDATA_STORAGE_CRONTAB_DRUID_MAINTAIN
    if hasattr(dataapi_settings, "BKDATA_STORAGE_CRONTAB_DRUID_MAINTAIN")
    else {MINUTE: "30", HOUR: "9"}
)

CRONTAB_CLICKHOUSE_MAINTAIN = (
    dataapi_settings.BKDATA_STORAGE_CRONTAB_CLICKHOUSE_MAINTAIN
    if hasattr(dataapi_settings, "BKDATA_STORAGE_CRONTAB_CLICKHOUSE_MAINTAIN")
    else {MINUTE: "30", HOUR: "8"}
)

CRONTAB_HDFS_MAINTAIN = (
    dataapi_settings.BKDATA_STORAGE_CRONTAB_HDFS_MAINTAIN
    if hasattr(dataapi_settings, "BKDATA_STORAGE_CRONTAB_HDFS_MAINTAIN")
    else {MINUTE: "15", HOUR: "6"}
)

CRONTAB_MYSQL_MAINTAIN = (
    dataapi_settings.BKDATA_STORAGE_CRONTAB_MYSQL_MAINTAIN
    if hasattr(dataapi_settings, "BKDATA_STORAGE_CRONTAB_MYSQL_MAINTAIN")
    else {MINUTE: "30", HOUR: "7"}
)

EXTEND_VER = "tencent"
OPEN_VER = "ee"
EXTEND_DIR = "extend"

METRIC_REPORT_BATCH = 1000

ALARM_TYPE_PHONE = "phone"
ALARM_TYPE_MAIL = "mail"
