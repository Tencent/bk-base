/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.datahub.hubmgr;

public class MgrConsts {

    // 一些常量定义
    public static final String REST_SERVICE = "RestService";
    public static final String CRON_SERVICE = "CronService";
    public static final String CLUSTER_STAT_HANDLER = "ClusterStatHandler";
    public static final String ERROR_LOG_HANDLER = "ErrorLogHandler";
    public static final String EVENT_HANDLER = "EventHandler";
    public static final String CONNECTOR_STAT_HANDLER = "ConnectorStatHandler";

    public static final String HTTP_PROTOCOL = "http://";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;
    public static final String DATABUSMGR_PATH_DEFAULT = "hubmanager";
    public static final String DEFAULT_REST_PACKAGE = "com.tencent.bk.base.datahub.hubmgr.rest";

    public static final String JOB_GROUP_DEFAULT = "databusmgr_job";
    public static final String TRIGGER_GROUP_DEFAULT = "databusmgr_trigger";

    public static final String CONNECTOR_MANAGER = "connector-manager";

    // property keys
    public static final String SERVER_HTTP_PORT = "databusmgr.server.http.port";
    public static final String SERVER_HTTP_HOST = "databusmgr.server.http.host";
    public static final String SERVER_URL_PATH = "databusmgr.server.url.path";
    public static final String SERVER_REST_PACKAGES = "databusmgr.server.rest.packages";
    public static final String DATABUSMGR_SERVICES = "databusmgr.services";
    public static final String DATABUSMGR_ZK_ADDR = "databusmgr.zk.addr";
    public static final String DATABUSMGR_CRON_JOBS = "databusmgr.cron.jobs";
    public static final String DATABUSMGR_CRON_TRIGGERS = "databusmgr.cron.triggers";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String DATABUSMGR_JOB_REASSIGN_LIMIT = "databusmgr.job.reassign.limit";
    public static final int DATABUSMGR_JOB_REASSIGN_LIMIT_DEFAULT = 1200;
    public static final String DATABUSMGR_JOB_REASSIGN_MODULE = "databusmgr.job.reassign.module";
    public static final String DATABUSMGR_JOB_REASSIGN_MODULE_DEFAULT = "shipper,clean";
    public static final String DATABUSMGR_JOB_REASSIGN_COMPONENT = "databusmgr.job.reassign.component";
    public static final String DATABUSMGR_JOB_REASSIGN_COMPONENT_DEFAULT = "es,eslog";
    public static final String DATABUSMGR_JOB_REASSIGN_STORE_MAPPING = "databusmgr.job.reassign.%s.store";
    public static final String DATABUSMGR_JOB_REASSIGN_THREAD_NUM = "databusmgr.job.reassign.thread.num";
    public static final String DATABUSMGR_JOB_REASSIGN_SKIP_CLUSTERS = "databusmgr.job.reassign.skip.clusters";

    public static final String MONITOR_METRIC_DB = "monitor.metric.dbname";
    public static final String MONITOR_METRIC_DB_DEFAULT = "monitor_data_metrics";

    public static final String API_CLUSTER_STATUS_CHECK = "api.cluster.status.check";
    public static final String API_CLUSTER_STATUS_CHECK_DEFAULT = "/v3/databus/clusters/check_all/";

    public static final String API_MIGRATION_LIST = "api.migration.list";
    public static final String API_MIGRATION_LIST_DEFAULT = "/v3/databus/migrations/";

    public static final String API_MIGRATION_TASK_UPDATE = "api.migration.task.update";
    public static final String API_MIGRATION_TASK_UPDATE_DEFAULT = "/v3/databus/migrations/update_task_status/";

    public static final String API_MIGRATION_TASK_START = "api.migration.task.start";
    public static final String API_MIGRATION_TASK_START_DEFAULT = "/v3/databus/migrations/#taskId#/start/";

    public static final String API_MIGRATION_TASK_STOP = "api.migration.task.stop";
    public static final String API_MIGRATION_TASK_STOP_DEFAULT = "/v3/databus/migrations/#taskId#/stop/";

    public static final String API_MIGRATION_TASK_STATUS = "api.migration.task.status";
    public static final String API_MIGRATION_TASK_STATUS_DEFAULT = "/v3/databus/migrations/#taskId#/status/";

    public static final String API_MIGRATION_OFFSET = "api.migration.offset";
    public static final String API_MIGRATION_OFFSET_DEFAULT = "/v3/databus/migrations/set_offset/";

    public static final String API_TASK_LIST = "api.task.list";
    public static final String API_TASK_LIST_DEFAULT = "/v3/databus/tasks/";

    public static final String API_QUERY_METRIC = "api.query.metric";
    public static final String API_QUERY_METRIC_DEFAULT = "/v3/datamanage/dmonitor/metrics/query/";

    public static final String TASK_FAIL_MAX = "task.fail.max";
    public static final int TASK_FAIL_MAX_DEFAULT = 100;

    public static final String API_CHANNEL = "api.channel";
    public static final String API_CHANNEL_DEFAULT = "/v3/databus/channels/";

    public static final String API_RESTART_CONNECTOR = "api.restart.connector";
    public static final String API_RESTART_CONNECTOR_DEFAULT =
            "/v3/databus/clusters/CLUSTER/connectors/CONNECTOR/restart/";

    public static final String API_START_CONNECTOR = "api.start.connector";
    public static final String API_START_CONNECTOR_DEFAULT = "/v3/databus/tasks/";

    public static final String API_START_DATANODE_CONNECTOR = "api.start.datanode.connector";
    public static final String API_START_DATANODE_CONNECTOR_DEFAULT = "/v3/databus/tasks/datanode/";

    public static final String API_QUERY_LHOTSE_TASK_LIST = "api.query.lhotse.task.list";
    public static final String API_QUERY_LHOTSE_TASK_LIST_DEFAULT = "/v3/databus/tdw_shipper/";

    public static final String API_UPDATE_LHOTSE_TASK = "api.update.lhotse.task";
    public static final String API_UPDATE_LHOTSE_TASK_DEFAULT = "/v3/databus/tdw_shipper/update_task/";

    public static final String API_RT_STORAGES = "api.rt.storages";
    public static final String API_RT_STORAGES_DEFAULT = "/v3/storekit/result_tables/";

    public static final String API_ALL_HDFS_CONFIG_PATH = "api.all.hdfs.config.path";
    public static final String API_ALL_HDFS_CONFIG_PATH_DEFAULT = "/v3/storekit/hdfs/all_clusters_conf/";

    public static final String API_META_RT_INFO = "api.meta.rt";
    public static final String API_META_RT_INFO_DEFAULT = "/v3/meta/result_tables/";

    public static final String PROCESSING_TYPE = "processing_type";
    public static final String HDFS = "hdfs";
    public static final String STORAGES = "storages";
    public static final String STORAGE_CONFIG = "storage_config";
    public static final String EXPIRES = "expires";
    public static final String SNAPSHOT = "snapshot";
    public static final String QUERYSET = "queryset";

    public static final String LHOTSE_DNS = "lhotse.dns";
    public static final String LHOTSE_USER = "lhotse.auth.user";
    public static final String LHOTSE_KEY = "lhotse.auth.key";

    public static final String API_LHOTSE_TASK = "api.lhotse.task";
    public static final String API_LHOTSE_TASK_DEFAULT = "/Uscheduler/ExternDrive";
    public static final String LHOTSE_TASK_MAX_WAIT_TIME = "lhotse.task.max.wait.time";

    public static final String DATABUSMGR_OFFSET_COMMIT_TOPIC = "databusmgr.offset.committed.topic";
    public static final String DATABUSMGR_OFFSET_COMMIT_TOPIC_DEFAULT = "offset_committed_event";
    public static final String DATABUSMGR_EVENT_TOPIC = "databusmgr.event.topic";
    public static final String DATABUSMGR_EVENT_TOPIC_DEFAULT = "databus_event";
    public static final String DATABUSMGR_ERROR_LOG_TOPIC = "databusmgr.error.log.topic";
    public static final String DATABUSMGR_ERROR_LOG_TOPIC_DEFAULT = "databus_error_log";
    public static final String DATABUSMGR_CLUSTER_STAT_TOPIC = "databusmgr.cluster.stat.topic";
    public static final String DATABUSMGR_CLUSTER_STAT_TOPIC_DEFAULT = "databus_cluster_stat";
    public static final String DATABUSMGR_CONNECTOR_STAT_TOPIC = "databusmgr.connector.stat.topic";
    public static final String DATABUSMGR_CONNECTOR_STAT_TOPIC_DEFAULT = "databus_connector_stat";
    public static final String DATABUSMGR_CONNECTOR_REPORT_TOPIC = "databusmgr.connector.report.topic";
    public static final String DATABUSMGR_CONNECTOR_REPORT_TOPIC_DEFAULT = "connector_unassigned591";
    public static final String DAILY_COUNT_RT_DEFAULT = "daily_count_rt";
    public static final String DAILY_COUNT_BIZ_DEFAULT = "daily_count_biz";
    public static final String DAILY_COUNT_PROJECT_DEFAULT = "daily_count_project";
    public static final String DAILY_COUNT_COMPONENT_DEFAULT = "daily_count_component";
    public static final String DAILY_STAT_DELTA_DAYS = "daily.stat.delta.days";
    public static final String ICEBERG_EVENT_TOPIC = "iceberg.event.topic";
    public static final String ICEBERG_EVENT_TOPIC_DEFAULT = "iceberg_event";
    public static final String ICEBERG_TABLE_STAT = "iceberg_table_stat";

    public static final String DATABUS_ADMIN = "databusmgr.databus.admin";
    public static final String STOREKIT_ADMIN = "databusmgr.storekit.admin";
    public static final String STREAM_ADMIN = "databusmgr.stream.admin";
    public static final String BATCH_ADMIN = "databusmgr.batch.admin";
    public static final String ES_ADMIN = "databusmgr.es.admin";
    public static final String DRUID_ADMIN = "databusmgr.druid.admin";
    public static final String ALARM_TAG = "databusmgr.alarm.tag";
    public static final String ALARM_SERVICES_LIST = "databusmgr.alert.enable.services";
    public static final String MINOR_ALARM_TYPE = "databusmgr.minor.alarm.template";
    public static final String ORDINARY_ALARM_TYPE = "databusmgr.ordinary.alarm.template";
    public static final String FATAL_ALARM_TYPE = "databusmgr.fatal.alarm.template";
    public static final String DEFAULT_MINOR_ALARM_TYPE = "email";
    public static final String DEFAULT_ORDINARY_ALARM_TYPE = "wechat";
    public static final String DEFAULT_FATAL_ALARM_TYPE = "wechat,phone";
    public static final String DEFAULT_ALARM_SERVICES_LIST = "ClickHouseRoutePolicy,ClusterCapacityCheck,"
            + "ClusterStatusCheck,CollectRtDailyCount,LhotseTaskStatusCheck,MigrationScheduler,JobAspect,"
            + "ReassignConnectorCluster,EventHandler,KafkaOffsetHandler,KafkaOffsetWorker";

    public static final String KAFKA_SASL_CLUSTER = "kafka.sasl.cluster";
    public static final String KAFKA_SASL_USER = "kafka.sasl.user";
    public static final String KAFKA_SASL_PASS = "kafka.sasl.pass";
    public static final String KAFKA_JMX_PORT = "kafka.jmx.port";
    public static final String CONSUMER_OFFSET_TOPIC = "__consumer_offsets";
    public static final String KAFKA_OFFSET_COMMIT_TIMEOUT = "kafka.offset.commit.timeout";
    public static final long KAFKA_OFFSET_COMMIT_TIMEOUT_DEFAULT = 180;

    public static final String INSTANCE_KEY = "admin.instance.key";

    // 一些错误码定义
    public static final String ERRCODE_BAD_CONFIG = "1571000";
    public static final String ERRCODE_START_SERVICE = "1571001";
    public static final String ERRCODE_STOP_SERVICE = "1571002";
    public static final String ERRCODE_STOP_MGRSERVER = "1571003";
    public static final String ERRCODE_DATA_NOT_EXIST = "1571004";
    public static final String ERRCODE_DECODE_PASS_FAIL = "1571005";

    public static final String CACHE_NOT_FOUND_CODE = "1502404";

    // 定义一些总线事件
    public static final String TASK_START_FAIL = "task_start_fail";
    public static final String TASK_START_SUCC = "task_start_succ";
    public static final String TASK_STOP_FAIL = "task_stop_fail";
    public static final String TASK_RUN_FAIL = "task_run_fail";
    public static final String TASK_STOP_SUCC = "task_stop_succ";
    public static final String CONNECTOR_START_FAIL = "connector_start_fail";
    public static final String CONNECTOR_STOP_FAIL = "connector_stop_fail";
    public static final String CACHE_SDK_FAIL = "cache_sdk_fail";

    // 离线数据导入事件
    public static final String BATCH_IMPORT_EMPTY_DIR = "empty_dir";
    public static final String BATCH_IMPORT_TOO_MANY_RECORDS = "too_many_records";
    public static final String BATCH_IMPORT_BAD_DATA_FILE = "bad_data_file";

    // iceberg 入库事件
    public static final String ICEBERG_ACQUIRE_LOCK_TIMEOUT = "acquire_lock_timeout";

    public static final int TSDB_BATCH_SIZE = 1000;
    public static final String ZERO = "0";

    // offset 采集相关
    public static final String CONSUL_DNS = "consul.dns";
    public static final String CONCERN_KAFKA_CLUSTERS = "concern.kafka.clusters";
    public static final String LAG_TO_WARN = "lag.warn";
    public static final String PERCENT_TO_WARN = "percent.warn";
    public static final String ALARM_INTERVAL = "alarm.interval";
    public static final String WARN_IGNORE_KAFKA_CLUSTERS = "warn.ignore.kafka.clusters";
    public static final String WARN_IGNORE_GROUPS = "warn.ignore.groups";


    //数据平台存储接口
    public static final String STOREKIT_CLUSTER_TYPE_LIST = "storekit.cluster.type.list";
    public static final String STOREKIT_CAPACITY_WARN_LIMIT = "storekit.capacity.warn.limit";

    // 缓存类型定义
    public static final String IGNITE_REPLICATED_CACHE_MODE = "REPLICATED";
    public static final String IGNITE_PARTITIONED_CACHE_MODE = "PARTITIONED";
    public static final String DEFAULT_SCHEMA = "PUBLIC";
    public static final String[] IGNITE_SYS_FIELDS = {"_KEY", "_VAL", "_VER"};

    public static final String DEFAULT_TASK_CACHE_NAME = "LOAD_TASK_CACHE";

    public static final String TASK_PREPARED = "prepared";
    public static final String TASK_RUNNING = "running";
    public static final String TASK_SUCCESS = "success";
    public static final String TASK_FAILED = "failed";

    // tp 暂停事件， 恢复事件
    public static final String TASK_TP_PAUSE = "task_tp_pause";
    public static final String TASK_TP_RESUME = "task_tp_resume";

    public static final String PARQUET_SUFFIX = ".parquet";

    public static final String INT = "int";
    public static final String STRING = "string";
    public static final String LONG = "long";
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String MEMORY_USAGE = "java.lang.management.MemoryUsage";

    public static final String MEMORY_NAME = "java.lang:type=Memory";
    public static final String THREAD_NAME = "java.lang:type=Threading";


    public static final String CAPACITY_DATABASE = "monitor_custom_metrics";
    public static final String MONITOR_TOPIC = "bkdata_monitor_metrics591";
    public static final int REPORT_BATCH_COUNT = 1000;
    public static final String DEFAULT_GEOG_AREA_TAG = "inland";
    public static final String GEOG_AREA = "geog.area";
    public static final String GEOG_AREAS = "geog_areas";
    public static final String HDFS_CONF = "hdfs_conf";


    public static final String DATABUSMGR_JOB_PUSLAR_HOST_KEY = "databusmgr.job.puslar.host";
    public static final String DATABUSMGR_JOB_PUSLAR_REPORT_BATCH = "databusmgr.job.puslar.report.batch";
    public static final String DATABUSMGR_JOB_PULSAR_IN_MESSAGES_TOTAL = "pulsar_in_messages_total";
    public static final String DATABUSMGR_JOB_PULSAR_DATAMANAGE_TAGS = "databusmgr.job.pulsar.datamanage.tags";
    public static final String API_DMONITOR_METRICS = "api.dmonitor.metrics";
    public static final String API_DMONITOR_METRICS_DEFAULT = "/v3/datamanage/dmonitor/metrics/report/";
    public static final String API_DATAMANAGE_DNS = "api.datamanage.dns";
    public static final String API_META_DNS = "api.meta.dns";
    public static final String API_DATAHUB_DNS = "api.datahub.dns";

    public static final String TP_EVENT_ACCESS_TIMES = "tp.event.access.times";
    public static final Integer TP_EVENT_ACCESS_TIMES_DEFAULT = 3;

    public static final String DATABUS_SHIPPER_STORAGES = "databus.shipper.storages";
    public static final String DATABUS_SHIPPER_STORAGES_DEFAULT =
            "clean,druid,ignite,es,eslog,hdfs,hermes,postgresql,queue,tredis,tsdb,tspider,mysql,tdw";

    public static final String ICEBERG_MAINTAIN_PARALLEL = "iceberg.maintain.parallel";
    public static final String ICEBERG_SUMMARY_PARALLEL = "iceberg.summary.parallel";
    public static final String ICEBERG_TO_DELETE_TABLE_EXPIRE = "iceberg.to.delete.expire";
    public static final String ICEBERG_COMPACT_DAYS = "iceberg.compact.days";
    public static final String ICEBERG_CLEAN_ORPHAN_DAYS = "iceberg.clean.orphan.days";
    public static final String ICEBERG_CLEAN_ORPHAN_METHOD = "iceberg.clean.orphan.method";
    public static final String ICEBERG_REMOVE_TODELETE_DAYS = "iceberg.remove.todelete.days";

    public static final int MAX_MSG_LENGTH = 100;

    public static final String STOREKIT_ALARM_SHIPPER_TYPE = "storekit.alarm.shipper.type";
    public static final String STOREKIT_ALARM_SHIPPER_TYPE_DEFAULT = "hdfsiceberg,druid,ignite";

}
