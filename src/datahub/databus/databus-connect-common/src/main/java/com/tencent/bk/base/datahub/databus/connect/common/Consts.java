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

package com.tencent.bk.base.datahub.databus.connect.common;

public class Consts {


    public static final String API_DNS = "api.dns";
    public static final String API_CLUSTER_PATH = "api.cluster.path";
    public static final String API_RT_PATH = "api.rt.path";
    public static final String API_DATANODE_PATH = "api.datanode.path";
    public static final String API_ADD_OFFLINE_TASK = "api.add.offline.task";
    public static final String API_GET_OFFLINE_TASK = "api.get.offline.task";
    public static final String API_GET_OFFLINE_TASKS = "api.get.offline.tasks";
    public static final String API_UPDATE_OFFLINE_TASK = "api.update.offline.task";
    public static final String API_FINISH_OFFLINE_TASK = "api.finish.offline.task";
    public static final String API_GET_DATAIDS_TOPICS = "api.get.dataids.topics";
    public static final String API_ADD_DATABUS_STORAGE_EVENT = "api.add.databus.storage.event";
    public static final String API_GET_EARLIEST_HDFS_IMPORT_TASK = "api.get.earliest.hdfs.import.task";
    public static final String API_UPDATE_HDFS_IMPORT_TASK = "api.update.hdfs.import.task";
    public static final String API_DNS_DEFAULT = "xx.xx.xx.xx";
    public static final String API_SERVICE_STATUS = "/get_service_status";
    public static final String API_CLUSTER_PATH_DEFAULT = "/databus/shipper/get_cluster_info";
    public static final String API_RT_PATH_DEFAULT = "/databus/shipper/get_rt_info";
    public static final String API_DATANODE_PATH_DEFAULT = "/v3/databus/datanodes/";
    public static final String API_ADD_OFFLINE_TASK_DEFAULT = "/databus/shipper/add_offline_task";
    public static final String API_GET_OFFLINE_TASK_DEFAULT = "/databus/shipper/get_offline_task_info";
    public static final String API_GET_OFFLINE_TASKS_DEFAULT = "/databus/shipper/get_offline_tasks";
    public static final String API_UPDATE_OFFLINE_TASK_DEFAULT = "/databus/shipper/update_offline_task_info";
    public static final String API_FINISH_OFFLINE_TASK_DEFAULT = "/databus/shipper/finish_offline_task";
    public static final String API_GET_DATAIDS_TOPICS_DEFAULT = "/databus/shipper/get_dataids_topics";
    public static final String API_ADD_DATABUS_STORAGE_EVENT_DEFAULT = "/v3/databus/events/";
    public static final String API_GET_EARLIEST_HDFS_IMPORT_TASK_DEFAULT = "/databus/shipper"
            + "/get_earliest_hdfs_import_task";
    public static final String API_UPDATE_HDFS_IMPORT_TASK_DEFAULT = "/databus/shipper/update_hdfs_import_task";

    // 数据打点使用的常量
    public static final String ETH0 = "eth0";
    public static final String ETH1 = "eth1";
    public static final String LOOPBACK_IP = "127.0.0.1";
    public static final String UP_STREAM = "up.stream";
    public static final String UP_STREAM_DEFAULT = "outer";
    public static final String DOWN_STREAM = "down.stream";
    public static final String DOWN_STREAM_DEFAULT = "inner";
    public static final String MODULE_NAME = "module.name";
    public static final String MODULE_NAME_DEFAULT = "databus";
    public static final String COMP_NAME = "component.name";
    public static final String COMP_DEFAULT = "databus";
    public static final String CLUSTER_DEFAULT = "databus";
    public static final String INFO = "info";
    public static final String TIME = "time";
    public static final String COMPONENT = "component";
    public static final String CLEAN = "clean";
    public static final String SHIPPER = "shipper";
    public static final String ACCESS = "access";
    public static final String OFFLINE = "offline";
    public static final String MODULE = "module";
    public static final String DATABUS = "databus";
    public static final String LOGICAL_TAG = "logical_tag";
    public static final String PHYSICAL_TAG = "physical_tag";
    public static final String CUSTOM_TAGS = "custom_tags";
    public static final String TAG = "tag";
    public static final String DESC = "desc";
    public static final String RESULT_TABLE_ID = "result_table_id";
    public static final String CLUSTER = "cluster";
    public static final String WORKER_IP = "worker_ip";
    public static final String THREAD = "thread";
    public static final String CONNECTOR = "connector";
    public static final String LOCATION = "location";
    public static final String DOWNSTREAM = "downstream";
    public static final String UPSTREAM = "upstream";
    public static final String METRICS = "metrics";
    public static final String DATA_MONITOR = "data_monitor";
    public static final String RESOURCE_MONITOR = "resource_monitor";
    public static final String CUSTOM_METRICS = "custom_metrics";
    public static final String DATA_LOSS = "data_loss";
    public static final String DATA_DELAY = "data_delay";
    public static final String INPUT = "input";
    public static final String OUTPUT = "output";
    public static final String TAGS = "tags";
    public static final String TOTAL_CNT = "total_cnt";
    public static final String TOTAL_CNT_INCREMENT = "total_cnt_increment";
    public static final String DATA_DROP = "data_drop";
    public static final String CNT = "cnt";
    public static final String REASON = "reason";
    public static final String MIN_DELAY = "min_delay";
    public static final String MAX_DELAY = "max_delay";
    public static final String WINDOW_TIME = "window_time";
    public static final String WAITING_TIME = "waiting_time";
    public static final String OUTPUT_TIME = "output_time";
    public static final String DATA_TIME = "data_time";
    public static final String DELAY_TIME = "delay_time";
    public static final String RECORD_COUNT = "record_count";
    public static final String MSG_COUNT = "msg_count";
    public static final String MSG_SIZE = "msg_size";
    public static final String DATABUS_PROCESS_METRIC = "databus_process_metric";
    public static final String CLUSTER_TYPE = "cluster_type";
    // 清洗失败的数据的字符串常量
    public static final String RTID = "rtId";
    public static final String DATAID = "dataId";
    public static final String MSGKEY = "msgKey";
    public static final String MSGVALUE = "msgValue";
    public static final String PARTITION = "partition";
    public static final String ERROR = "error";


    /*
     * 所有配置项按照用途分类,以不同的prefix来定义:
     *     deploy.            -> 部署相关配置
     *     cluster.           -> kafka connect worker相关配置项
     *     consumer.          -> kafka consumer相关配置,用于sink connector
     *     monitor.           -> 数据埋点,数据上报,自身检测等相关配置
     *     monitor.producer.  -> 埋点数据上报的kafka配置
     *     connector.         -> connector的一些配置项,用于某些场景下覆盖集群中所有connector的配置项
     */
    public static final String DEPLOY_PREFIX = "deploy.";
    public static final String CCCACHE_PREFIX = "cc.cache.";
    public static final String CLUSTER_PREFIX = "cluster.";
    public static final String CONSUMER_PREFIX = "consumer.";
    public static final String PRODUCER_PREFIX = "producer.";
    public static final String MONITOR_PREFIX = "monitor.";
    public static final String METRIC_PREFIX = "monitor.producer.";
    public static final String CONNECTOR_PREFIX = "connector.";


    public static final String VERSION = "version";
    public static final String MSG_SOURCE_TYPE = "msg.type";
    // 源数据类型定义
    public static final String ETL = "etl";
    public static final String AVRO = "avro";
    public static final String PARQUET = "parquet";
    public static final String JSON = "json";
    public static final String DOCKER_LOG = "dockerlog";

    // TaskContext 用到的常量
    public static final String TOPICS = "topics";
    public static final String TOPIC = "topic";
    public static final String ETL_CONF = "etl.conf";
    public static final String ETL_ID = "etl.id";
    public static final String DATA_ID = "data.id";
    public static final String COLUMNS = "columns";
    public static final String DIMENSIONS = "dimensions";
    public static final String HDFS_DATA_TYPE = "hdfs.data_type";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String GROUP_ID = "group.id";
    public static final String KEY_DESER = "key.deserializer";
    public static final String VALUE_DESER = "value.deserializer";
    public static final String NETWORK_LIST = "network.list";

    // 设定数据库连接池的大小(每个connectionUrl对应一个连接池
    public static final String CONN_POOL_SIZE = "conn.pool.size";
    public static final int CONN_POOL_SIZE_DEFAULT = 3;

    // monitor相关配置项(和kafka producer相关的配置,均以monitor.producer开头)
    public static final String METRIC_TOPIC = "monitor.producer.topic";
    public static final String DEFAULT_METRIC_TOPIC = "bkdata_monitor_metrics";
    public static final String BAD_MSG_TOPIC = "monitor.badmsg.topic";
    public static final String DEFAULT_BAD_MSG_TOPIC = "bkdata_bad_msg591";
    public static final String DATABUS_CLUSTER_STAT_TOPIC = "monitor.cluster.stat.topic";
    public static final String DEFAULT_DATABUS_CLUSTER_STAT_TOPIC = "databus_cluster_stat";
    public static final String DATABUS_EVENT_TOPIC = "monitor.databus.event.topic";
    public static final String DEFAULT_DATABUS_EVENT_TOPIC = "databus_event";
    public static final String DATABUS_ERROR_LOG_TOPIC = "monitor.databus.error.log.topic";
    public static final String DEFAULT_DATABUS_ERROR_LOG_TOPIC = "databus_error_log";
    public static final String METRIC_KAFKA_BS = "monitor.producer.bootstrap.servers";
    public static final String DEFAULT_METRIC_KAFKA_BS = "xxx.xx.xx.xx:9092";
    public static final String METRIC_TIMEOUT = "monitor.producer.request.timeout.ms";
    public static final String METRIC_RETRIES = "monitor.producer.retries";
    public static final String METRIC_ACKS = "monitor.producer.acks";
    public static final String METRIC_INFLIGHT_REQUESTS = "monitor.producer.max.in.flight.requests.per.connection";
    public static final String CLUSTER_DISABLE_CONTEXT_REFRESH = "cluster.disable.context.refresh";


    // monitor 相关配置项,和上报周期,端口,自检测周期,忽略关键字等
    public static final String REPORT_INTERVAL_MS = "monitor.report.interval.ms";
    public static final String ALARM_MEMORY_PERCENT = "monitor.alarm.memory.percent";

    public static final String TIMESTAMP = "timestamp";
    public static final String OFFSET = "offset";
    public static final String DTEVENTTIME = "dtEventTime";
    public static final String DTEVENTTIMESTAMP = "dtEventTimeStamp";
    public static final String THEDATE = "thedate";
    public static final String LOCALTIME = "localTime";
    public static final String ITERATION_IDX = "_iteration_idx";

    public static final String JDBC_CRATE = "jdbc:crate:";
    public static final String JDBC_MYSQL = "jdbc:mysql:";
    public static final String JDBC_POSTGRESQL = "jdbc:postgresql:";
    public static final String CRATE_DB = "crate";
    public static final String MYSQL_DB = "mysql";

    public static final String _VALUE_ = "_value_";
    public static final String _TAGTIME_ = "_tagTime_";
    public static final String _METRICTAG_ = "_metricTag_";
    public static final String UTF8 = "UTF-8";
    public static final String ISO_8859_1 = "ISO-8859-1";
    public static final String BAD_AVRO_RECORD = "BadAvroRecord";
    public static final String BAD_KAFKA_MSG_VALUE = "BadKafkaMsgValue";
    public static final String BAD_KAFKA_MSG_KEY = "BadKafkaMsgKey";

    public static final String BAD_MSG_PARTITION = "bad_msg_partition";
    public static final String BAD_MSG_OFFSET = "bad_msg_offset";
    public static final String BAD_MSG = "bad_msg";
    public static final String _TIME_ = "_time_";
    public static final String SYSTEM_CLEAN_BADMSG = "system_clean_badmsg";

    public static final String SASL_USER = "sasl.user";
    public static final String SASL_PASS = "sasl.pass";
    public static final String INSTANCE_KEY = "instance.key";
    public static final String PASSWD = "passwd";

    public static final String CONSUMER_OFFSET_TOPIC = "__consumer_offsets";

    public static final String ZK_ETL_CHANGE_PATH = "/config/databus/etl_change";

    public static final String TDW_FINISH_DIR = "tdwFinishDir";
    public static final String COLLECTOR_DS = "ds";
    public static final String COLLECTOR_DS_TAG = "tag";
    public static final String OLD_COLLECTOR_DS = "collector-ds";
    public static final String OLD_COLLECTOR_DS_TAG = "collector-ds-tag";
    public static final String DATABUS_EVENT = "DatabusEvent";
    public static final String STORAGE = "storage";
    public static final String EVENT_TYPE = "event_type";
    public static final String EVENT_VALUE = "event_value";
    public static final String RT_ID = "rt_id";
    public static final String STORAGE_HDFS = "hdfs";
    public static final String STORAGE_CLEAN = "clean";
    public static final String DATA_DIR = "data_dir";
    public static final String STATUS_UPDATE = "status_update";
    public static final String FINISH = "finish";
    public static final String ID = "id";

    public static final String BK_DATA_PREFIX = "bk_data_";


    // 定义一些总线事件
    public static final String TASK_START_FAIL = "task_start_fail";
    public static final String TASK_START_SUCC = "task_start_succ";
    public static final String TASK_STOP_FAIL = "task_stop_fail";
    public static final String TASK_RUN_FAIL = "task_run_fail";
    public static final String TASK_STOP_SUCC = "task_stop_succ";
    public static final String CONNECTOR_START_FAIL = "connector_start_fail";
    public static final String CONNECTOR_STOP_FAIL = "connector_stop_fail";
    // 离线数据导入事件
    public static final String BATCH_IMPORT_EMPTY_DIR = "empty_dir";
    public static final String BATCH_IMPORT_TOO_MANY_RECORDS = "too_many_records";
    public static final String BATCH_IMPORT_BAD_DATA_FILE = "bad_data_file";

    // tp 暂停事件， 恢复事件
    public static final String TASK_TP_PAUSE = "task_tp_pause";
    public static final String TASK_TP_RESUME = "task_tp_resume";
}
