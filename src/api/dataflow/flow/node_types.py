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

from django.utils.translation import ugettext_lazy as _


class NodeTypes(object):
    """
    节点类型相关的常量
    """

    # ------ 内部版节点
    # 数据源
    STREAM_SOURCE = "stream_source"
    BATCH_SOURCE = "batch_source"
    UNIFIED_KV_SOURCE = "unified_kv_source"
    # 数据处理
    STREAM = "realtime"
    BATCHV2 = "batchv2"
    SPLIT_KAFKA = "split"
    MERGE_KAFKA = "merge"
    SPARK_STRUCTURED_STREAMING = "spark_structured_streaming"
    FLINK_STREAMING = "flink_streaming"
    # 机器学习
    MODEL = "model"
    PROCESS_MODEL = "process_model"
    MODEL_TS_CUSTOM = "model_ts_custom"
    MODEL_APP = "model_app"
    # 数据模型
    DATA_MODEL_APP = "data_model_app"
    DATA_MODEL_STREAM_INDICATOR = "data_model_stream_indicator"
    DATA_MODEL_BATCH_INDICATOR = "data_model_batch_indicator"
    # 数据存储
    DRUID_STORAGE = "druid_storage"
    ES_STORAGE = "elastic_storage"
    QUEUE_PULSAR_STORAGE = "queue_pulsar"
    QUEUE_STORAGE = "queue_storage"
    HDFS_STORAGE = "hdfs_storage"
    IGNITE_STORAGE = "ignite"
    MYSQL_STORGAE = "mysql_storage"
    CLICKHOUSE_STORAGE = "clickhouse_storage"
    # 其它内部常量
    RT_SOURCE = "rtsource"
    CLEAN = "clean"
    # channel
    KAFKA = "kafka"
    HDFS = "hdfs"
    TDW = "tdw"
    MEMORY = "memory"
    UNKNOWN = "unknown"
    SYSTEM_TYPE = "system"
    USER_TYPE = "user"
    # ------ extend 节点
    # 数据源
    KV_SOURCE = "kv_source"
    BATCH_KV_SOURCE = "batch_kv_source"
    TDW_SOURCE = "tdw_source"
    # 数据处理
    BATCH = "offline"
    TDW_BATCH = "tdw_batch"
    TDW_JAR_BATCH = "tdw_jar_batch"
    # 数据存储
    HERMES_STORAGE = "hermes_storage"
    TPG_STORAGE = "tpg"
    TSPIDER_STORAGE = "tspider_storage"
    TDBANK_STORAGE = "tdbank"
    TDW_STORAGE = "tdw_storage"
    TREDIS_STORAGE = "tredis_storage"
    TSDB_STORAGE = "tsdb_storage"
    TCAPLUS_STORAGE = "tcaplus_storage"
    POSTGRESQL_STORAGE = "pgsql_storage"

    # 这个 map 的目的是 flow 这边存的是 mysql_storage, 总线存的是 mysql, 做个映射
    STORAGE_NODES_MAPPING = {
        DRUID_STORAGE: "druid",
        ES_STORAGE: "es",
        QUEUE_PULSAR_STORAGE: "queue_pulsar",
        QUEUE_STORAGE: "queue",
        HDFS_STORAGE: "hdfs",
        IGNITE_STORAGE: "ignite",
        MYSQL_STORGAE: "mysql",
        CLICKHOUSE_STORAGE: "clickhouse",
        # extend 存储
        HERMES_STORAGE: "hermes",
        TPG_STORAGE: "tpg",
        TSPIDER_STORAGE: "tspider",
        TDBANK_STORAGE: "tdbank",
        TREDIS_STORAGE: "tredis",
        TSDB_STORAGE: "tsdb",
        TCAPLUS_STORAGE: "tcaplus",
        POSTGRESQL_STORAGE: "postgresql",
    }

    # 运行画布时控制台输出的日志, 取的这里面的值
    DISPLAY = {
        STREAM_SOURCE: _("实时数据源"),
        BATCH_SOURCE: _("离线数据源"),
        UNIFIED_KV_SOURCE: _("关联数据源"),
        STREAM: _("实时计算"),
        BATCHV2: _("离线计算V2"),
        SPLIT_KAFKA: _("切分计算"),
        MERGE_KAFKA: _("合流计算"),
        SPARK_STRUCTURED_STREAMING: _("spark_structured_streaming"),
        FLINK_STREAMING: _("flink_streaming"),
        MODEL: _("ModelFlow模型"),
        PROCESS_MODEL: _("单指标异常检测"),
        MODEL_TS_CUSTOM: _("时序模型应用"),
        MODEL_APP: _("模型应用"),
        DATA_MODEL_APP: _("数据模型应用"),
        DATA_MODEL_STREAM_INDICATOR: _("数据模型指标(实时)"),
        DATA_MODEL_BATCH_INDICATOR: _("数据模型指标(离线)"),
        DRUID_STORAGE: _("Druid"),
        ES_STORAGE: _("Elasticsearch"),
        QUEUE_PULSAR_STORAGE: _("Pulsar"),
        QUEUE_STORAGE: _("队列服务"),
        HDFS_STORAGE: _("HDFS"),
        IGNITE_STORAGE: _("Ignite"),
        MYSQL_STORGAE: _("MySQL"),
        CLICKHOUSE_STORAGE: _("ClickHouse"),
        RT_SOURCE: _("结果数据"),
        UNKNOWN: _("未知节点类型"),
        # extend 节点
        KV_SOURCE: _("TRedis实时KV数据源"),
        BATCH_KV_SOURCE: _("离线KV数据源"),
        TDW_SOURCE: _("TDW数据源"),
        BATCH: _("离线计算"),
        TDW_BATCH: _("TDW离线计算"),
        TDW_JAR_BATCH: _("TDW_JAR离线计算"),
        TSDB_STORAGE: _("TSDB"),
        TREDIS_STORAGE: _("Tredis"),
        TSPIDER_STORAGE: _("Tspider"),
        HERMES_STORAGE: _("Hermes"),
        TDW_STORAGE: _("TDW存储"),
        POSTGRESQL_STORAGE: _("Postgresql"),
        TPG_STORAGE: _("TPG"),
        TDBANK_STORAGE: _("TDBANK"),
        TCAPLUS_STORAGE: _("Tcaplus"),
    }
    # 分组
    # channel 存储
    CHANNEL_STORAGE_CATEGORY = [KAFKA, HDFS, TDW]
    # 数据源
    SOURCE_CATEGORY = [KV_SOURCE, BATCH_KV_SOURCE, BATCH_SOURCE, STREAM_SOURCE, TDW_SOURCE, UNIFIED_KV_SOURCE]
    BATCH_SOURCE_CATEGORY = [BATCH_SOURCE, TDW_SOURCE]
    BATCH_CATEGORY = [BATCH, BATCHV2, TDW_BATCH, TDW_JAR_BATCH, MODEL_APP, DATA_MODEL_BATCH_INDICATOR]
    # 三个关联数据源的种类：实时、离线、总关联数据源
    STREAM_KV_SOURCE_CATEGORY = [KV_SOURCE, UNIFIED_KV_SOURCE]
    BATCH_KV_SOURCE_CATEGORY = [BATCH_KV_SOURCE, UNIFIED_KV_SOURCE]
    ALL_KV_SOURCE_CATEGORY = [KV_SOURCE, BATCH_KV_SOURCE, UNIFIED_KV_SOURCE]
    MODEL_APP_CATEGORY = [MODEL_APP]
    CALC_CATEGORY = [
        STREAM,
        BATCH,
        BATCHV2,
        TDW_BATCH,
        TDW_JAR_BATCH,
        SPARK_STRUCTURED_STREAMING,
        FLINK_STREAMING,
        MODEL_APP,
        DATA_MODEL_APP,
        DATA_MODEL_STREAM_INDICATOR,
        DATA_MODEL_BATCH_INDICATOR,
    ]
    CORRECT_SQL_CATEGORY = [STREAM, BATCH, BATCHV2]
    # TODO: from_result_table_ids 参数改造，当前只适用部分节点
    NEW_FROM_RESULT_TABLE_IDS_CATEGORY = [
        SPARK_STRUCTURED_STREAMING,
        FLINK_STREAMING,
        BATCH_KV_SOURCE,
        TDW_JAR_BATCH,
        QUEUE_PULSAR_STORAGE,
        TDW_BATCH,
        IGNITE_STORAGE,
        MODEL_APP,
        DATA_MODEL_APP,
        DATA_MODEL_STREAM_INDICATOR,
        DATA_MODEL_BATCH_INDICATOR,
    ]
    NEW_INPUTS_CATEGORY = [BATCHV2]
    PROCESSING_CATEGORY = [
        STREAM,
        BATCH,
        BATCHV2,
        MERGE_KAFKA,
        SPLIT_KAFKA,
        MODEL,
        PROCESS_MODEL,
        MODEL_TS_CUSTOM,
        TDW_STORAGE,
        TDW_JAR_BATCH,
        TDW_BATCH,
        SPARK_STRUCTURED_STREAMING,
        FLINK_STREAMING,
        MODEL_APP,
        DATA_MODEL_APP,
        DATA_MODEL_STREAM_INDICATOR,
        DATA_MODEL_BATCH_INDICATOR,
    ]
    # 混合计算节点，具有实时和离线属性
    MIX_PROCESSING_CATEGORY = [MODEL, PROCESS_MODEL, MODEL_TS_CUSTOM]
    SUPPORT_RESTART_SLIGHTLY_CATEGORY = [MERGE_KAFKA]
    # 根据下游节点类型，动态增减 channel 存储的节点类型
    DYNAMIC_ADD_CHANNEL_CATEGORY = [
        STREAM,
        BATCH,
        BATCHV2,
        MODEL,
        SPARK_STRUCTURED_STREAMING,
        FLINK_STREAMING,
        MODEL_APP,
        DATA_MODEL_APP,
        DATA_MODEL_STREAM_INDICATOR,
        DATA_MODEL_BATCH_INDICATOR,
    ]
    STORAGE_CATEGORY = list(STORAGE_NODES_MAPPING.keys())
    SUPPORT_RECALC_CATEGORY = [BATCH, BATCHV2, TDW_JAR_BATCH, TDW_BATCH, DATA_MODEL_BATCH_INDICATOR]
    TDW_CATEGORY = [TDW_STORAGE, TDW_JAR_BATCH, TDW_BATCH, TDW_SOURCE, TPG_STORAGE]
