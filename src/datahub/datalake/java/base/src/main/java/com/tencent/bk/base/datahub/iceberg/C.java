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

package com.tencent.bk.base.datahub.iceberg;

public class C {

    public static final String FILE_FORMAT = "parquet";
    public static final String RETRY_COMMIT_COUNT = "100";

    public static final String DEFAULT_FS = "fs.defaultFS";

    public static final String INT = "int";
    public static final String LONG = "long";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String STRING = "string";
    public static final String TEXT = "text";
    public static final String TIMESTAMP = "timestamp";

    public static final String YEAR = "year";
    public static final String MONTH = "month";
    public static final String DAY = "day";
    public static final String HOUR = "hour";
    public static final String IDENTITY = "identity";
    public static final String BUCKET = "bucket";
    public static final String TRUNCATE = "truncate";

    public static final String RETRY_TIMES = "100";
    public static final String MIN_RETRY_WAIT_MS = "200";  // 0.2s
    public static final String MAX_RETRY_WAIT_MS = "2000";  // 2s
    public static final String TOTAL_RETRY_TIME_MS = "60000";  // 60s

    public static final String ET = "____et";  // 默认分区字段
    public static final String DTET = "dtEventTime";
    public static final String DTETS = "dtEventTimeStamp";
    public static final String THEDATE = "thedate";
    public static final String LOCALTIME = "localTime";
    public static final String DATABUSEVENT = "DatabusEvent";
    public static final String VALUE = "_value_";
    public static final String TAGTIME = "_tagTime_";
    public static final String METRICTAG = "_metricTag_";

    // 定义在snapshot的summary中使用的常量
    // {"dt-sdk-msg": {"operation": "dt-add", "data": {"abc-0": 1212, "abc-1": 888}}}
    public static final String DT_SDK_MSG = "dt-sdk-msg";
    public static final String OPERATION = "operation";
    public static final String DATA = "data";
    public static final String DT_ADD = "dt-add";
    public static final String DT_UPDATE = "dt-update";
    public static final String DT_DELETE = "dt-delete";
    public static final String DT_RESET = "dt-reset";
    public static final String TRUNCATE_FILES = "truncate_files";
    public static final String COUNT = "count";

    public static final String EQUALS = "=";
    public static final String EXPR = "expr";
    public static final String FIELDS = "fields";
    public static final String TABLE = "table";
    public static final String PROPERTIES = "properties";
    public static final String PARTITION = "partition";
    public static final String SCHEMA = "schema";
    public static final String ID = "id";
    public static final String TIMESTAMP_MS = "timestamp_ms";
    public static final String SUMMARY = "summary";
    public static final String SNAPSHOTS = "snapshots";
    public static final String PARTITION_PATHS = "partition_paths";
    public static final String SAMPLE = "sample";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String OPTIONAL = "optional";
    public static final String LOCATION = "location";
    public static final String MANIFEST_LIST_LOCATION = "manifestListLocation";

    public static final String TABLE_PREFIX = "table.";
    public static final String MAX_REWRITE_FILES = "max.rewrite.files";
    public static final String PRESERVE_SNAPSHOT_DAYS = "preserve.snapshot.days";
    public static final String PRESERVE_SNAPSHOT_NUMS = "preserve.snapshot.nums";
    public static final String MAX_COMPACT_FILE_SIZE = "max.compact.file.size";
    public static final String WORKER_THREADS = "worker.threads";
    public static final String HADOOP_WAREHOUSE_DIR = "hadoop.warehouse.dir";
    public static final String HADOOP_WAREHOUSE_DIR_DEFAULT = "iceberg/warehouse";
    public static final String MAX_COMMIT_DATAFILES = "max.commit.datafiles";
    public static final int DFT_MAX_COMMIT_DATAFILES = 100;

    public static final String SCAN_FILES = "scan.files";
    public static final String SCAN_BYTES = "scan.bytes";
    public static final String SCAN_RECORDS = "scan.records";
    public static final String AFFECTED_RECORDS = "affected.records";
    public static final String FINISHED = "finished";
    public static final String DURATION = "duration";

    public static final String HTTP = "http";
    public static final String COLON = ":";
    public static final String RESULT = "result";
    public static final String ICEBERG_SNAPSHOT = "iceberg_snapshot";
    public static final String TABLE_NAME = "table_name";
    public static final String SNAPSHOT_ID = "snapshot_id";
    public static final String SEQUENCE_NUMBER = "sequence_number";
    public static final String FALSE = "false";

}