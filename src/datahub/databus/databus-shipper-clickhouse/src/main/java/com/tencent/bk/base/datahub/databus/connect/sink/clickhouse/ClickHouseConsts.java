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

package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;

public class ClickHouseConsts {

    // variable
    public static final String SHIPPER_FLUSH_SIZE = "shipper_flush_size";
    public static final String SHIPPER_FLUSH_INTERVAL = "shipper_flush_interval";
    public static final String HTTP_TGW = "http_tgw";
    public static final String COLON = ":";
    public static final String COMMA = ",";
    public static final String CLICKHOUSE_PROPERTIES = "clickhouse.properties";
    public static final String CLICKHOUSE_COLUMN_ORDER = "clickhouse.column.order";
    public static final String DB_NAME = "db.name";
    public static final String RT_ID = "rt.id";
    public static final String REPLICATED_TABLE = "replicated.table";
    public static final String CLUSTER_TYPE = "cluster.type";
    public static final String CONNECTOR_GROUP = "Connector";
    public static final String CLICKHOUSE_GROUP = "ClickHouse";
    public static final String CLICKHOUSE = "clickhouse";
    public static final String FLUSH_SIZE = "flushSize";
    public static final String FLUSH_INTERVAL = "flushInterval";
    public static final String PROCESSORS_SIZE = "processorsSize";

    // type
    public static final String INT64 = "Int64";
    public static final String INT32 = "Int32";
    public static final String FLOAT64 = "Float64";
    public static final String FLOAT32 = "Float32";
    public static final String STRING = "String";
    public static final String DATE_TIME = "DateTime";
    public static final String DT_EVENT_TIMESTAMP = "dteventtimestamp";
    public static final String TIME_COLUMN = "__time";
    public static final String WEIGHTS = "weights";

    // default value
    public static final int DEFAULT_SHIPPER_FLUSH_SIZE = 200_000;
    public static final int DEFAULT_SHIPPER_FLUSH_INTERVAL = 60; // 单位 s
    public static final int DEFAULT_UPDATE_WEIGHTS_INTERVAL = 1; //单位小时
    public static final int MAX_RETRY = 3;
    public static final int DEFAULT_PROCESSORS_SIZE = 1;

    // error code
    public static final String CLICKHOUSE_QUERY_WEIGHTS_ERROR = "clickhouse_query_weights_error";
    public static final String CLICKHOUSE_UPDATE_WEIGHTS_ERROR = "clickhouse_update_weights_error";
    public static final String CLICKHOUSE_UNSUPPORTED_DATA_TYPE = "clickhouse_unsupported_data_type";
    public static final String CLICKHOUSE_BAD_PROPERTIES = "clickhouse_bad_properties";
    public static final String CLICKHOUSE_WRITE_ERROR = "clickhouse_write_error";
    public static final String CLICKHOUSE_MAINTAIN_PROCESSORS_ERROR = "clickhouse_maintain_processors_error";

    // URL
    public static final String DEFAULT_API_RT_WEIGHTS_PATH = "/v3/storekit/clickhouse/%s/weights/";

}
