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

package com.tencent.bk.base.dataflow.bksql.util;

public class Constants {
    public static final String PROPERTY_PREFIX = "flink";
    public static final String PROPERTY_KEY_WINDOW_TYPE = "window_type";
    public static final String PROPERTY_KEY_WINDOW_LENGTH = "window_length";
    public static final String PROPERTY_KEY_COUNT_FREQ = "count_freq";
    public static final String PROPERTY_KEY_WAITING_TIME = "waiting_time";
    public static final String PROPERTY_KEY_SESSION_GAP = "session_gap";
    public static final String PROPERTY_KEY_EXPIRED_TIME = "expired_time";
    public static final String PROPERTY_KEY_ALLOWED_LATENESS = "allowed_lateness";
    public static final String PROPERTY_KEY_LATENESS_TIME = "lateness_time";
    public static final String PROPERTY_KEY_LATENESS_COUNT_FREQ = "lateness_count_freq";
    public static final String PROPERTY_KEY_MODE = "parse.mode";
    public static final String PROPERTY_KEY_SYSTEM_FIELDS = "system_fields";
    public static final String PROPERTY_KEY_RESULT_TABLE_NAME = "result_table_name";
    public static final String PROPERTY_KEY_BIZ_ID = "bk_biz_id";
    public static final String PROPERTY_KEY_ENV = "env";
    public static final String PROPERTY_KEY_PRODUCT_ENV = "product";
    public static final String PROPERTY_KEY_FUNCTION_NAME = "function_name";
    public static final String PROPERTY_KEY_SOURCE_DATA = "source_data";
    public static final String PROPERTY_KEY_IS_CURRENT_RT_NEW = "is_current_rt_new";
    public static final String PROPERTY_KEY_IS_REUSE_TIME_FIELD = "is_reuse_time_field";
    public static final String PROPERTY_KEY_IS_STATIC_DATA = "static_data";

    public static final String PROPERTY_VALUE_MODE_AGGRESSIVE = "aggressive";
    public static final String PROPERTY_VALUE_MODE_TYPICAL = "typical";

    // window type
    public static final String TUMBLING_WINDOW = "tumbling";
    public static final String SLIDING_WINDOW = "sliding";
    public static final String SESSION_WINDOW = "session";
    public static final String ACCUMULATE_WINDOW = "accumulate";

    // window start/end time function
    public static final String TUMBLE_START = "TUMBLE_START";
    public static final String TUMBLE_END = "TUMBLE_END";
    public static final String HOP_START = "HOP_START";
    public static final String HOP_END = "HOP_END";
    public static final String SESSION_START = "SESSION_START";
    public static final String SESSION_END = "SESSION_END";

    public static final String TIME_ATTRIBUTE = "rowtime";
    public static final String TIME_ATTRIBUTE_OUTPUT_NAME = "dtEventTime";
    public static final String WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME = "_startTime_";
    public static final String WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME = "_endTime_";

    // static processor type
    public static final String PROCESSOR_TYPE_JOIN_TRANSFORM = "join_transform";
    public static final String PROCESSOR_TYPE_STATIC_JOIN_TRANSFORM = "static_join_transform";
    // generate table suffix
    public static final String GENERATE_STATIC_JOIN_TABLE_SUFFIX = "StaticJoin";

    public static final String GENERATE_FLINKSQL_STATIC_JOIN_TABLE_SUFFIX = "FlinkSqlStaticJoin";
    public static final String GENERATE_FLINKSQL_JOIN_TABLE_SUFFIX = "FlinkSqlJoin";
    public static final String GENERATE_FLINKSQL_SUBQUERY_TABLE_SUFFIX = "FlinkSqlSubquery";

    public static final String SUBQUERY = "Subquery";

}
