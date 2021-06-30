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

package com.tencent.bk.base.dataflow.core.conf;

public final class ConfigConstants {

    // 静态关联配置
    public static final long STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS = 1000L;
    public static final int STATIC_JOIN_DEFAULT_BATCH_SIZE = 100;
    // 静态关联Ignite配置
    public static final int IGNITE_DEFAULT_POOL_SIZE = 1024;
    public static final int IGNITE_DEFAULT_DATA_CACHE = 10000;
    public static final int IGNITE_DEFAULT_ASYNC_IO_TIMEOUT_SEC = 30;
    public static final int IGNITE_DEFAULT_ASYNC_IO_CONCURRENCY = 10;

    // 数据时间水位配置
    public static final long EVENT_TIME_WATERMARK_FUTURE_TIME_TOLERANCE_MS = 60000L;
    public static final long EVENT_TIME_WATERMARK_BATCH_PROCESS_NUM = 1000L;
    public static final long EVENT_TIME_WATERMARK_BATCH_PROCESS_INTERVAL_MS = 1000L;

    // kafka sink 配置
    public static final String SINK_KAFKA_ACKS_DEFAULT_VALUE = "1";
    public static final int SINK_KAFKA_RETRIES_DEFAULT_VALUE = 5;
    public static final int SINK_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT_VALUE = 5;
    public static final int SINK_KAFKA_BATCH_SIZE_DEFAULT_VALUE = 1048576;
    public static final int SINK_KAFKA_LINGER_MS_DEFAULT_VALUE = 1000;
    public static final int SINK_KAFKA_BUFFER_MEMORY_DEFAULT_VALUE = 52428800;
    public static final int SINK_KAFKA_MAX_REQUEST_SIZE_DEFAULT_VALUE = 3145728;

    // http 配置
    public static final int HTTP_CONNECT_TIMEOUT = 10000;
    public static final int HTTP_READ_TIMEOUT = 10000;

    // 调试任务配置
    public static final int DEBUG_STREAM_JOB_MAX_RUNNING_TIME_S = 2 * 60;

}
