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

public class ConstantVar {

    /**
     * metric node id 和 task id之间的分隔符
     */
    public static final String METRIC_ID_SEPARATOR = " ";
    /**
     * 记录数据源partition的offset的字段
     */
    public static final String BKDATA_PARTITION_OFFSET = "bkdata_par_offset";
    public static final String EVENT_TIME = "dtEventTime";
    public static final String LOCAL_TIME = "localTime";
    public static final String EVENT_TIMESTAMP = "dtEventTimeStamp";
    /**
     * window开始时间和结束时间
     */
    public static final String WINDOW_START_TIME = "_startTime_";
    public static final String WINDOW_END_TIME = "_endTime_";

    /**
     * 区分引擎框架
     */
    public enum Component {
        flink,
        spark_sql,
        one_time_sql,
        hive,
        tdw_spark,
        tdw_spark_jar,
        spark_streaming,
        local,
        spark_mllib
    }

    /**
     * role 可以用于node和job
     */
    public enum Role {
        stream,
        batch,
        modeling,
        queryset
    }

    /**
     * 窗口类型
     */
    public enum WindowType {
        tumbling,
        sliding,
        accumulate,
        session
    }

    public enum TopoNodeType {
        source,
        transform,
        sink
    }

    public enum DataDriveType {
        event_time,
        process_time,
        session
    }

    public enum PeriodUnit {
        second, minute, hour, day, week, month
    }

    /**
     * 运行模式
     */
    public enum RunMode {
        product, debug, udf_debug, release_debug
    }

    public enum CodeLanguage {
        java, python
    }

    public enum UserFunction {
        udf, udtf, udaf
    }

    public enum ChannelType {
        kafka, hdfs, iceberg, string, json_map
    }

    public enum TableType {
        result_table, query_set, other
    }

    /**
     * 数据类型
     */
    public enum DataType {
        INT,
        LONG,
        DOUBLE,
        FLOAT,
        STRING
    }

    /**
     * HDFS上表数据的存储结构
     */
    public enum HdfsTableFormat {
        parquet,
        iceberg,
        other
    }

    /**
     * 源节点类型，数据或是模型
     */
    public enum DataNodeType {
        data,
        model
    }

    /**
     * Transform的类型
     */
    public enum TransformType {
        mlsql_query,
        sub_query
    }

    /**
     * Spark mllib的运行模式
     */
    public enum SparkMLlibRunMode {
        train("train"),
        untrained_run("untrained-run"),
        trained_run("trained-run");
        private String value;

        SparkMLlibRunMode(String value) {
            this.value = value;
        }

        public static SparkMLlibRunMode parse(String value) {
            for (SparkMLlibRunMode runMode : SparkMLlibRunMode.values()) {
                if (runMode.value.equalsIgnoreCase(value)) {
                    return runMode;
                }
            }
            throw new RuntimeException("Unsupported value:" + value);
        }

    }
}
