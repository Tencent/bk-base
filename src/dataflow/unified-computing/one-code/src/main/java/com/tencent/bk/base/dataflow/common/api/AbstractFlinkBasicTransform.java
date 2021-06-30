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

package com.tencent.bk.base.dataflow.common.api;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public abstract class AbstractFlinkBasicTransform extends AbstractBasicTransform<DataStream<Row>> {

    private transient StreamExecutionEnvironment env;

    private transient StreamTableEnvironment tableEnv;

    private void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * 获取 Flink StreamExecutionEnvironment 运行环境对象
     *
     * @return
     */
    public StreamExecutionEnvironment getEnv() {
        return this.env;
    }

    private void setTableEnv(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    /**
     * 获取 Flink StreamTableEnvironment 运行环境对象
     *
     * @return
     */
    public StreamTableEnvironment getTableEnv() {
        return this.tableEnv;
    }

    /**
     * 根据字段名称获取字段在 dataStream 数据表中的索引， dtEventTimeStamp,localTime,dtEventTime 可使用
     *
     * @param dataStream
     * @param fieldName
     * @return
     */
    public int getFieldIndexByName(DataStream<Row> dataStream, String fieldName) {
        TableSchema tableSchema = this.tableEnv.fromDataStream(dataStream).getSchema();
        int index = Arrays.asList(tableSchema.getFieldNames()).indexOf(fieldName);
        if (index == -1) {
            throw new RuntimeException(String.format("获取字段位置索引失败，dataStream 不存在指定字段: %s，所有字段为：%s",
                    fieldName, StringUtils.join(tableSchema.getFieldNames(), ",")));
        }
        return index;
    }

}
