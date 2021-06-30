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

package com.tencent.bk.base.dataflow.flink.streaming.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;


public class TestFlinkRegExpExtract {

    /**
     * 使用main函数测试，因为测试过程比较耗时，不在maven编译时测试
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Tuple4<Long, String, Integer, String>> ds = env
                .addSource(new SourceFunction<Tuple4<Long, String, Integer, String>>() {
                    @Override
                    public void run(SourceContext<Tuple4<Long, String, Integer, String>> sourceContext)
                            throws Exception {
                        sourceContext.collect(new Tuple4<>(1L, "100-200", 1, "2019-05-22 10:06:00"));
                        sourceContext.collect(new Tuple4<>(2L, "200-300", 1, "2019-05-22 10:06:00"));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        tableEnv.registerDataStream("test_table", ds, "a, b, c, d");

        tableEnv.registerFunction("udf_regexp_extract", new FlinkRegExpExtract());

        String sql = "SELECT a, udf_regexp_extract(b, '(\\d+)-(\\d+)', 2) as cc from test_table";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).map(new MapFunction<Row, Object>() {
            @Override
            public Object map(Row row) throws Exception {
                Long a = (Long) row.getField(0);
                String cc = (String) row.getField(1);
                if (1L == a) {
                    Assert.assertEquals(cc, "200");
                }
                if (2L == a) {
                    Assert.assertEquals(cc, "300");
                }
                return null;
            }
        });
        env.execute();
    }
}
