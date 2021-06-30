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

package com.tencent.bk.base.dataflow.udt.flink.sql.tests;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestUdtf {

    /**
     * flink udtf 测试
     *
     * @param args 空
     * @throws Exception 测试异常
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Tuple4<Long, String, Integer, String>> ds = env
                .addSource(new SourceFunction<Tuple4<Long, String, Integer, String>>() {
                    @Override
                    public void run(SourceContext<Tuple4<Long, String, Integer, String>> sourceContext)
                            throws Exception {
                        sourceContext.collect(new Tuple4<>(1L, "a,aa", 1, "2019-05-22 10:06:00"));
                        sourceContext.collect(new Tuple4<>(1L, "b,bb", 2, "2019-05-22 10:06:04"));
                        sourceContext.collect(new Tuple4<>(1L, "bbb,bbbb", 2, "2019-05-22 10:06:05"));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        //tableEnv.registerFunction("split", new FlinkJavaTestUdtf());
        //tableEnv.registerFunction("split", new FlinkPyTestUdtf());
        tableEnv.registerDataStream("test_table", ds, "a, b, c, d");
        Table table = tableEnv
                .sqlQuery("select  word, length from test_table, lateral table(split(b, ',')) as T(word, length)");

        tableEnv.toAppendStream(table, Row.class).print();
        System.out.println("begin");
        env.execute();
    }
}
