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

import com.tencent.bk.base.dataflow.udf.UdfRegister;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class TestUdaf {

    /**
     * flink udaf 测试
     *
     * @param args null
     * @throws Exception 测试异常
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        DataStream<Tuple4<Long, String, Integer, String>> ds = env
                .addSource(new SourceFunction<Tuple4<Long, String, Integer, String>>() {
                    @Override
                    public void run(SourceContext<Tuple4<Long, String, Integer, String>> sourceContext)
                            throws Exception {
                        sourceContext.collect(new Tuple4<>(2L, "a,aa", 6, "2019-05-22 10:06:00"));
                        sourceContext.collect(new Tuple4<>(3L, "b,bb", 7, "2019-05-22 10:06:00"));
                        sourceContext.collect(new Tuple4<>(4L, "bbb,bbbb", 28, "2019-05-22 10:06:00"));
                        sourceContext.collect(new Tuple4<>(4L, "bbb,bbbb", 28, "2019-05-22 10:08:00"));
                    }

                    @Override
                    public void cancel() {

                    }
                }).assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, String>>() {
                            /**
                             * utc 时间格式
                             */
                            private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
                                {
                                    setTimeZone(TimeZone.getTimeZone("UTC"));
                                }
                            };

                            private Long currentMaxTimestamp = 0L;
                            private Watermark watermark = null;
                            //
                            private Long lastTick = 0L;
                            private Long currentTickMinTimestamp = 0L;
                            private Long rowCount = 0L;

                            @Override
                            public Watermark getCurrentWatermark() {
                                watermark = new Watermark(currentMaxTimestamp - 0);
                                return watermark;
                            }

                            @Override
                            public long extractTimestamp(Tuple4<Long, String, Integer, String> element, long l) {
                                try {
                                    rowCount++;
                                    Date date = utcFormat.parse(element.f3.toString());
                                    Long timestamp = date.getTime();

                                    long now = System.currentTimeMillis();
                                    // 增加数据时间最多比本地时间大于60s
                                    if ((timestamp - now) < 60000) {
                                        if ((now - lastTick) > 1000 || rowCount > 1000) {
                                            // 保证Watermark只增，一秒钟或者1000条更新一次Watermark。
                                            currentMaxTimestamp = Math
                                                    .max(currentTickMinTimestamp, currentMaxTimestamp);
                                            // 一秒钟重置当前Tick最小时间
                                            currentTickMinTimestamp = timestamp;
                                            // 重置计数
                                            rowCount = 0L;
                                            // 重置上一次更新时间
                                            lastTick = now;
                                        } else {
                                            currentTickMinTimestamp = Math.min(currentTickMinTimestamp, timestamp);
                                        }
                                        return timestamp;
                                    } else {
                                        // 未来时间数据
                                        return 0;
                                    }
                                } catch (ParseException e) {
                                    throw new RuntimeException("Parse time stamp failed.");
                                }
                            }
                        });

        // tEnv.registerFunction("udaf", new FlinkJavaTestUdaf());
        // tEnv.registerFunction("udaf", );
        registerFunction(tEnv);
        tEnv.registerDataStream("test_table", ds, "a, b, c, d, rowtime.rowtime");
        Table table = tEnv.sqlQuery(
                "select TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), udaf(a, c) as ccc "
                        + "from test_table group by TUMBLE(`rowtime`, INTERVAL '1' SECOND)");

        tEnv.toAppendStream(table, Row.class).print();
        System.out.println("begin");
        env.execute();
    }

    public static void registerFunction(StreamTableEnvironment tableEnvironment) {
        Object udfInstance = UdfRegister.getUdfInstance("stream", "test_udaf", "python");
        if (udfInstance instanceof AggregateFunction) {
            tableEnvironment.registerFunction("udaf", (AggregateFunction) udfInstance);
        }
    }
}
