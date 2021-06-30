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

package com.tencent.bk.base.dataflow.flink.streaming.transform;

import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.flink.streaming.transform.window.ProcessingTimeAndEventTimeTrigger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestProcessingTimeAndEventTimeTrigger {

    /**
     * 使用main函数测试，因为测试过程比较耗时，不在maven编译时测试
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(4);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        DataStream<Tuple4<Long, String, Integer, Long>> ds = env
                .addSource(new CustomSourceFunction())
                .assignTimestampsAndWatermarks(new MyWatermarkExtractor());
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, proctime.proctime, rowtime.rowtime");

        Table table = tEnv.sqlQuery("\n"
                + "SELECT\n"
                + "  user,\n"
                + "  SESSION_START(rowtime, INTERVAL '3' second) as wStart,\n"
                + "  SESSION_END(rowtime, INTERVAL '3' second) as wEnd,\n"
                + "  SUM(amount)\n"
                + " FROM Orders\n"
                + " GROUP BY SESSION(rowtime, INTERVAL '3' second), user");

        BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig =
                new BkDataGroupWindowAggregateStreamQueryConfig();

        // 自定义触发器实现混合窗口计算
        ProcessingTimeAndEventTimeTrigger trigger = ProcessingTimeAndEventTimeTrigger.of(Time.seconds(70));
        streamQueryConfig.trigger(trigger);

        DataStream<Row> result = tEnv.toAppendStream(table, Row.class, streamQueryConfig);
        DataStream<String> sinkDataStream = result.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                return row.toString();
            }
        });
        sinkDataStream.addSink(new CollectSink());

        env.execute("test-for-session-trigger");
        assertTrue(CollectSink.VALUES.contains("2,2018-12-31 16:00:03.0,2018-12-31 16:00:33.0,10"));
        assertTrue(CollectSink.VALUES.contains("2,2018-12-31 16:01:35.0,2018-12-31 16:01:40.0,2"));
        assertTrue(CollectSink.VALUES.contains("5,2018-12-31 16:01:40.0,2018-12-31 16:01:47.0,3"));
        assertTrue(CollectSink.VALUES.contains("3,2018-12-31 16:01:35.0,2018-12-31 16:01:38.0,1"));
        assertTrue(CollectSink.VALUES.contains("4,2018-12-31 16:01:37.0,2018-12-31 16:01:43.0,2"));
    }

    private static String nowStr() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    private static class MyWatermarkExtractor implements
            AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Long>> {

        private static final long serialVersionUID = 1L;

        private Long currentTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            if (currentTimestamp == Long.MIN_VALUE) {
                return new Watermark(Long.MIN_VALUE);
            } else {
                return new Watermark(currentTimestamp - 5000L);
            }
        }

        @Override
        public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
            this.currentTimestamp = element.f3;
            return this.currentTimestamp;
        }
    }

    private static class CustomSourceFunction implements SourceFunction<Tuple4<Long, String, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final SimpleDateFormat dateFormat;

        CustomSourceFunction() {
            this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }

        @Override
        public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
            long dataTime = dateFormat.parse("2019-01-01 00:00:00").getTime();
            for (int i = 0; i < 10; i++) {
                dataTime = dataTime + 3 * 1000L;
                ctx.collect(new Tuple4<>(2L, "rubber", 1, dataTime));
                Thread.sleep(100);
            }

            dataTime = dataTime + 65 * 1000L;
            // 窗口创建
            ctx.collect(new Tuple4<>(2L, "rubber", 1, dataTime));
            // 输出累计10条记录
            ctx.collect(new Tuple4<>(3L, "rubber", 1, dataTime));
            // 窗口加长10秒
            Thread.sleep(2 * 1000);
            dataTime = dataTime + 2 * 1000L;
            ctx.collect(new Tuple4<>(2L, "rubber", 1, dataTime));
            Thread.sleep(4 * 1000 - 100);
            // 输出超时数据

            // 窗口创建1
            ctx.collect(new Tuple4<>(4L, "rubber", 1, dataTime));
            // 窗口加长60秒（窗口会定时器会扩大60秒）
            Thread.sleep(3 * 1000);
            dataTime = dataTime + 3 * 1000L;
            ctx.collect(new Tuple4<>(4L, "rubber", 1, dataTime));
            Thread.sleep(4 * 1000 - 100);
            // 输出扩大窗口超时数据
            Thread.sleep(1000);
            // 窗口合并测试
            // 窗口创建2
            ctx.collect(new Tuple4<>(5L, "rubber", 1, dataTime));
            Thread.sleep(4 * 1000);
            dataTime = dataTime + 4 * 1000L;
            ctx.collect(new Tuple4<>(5L, "rubber", 1, dataTime));
            dataTime = dataTime - 3 * 1000L;
            ctx.collect(new Tuple4<>(5L, "rubber", 1, dataTime));
            Thread.sleep(4 * 1000 - 100);
            // 输出窗口合并超时数据
        }

        @Override
        public void cancel() {
        }
    }

    private static class CollectSink implements SinkFunction<String> {
        // must be static
        public static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value) throws Exception {
            VALUES.add(value);
        }
    }
}
