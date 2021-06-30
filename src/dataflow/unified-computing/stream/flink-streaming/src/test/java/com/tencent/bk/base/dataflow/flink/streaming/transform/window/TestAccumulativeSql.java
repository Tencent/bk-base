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

package com.tencent.bk.base.dataflow.flink.streaming.transform.window;

import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.BkDataSideOutputProcess;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestAccumulativeSql {

    private static AtomicLong id = new AtomicLong(10000);

    @Test
    public void testAccumulative() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        DataStream<Tuple4<Long, String, Integer, Long>> ds = env
                .addSource(new CustomSourceFunction())
                .assignTimestampsAndWatermarks(new MyWatermarkExtractor());

        tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, proctime.proctime, rowtime.rowtime");

        Table table = tEnv.sqlQuery("\n"
                + "SELECT\n"
                + "  user,\n"
                + "  HOP_START(rowtime, INTERVAL '1' HOUR, INTERVAL '1' day) as wStart,\n"
                + "  SUM(amount)\n"
                + " FROM Orders\n"
                + " GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' day), user");

        BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig =
                new BkDataGroupWindowAggregateStreamQueryConfig();
        // 设置为累加计算
        streamQueryConfig.setAccumulate(true);
        // 设置上海时区偏移
        streamQueryConfig.setBkSqlWindowOffset(TimeZone.getTimeZone("Asia/Shanghai").getRawOffset());

        // 自定义允许延迟数据trigger
        streamQueryConfig.allowedLateness(Time.hours(1).toMilliseconds());
        streamQueryConfig.trigger(AllowedLatenessEventTimeTrigger.of(30, 1));

        // 设置输出延迟数据
        streamQueryConfig.sideOutputLateData("output tag test");
        streamQueryConfig.setSideOutputProcess(new BkDataSideOutputProcess() {
            private static final long serialVersionUID = 1L;

            @Override
            public void process(DataStream<CRow> dataStream, RowSchema inputSchema) {
                int rowTimeIdx = inputSchema.fieldNames().indexOf("rowtime");
                if (rowTimeIdx < 0) {
                    throw new TableException("Time attribute could not be found. This is a bug.");
                }
                dataStream.map(new MapFunction<CRow, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(CRow value) throws Exception {
                        Date rowTime = new Date(Long.valueOf(value.row().getField(rowTimeIdx).toString()));
                        String key = new SimpleDateFormat("yyyy-MM-dd HH:mm:00").format(rowTime);
                        return new Tuple2<>(key, 1L);
                    }
                }).returns(new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
                        .keyBy(0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                        .sum(1).print();
            }
        });

        // 设置自定义的查询配置
        DataStream<Row> result = tEnv.toAppendStream(table, Row.class, streamQueryConfig);

        DataStream<Row> sinkDataStream = result.map(new MapFunction<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row map(Row value) throws Exception {
                // UTC时间转当前时区显示
                String dt = dateFormat.format(utcFormat.parse(value.getField(1).toString()));
                value.setField(1, dt);
                return value;
            }
        });

        sinkDataStream.addSink(new CollectSink());

        env.execute("test-for-acc");
        assertTrue(CollectSink.VALUES.contains(Row.of(10000L, "2019-03-20 00:00:00", 12)));
        assertTrue(CollectSink.VALUES.contains(Row.of(10000L, "2019-03-20 00:00:00", 42)));
        assertTrue(CollectSink.VALUES.contains(Row.of(10000L, "2019-03-20 00:00:00", 63)));
        assertTrue(CollectSink.VALUES.contains(Row.of(10000L, "2019-03-20 00:00:00", 66)));
    }

    private static long getId() {
        return id.get();
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
                return new Watermark(currentTimestamp - 30 * 1000L);
            }
        }

        @Override
        public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
            this.currentTimestamp = element.f3;
            return this.currentTimestamp;
        }
    }

    private static class CollectSink implements SinkFunction<Row> {
        // must be static
        public static final List<Row> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Row value) throws Exception {
            VALUES.add(value);
        }
    }

    private static class CustomSourceFunction implements SourceFunction<Tuple4<Long, String, Integer, Long>> {

        private final SimpleDateFormat dateFormat;

        CustomSourceFunction() {
            this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }

        @Override
        public void run(SourceContext<Tuple4<Long, String, Integer, Long>> sourceContext) throws Exception {
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:01:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:02:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:30:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:04:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:06:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 07:00:00").getTime()));

            // delay time
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-19 01:30:00").getTime()));

            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 08:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 09:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 10:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 11:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 12:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 13:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 14:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:31").getTime()));
            sourceContext.collect(
                    new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 23:01:30").getTime()));
        }

        @Override
        public void cancel() {

        }
    }

}
