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

package com.tencent.bk.base.dataflow.flink.streaming.transform.join;

import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.topology.TestFlinkStreamingTopology;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

public abstract class AbstractTestJoin {

    protected static final long joinWindowSize = 60L;
    protected static final String timeZone = "Asia/Shanghai";
    protected StreamExecutionEnvironment env;
    protected TransformNode transformNode;
    protected Map<String, DataStream> dataStreams = new HashMap<>();
    protected JoinInfo joinInfo;

    protected void open() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> firstDataStream = env.addSource(new FirstStreamSource())
                .assignTimestampsAndWatermarks(new MyWatermarkExtractor());

        DataStream<Row> secondDataStream = env.addSource(new SecondStreamSource())
                .assignTimestampsAndWatermarks(new MyWatermarkExtractor());

        String firstNodeId = "1_first";
        String secondNodeId = "1_second";
        dataStreams.put(firstNodeId, firstDataStream);
        dataStreams.put(secondNodeId, secondDataStream);

        FlinkStreamingTopology flinkStreamingTopology = TestFlinkStreamingTopology.getFlinkStreamingTopology();
        transformNode = flinkStreamingTopology.getTransformNodes().get("1_test_join_transform");

        String firstJoinKey = "first_key";
        String secondJoinKey = "second_key";
        int firstJoinIndex = transformNode.getParentNodes().get(firstNodeId).getFieldIndex(firstJoinKey);
        List<Integer> firstKeyIndexes = Collections.singletonList(firstJoinIndex);
        int secondJoinIndex = transformNode.getParentNodes().get(secondNodeId).getFieldIndex(secondJoinKey);
        List<Integer> secondKeyIndexes = Collections.singletonList(secondJoinIndex);
        joinInfo = new JoinInfo(dataStreams.get(firstNodeId), dataStreams.get(secondNodeId),
                firstKeyIndexes, secondKeyIndexes, firstNodeId, secondNodeId);
    }

    private static class FirstStreamSource implements SourceFunction<Row> {

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            sourceContext.collect(Row.of("2019-03-20 00:00:01", "0_1", "key1", "a1"));
            sourceContext.collect(Row.of("2019-03-20 00:00:02", "0_2", "key3", "a3"));
            sourceContext.collect(Row.of("2019-03-20 00:00:03", "0_3", "key5", "a5"));
        }

        @Override
        public void cancel() {

        }
    }

    private static class SecondStreamSource implements SourceFunction<Row> {

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            sourceContext.collect(Row.of("2019-03-20 00:00:01", "0_1", "key1", "b1"));
            sourceContext.collect(Row.of("2019-03-20 00:00:02", "0_2", "key2", "b2"));
            sourceContext.collect(Row.of("2019-03-20 00:00:03", "0_3", "key4", "b4"));
        }

        @Override
        public void cancel() {

        }
    }

    private static class MyWatermarkExtractor implements
            AssignerWithPeriodicWatermarks<Row> {

        private static final long serialVersionUID = 1L;

        private Long currentTimestamp = Long.MIN_VALUE;

        private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            {
                setTimeZone(TimeZone.getTimeZone("UTC"));
            }
        };

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
        public long extractTimestamp(Row element, long previousElementTimestamp) {
            Date date = null;
            try {
                date = utcFormat.parse(element.getField(0).toString());
            } catch (ParseException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            return date.getTime();
        }
    }

    protected static class CollectSink implements SinkFunction<Row> {
        // must be static
        public static final List<Row> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Row value) throws Exception {
            VALUES.add(value);
        }
    }
}
