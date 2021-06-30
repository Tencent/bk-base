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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite;

import static com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite.TestIgniteStaticJoin.spyNode;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

public class TestIgniteAsyncStaticJoinOperator {

    protected static CountDownLatch latch = new CountDownLatch(2);

    @Test
    public void testIgniteAsyncStaticJoinOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure your test environment
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(500);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        // values are collected in a static variable
        TestIgniteAsyncStaticJoinOperator.CollectSink.VALUES.clear();
        // spy operator params

        FlinkStreamingTopology topo = Mockito.mock(FlinkStreamingTopology.class, Mockito.withSettings().serializable());
        Mockito.when(topo.getTimeZone()).thenReturn("Asia/Shanghai");

        // prepare mock res data
        Map<String, String> staticRes1 = Maps.newHashMap();
        staticRes1.put("province", "province1");
        staticRes1.put("city", "city1");
        Map<String, String> staticRes2 = Maps.newHashMap();
        staticRes2.put("province", "province2");
        staticRes2.put("city", "city2");
        Map<String, Map> result = Maps.newHashMap();
        result.put("127.0.0.1,2020-12-01 10:00:00", staticRes1);
        result.put("127.0.0.1,2020-12-02 11:00:00", staticRes2);
        // spy loadBatchFromIgnite
        TransformNode spyNode = spyNode();
        Integer[] arrKeyIndexs = {1, 2};
        List<Integer> streamKeyIndexs = Arrays.asList(arrKeyIndexs);
        IgniteClusterInfo igniteClusterInfo = new IgniteClusterInfo("testCacheName", "testIgniteHost",
                80, "testIgnitePassword", "testIgniteUser", "testIgniteCluster");
        IgniteAsyncStaticJoinTestOperator operator = new IgniteAsyncStaticJoinTestOperator(spyNode, topo,
                "left", streamKeyIndexs, ",", 3, igniteClusterInfo);
        IgniteAsyncStaticJoinTestOperator spyOperator = Mockito.mock(IgniteAsyncStaticJoinTestOperator.class,
                Mockito.withSettings().spiedInstance(operator)
                        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                        .serializable());
        Mockito.doReturn(result).when(spyOperator).loadBatchFromIgnite(Mockito.any());
        Mockito.doNothing().when(spyOperator).initIgnite();

        // create a stream of custom elements and apply transformations
        List<Row> rows = Lists.newArrayList();
        rows.add(Row.of(1603342237000L, "127.0.0.1", "2020-12-01 10:00:00"));
        rows.add(Row.of(1603342238000L, "127.0.0.1", "2020-12-02 11:00:00"));
        List<Row> rows2 = Lists.newArrayList();
        rows2.add(Row.of(1603342237000L, "127.0.0.1", "2020-12-01 10:00:00"));
        rows2.add(Row.of(1603342238000L, "127.0.0.1", "2020-12-02 11:00:00"));
        DataStream<List<Row>> sourceStream = env.fromElements(rows, rows2);
        int timeout = 10;
        RowTypeInfo igniteOpTypeInfo = new RowTypeInfo(TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class));
        ListTypeInfo igniteOpListTypeInfo = new ListTypeInfo<Row>(igniteOpTypeInfo);
        DataStream asyncStream = AsyncDataStream
                .unorderedWait(sourceStream, spyOperator, timeout, TimeUnit.SECONDS, 3)
                .name("IgniteAsyncOperator")
                .returns(igniteOpListTypeInfo);

        asyncStream.addSink(new TestIgniteAsyncStaticJoinOperator.CollectSink());
        // execute
        env.execute();
        try {
            latch.await(); // 主线程等待
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        // verify your results
        List<Row> expect = Lists.newArrayList();
        expect.add(Row.of(1603342237000L, "127.0.0.1", "2020-12-01 10:00:00", "province1", "city1"));
        expect.add(Row.of(1603342238000L, "127.0.0.1", "2020-12-02 11:00:00", "province2", "city2"));
        List<Row> actual = TestIgniteAsyncStaticJoinOperator.CollectSink.VALUES.get(0);
        assertEquals(expect, actual);
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<List<Row>> {

        // must be static
        public static final List<List<Row>> VALUES = Lists.newArrayList();

        @Override
        public synchronized void invoke(List<Row> value) throws Exception {
            VALUES.add(value);
        }
    }
}