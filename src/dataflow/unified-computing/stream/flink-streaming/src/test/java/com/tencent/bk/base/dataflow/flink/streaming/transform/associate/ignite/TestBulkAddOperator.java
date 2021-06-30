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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.util.List;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBulkAddOperator {

    @Test
    public void testBulkAddOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure your test environment
        env.setParallelism(1);
        env.enableCheckpointing(500);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        // values are collected in a static variable
        CollectSink.VALUES.clear();
        // spy operator params
        TransformNode node = Mockito.mock(TransformNode.class, Mockito.withSettings().serializable());
        Mockito.when(node.getNodeId()).thenReturn("testNodeId");
        FlinkStreamingTopology topo = Mockito.mock(FlinkStreamingTopology.class, Mockito.withSettings().serializable());
        Mockito.when(topo.getRunMode()).thenReturn(RunMode.debug.toString());
        // spy bulk operator
        BulkAddOperator operator = new BulkAddOperator(node, topo);
        BulkAddOperator spyOperator = Mockito.mock(BulkAddOperator.class, Mockito.withSettings()
                .spiedInstance(operator)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .serializable());
        Mockito.when(spyOperator.getBatchSize()).thenReturn(2);

        RowTypeInfo igniteOpTypeInfo = new RowTypeInfo(TypeInformation.of(Long.class), TypeInformation.of(Long.class),
                TypeInformation.of(Long.class));
        ListTypeInfo igniteOpListTypeInfo = new ListTypeInfo<Row>(igniteOpTypeInfo);
        // create a stream of custom elements and apply transformations
        env.fromElements(Row.of(1L, 2L, 3L), Row.of(4L, 5L, 6L), Row.of(7L, 8L, 9L))
                .transform("BulkOperator", igniteOpListTypeInfo, spyOperator)
                .addSink(new CollectSink());
        // execute
        env.execute();
        // verify your results
        assertEquals(Lists.newArrayList(Row.of(1L, 2L, 3L), Row.of(4L, 5L, 6L)), CollectSink.VALUES.get(0));
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