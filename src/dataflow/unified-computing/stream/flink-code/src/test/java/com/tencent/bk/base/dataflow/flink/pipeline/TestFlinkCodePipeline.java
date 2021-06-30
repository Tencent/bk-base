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

package com.tencent.bk.base.dataflow.flink.pipeline;

import static org.junit.Assert.assertEquals;

import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology;
import com.tencent.bk.base.dataflow.flink.topology.TestFlinkCodeTopology;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestFlinkCodePipeline {

    @Test
    public void testTransform() throws Exception {
        FlinkCodeTopology testFlinkCodeTopology = TestFlinkCodeTopology.getTestFlinkCodeTopology();
        FlinkCodePipeline flinkCodePipeline = new FlinkCodePipeline(testFlinkCodeTopology);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(new TypeInformation<?>[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO},
                new String[]{"int_field", "string_field", "long_field", "float_field", "double_field"});

        flinkCodePipeline.dataStreams.put("1_test_source",
                flinkCodePipeline.getRuntime().getEnv()
                        .addSource(new CustomSourceFunction())
                        .returns(rowTypeInfo));
        flinkCodePipeline.transform();
        Map<String, DataStream<Row>> dataStreams = flinkCodePipeline.dataStreams;
        dataStreams.get("1_test_code_transform").addSink(new CollectSink());
        flinkCodePipeline.getRuntime().getEnv().execute();
        assertEquals("[1,sss-args0--args1,1621912355000,1.1,2.2,1621912355000,"
                + " 2,aaa-args0--args1,1621912335000,3.3,4.5,1621912335000]", CollectSink.VALUES.toString());
    }

    private static class CollectSink implements SinkFunction<Row> {
        // must be static
        public static final List<Row> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Row value) throws Exception {
            VALUES.add(value);
        }
    }


    private static class CustomSourceFunction implements SourceFunction<Row> {
        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            sourceContext.collect(Row.of(1, "sss", 1621912355000L, 1.1F, 2.2D));
            Thread.sleep(100);
            sourceContext.collect(Row.of(2, "aaa", 1621912335000L, 3.3F, 4.5D));
        }

        @Override
        public void cancel() {

        }
    }
}
