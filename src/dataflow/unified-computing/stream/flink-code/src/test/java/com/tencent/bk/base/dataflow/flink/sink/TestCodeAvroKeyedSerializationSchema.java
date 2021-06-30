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

package com.tencent.bk.base.dataflow.flink.sink;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.flink.topology.TestFlinkCodeTopology;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestCodeAvroKeyedSerializationSchema {

    public static byte[] generateTestAvroKey() {
        MessageHead messageHead = new MessageHead();
        messageHead.setTag("local|avro|1621825380");
        List<Row> value = new ArrayList<>();
        value.add(Row.of(123, "abc", 1621829147));
        value.add(Row.of(234, "bcd", 1621829148));

        CodeAvroKeyedSerializationSchema serializationSchema =
                new CodeAvroKeyedSerializationSchema(TestFlinkCodeTopology.getTestFlinkCodeTopology(), null);
        return serializationSchema.serializeKey(new Tuple2<>(messageHead, value));
    }

    @Test
    public void testSerializeKey() {
        byte[] bytes = generateTestAvroKey();
        assertTrue(new String(bytes).startsWith("19700120023000"));
    }

    /**
     * 生成单元测试使用的 avro value 数据
     *
     * @return avro 数据的 byte[] 形式
     */
    public static byte[] generateTestAvroValue() {
        // 构造sink node
        SinkNode sinkNode = new SinkNode();
        SinkNode spySinkNode = spy(sinkNode);
        Map<String, Object> info = ImmutableMap.<String, Object>builder()
                .put("id", "1_test_node_id")
                .put("name", "test_node_id")
                .put("output", ImmutableMap.<String, String>builder()
                        .put("cluster_port", "1")
                        .put("type", "kafka")
                        .put("cluster_domain", "cluster domain")
                        .build())
                .put("fields", ImmutableList.<Map<String, Object>>builder()
                        .add(ImmutableMap.of("field", "int_field", "type", "int", "origin", "",
                                "event_time", false))
                        .add(ImmutableMap.of("field", "string_field", "type", "string",
                                "origin", "",
                                "event_time", false))
                        .add(ImmutableMap.of("field", "long_field", "type", "long",
                                "origin", "",
                                "event_time", false))
                        .add(ImmutableMap.of("field", "float_field", "type", "float",
                                "origin", "",
                                "event_time", false))
                        .add(ImmutableMap.of("field", "double_field", "type", "double",
                                "origin", "",
                                "event_time", false))
                        .add(ImmutableMap.of("field", "dtEventTimeStamp", "type", "long",
                                "origin", "",
                                "event_time", true))
                        .build()
                )
                .build();
        spySinkNode.map(info, Component.flink);

        // 构造数据
        MessageHead messageHead = new MessageHead();
        messageHead.setTag("local|avro|1621825380");
        List<Row> value = new ArrayList<>();
        value.add(Row.of(123, "abc", 1L, 2.1F, 3.4D, 1621829147));
        value.add(Row.of(234, "bcd", 2L, 3.1F, 4.4D, 1621829148));
        CodeAvroKeyedSerializationSchema codeAvroKeyedSerializationSchema =
                new CodeAvroKeyedSerializationSchema(TestFlinkCodeTopology.getTestFlinkCodeTopology(), spySinkNode);
        return codeAvroKeyedSerializationSchema.serializeValue(new Tuple2<>(messageHead, value));
    }

    @Test
    public void testSerializeValue() {
        byte[] bytes = generateTestAvroValue();
        assertTrue(new String(bytes).contains("avro"));
    }

}
