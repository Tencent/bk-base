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

package com.tencent.bk.base.dataflow.kafka.producer.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import com.tencent.bk.base.dataflow.topology.TestStreamTopology;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class TestAbstractAvroKeyedSerializationSchema {

    private TestAvroKeyedSerializationSchema testAvroKeyedSerializationSchema;

    @Before
    public void setUp() {
        testAvroKeyedSerializationSchema =
                new TestAvroKeyedSerializationSchema(TestStreamTopology.getTestStreamTopology(), getTestSinkNode());
    }

    @Test
    public void testSerializeValue() {
        // 构造数据
        MessageHead messageHead = new MessageHead();
        messageHead.setTag("local|avro|1621825380");
        List<Row> value = new ArrayList<>();
        value.add(Row.of(123, "abc", 1L, 2.1F, 3.4D, 1621829147));
        value.add(Row.of(234, "bcd", 2L, 3.1F, 4.4D, 1621829148));
        byte[] bytes = testAvroKeyedSerializationSchema.serializeValue(new Tuple2<>(messageHead, value));
        assertTrue(new String(bytes).contains("avro"));
        assertTrue(new String(bytes).contains("_metricTag_"));
    }

    @Test
    public void testGetTargetTopic() {
        String targetTopic = this.testAvroKeyedSerializationSchema.getTargetTopic(null);
        assertEquals("table_1_test_node_id", targetTopic);
    }

    /**
     * 获取测试的sink node
     * @return test sink node
     */
    public static SinkNode getTestSinkNode() {
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
        return spySinkNode;
    }

    public static class TestAvroKeyedSerializationSchema extends AbstractAvroKeyedSerializationSchema {

        public TestAvroKeyedSerializationSchema(StreamTopology topology,
                SinkNode node) {
            super(topology, node);
        }

        @Override
        public Object convertToAvroRecord(Schema schema, Schema recordSchema, List<Row> rows,
                List<NodeField> nodeFields, String metricTag) throws ParseException {
            GenericRecord msgRecord = new GenericData.Record(schema);
            List<GenericRecord> records = new ArrayList<>();
            for (Row row : rows) {
                GenericRecord avroRecord = new GenericData.Record(recordSchema);
                for (int i = 0; i < nodeFields.size(); i++) {
                    setNodeField(nodeFields, i, row, avroRecord, records);
                }
            }

            msgRecord.put("_tagTime_", System.currentTimeMillis() / 1000 / 60 * 60);
            msgRecord.put("_metricTag_", metricTag);
            Schema recordArrSchema = Schema.createArray(recordSchema);
            GenericArray arr = new GenericData.Array(100, recordArrSchema);
            for (GenericRecord record : records) {
                arr.add(record);
            }
            msgRecord.put("_value_", arr);

            return msgRecord;
        }

        private void setNodeField(
                List<NodeField> nodeFields,
                int fieldIndex,
                Row row,
                GenericRecord avroRecord,
                List<GenericRecord> records) {
            String type = nodeFields.get(fieldIndex).getType();
            String name = nodeFields.get(fieldIndex).getField();
            switch (name) {
                // 不存储系统字段
                case ConstantVar.BKDATA_PARTITION_OFFSET:
                    return;
                case ConstantVar.EVENT_TIMESTAMP:
                    //LOGGER.error("###############> " + row.getField(i).toString());
                    Long dtEventTimeStamp = Long.parseLong(row.getField(fieldIndex).toString());
                    String dtEventTime = this.dateFormat.format(new Date(dtEventTimeStamp));
                    String localTime = this.dateFormat.format(new Date(System.currentTimeMillis()));

                    avroRecord.put(ConstantVar.EVENT_TIME, dtEventTime);
                    avroRecord.put(ConstantVar.EVENT_TIMESTAMP, dtEventTimeStamp);
                    avroRecord.put(ConstantVar.LOCAL_TIME, localTime);
                    records.add(avroRecord);
                    break;
                default:
                    if (null == row.getField(fieldIndex)) {
                        avroRecord.put(name, null);
                    } else {
                        DataType dataType = DataType.valueOf(type.toUpperCase());
                        switch (dataType) {
                            case INT:
                                avroRecord.put(name, Integer.parseInt(row.getField(fieldIndex).toString()));
                                break;
                            case LONG:
                                avroRecord.put(name, Long.parseLong(row.getField(fieldIndex).toString()));
                                break;
                            case DOUBLE:
                                avroRecord.put(name, Double.parseDouble(row.getField(fieldIndex).toString()));
                                break;
                            case FLOAT:
                                avroRecord.put(name, Float.parseFloat(row.getField(fieldIndex).toString()));
                                break;
                            case STRING:
                                avroRecord.put(name, row.getField(fieldIndex).toString());
                                break;
                            default:
                                throw new RuntimeException("Currently we only support schemas of the following "
                                        + "form [int/long/double/float/string]. Given: " + type);
                        }
                    }
            }
        }

        @Override
        public byte[] serializeKey(Tuple2<MessageHead, List<Row>> element) {
            return new byte[0];
        }
    }
}
