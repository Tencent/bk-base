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

package com.tencent.bk.base.dataflow.kafka.consumer.parser;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.types.AvroMessage;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestAvroParser {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }
    };

    @Test
    public void testFlatMap() throws IOException {
        // 构造avro message
        AvroMessage avroMessage = new AvroMessage("table_1_test_node_id", 0, 100,
                generateTestAvroKey(),
                generateTestAvroValue());
        SourceNode sourceNode = new SourceNode();
        SourceNode spySourceNode = spy(sourceNode);
        Map<String, Object> info = ImmutableMap.<String, Object>builder()
                .put("id", "1_test_node_id")
                .put("name", "test_node_id")
                .put("input", ImmutableMap.<String, String>builder()
                        .put("cluster_port", "1")
                        .put("type", "kafka")
                        .put("cluster_domain", "cluster domain")
                        .build())
                .put("fields", ImmutableList.<Map<String, Object>>builder()
                        .add(ImmutableMap.of("field", "dtEventTime", "type", "string",
                                "origin", ""))
                        .add(ImmutableMap.of("field", "bkdata_par_offset", "type", "string",
                                "origin", ""))
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
                        .build()
                )
                .build();
        spySourceNode.map(info, Component.flink);
        List<Tuple2<MessageHead, Row>> out = new ArrayList<>();
        ListCollector<Tuple2<MessageHead, Row>> listCollector = new ListCollector<>(out);
        new AvroParser(spySourceNode, null).flatMap(avroMessage, listCollector);
        assertEquals("local|avro|1621825380", out.get(0).f0.getTag());
        assertEquals("local|avro|1621825380", out.get(1).f0.getTag());

        assertEquals("2021-05-24 03:01:47,0_0_100_table_1_test_node_id,123,abc,1,2.1,3.4", out.get(0).f1.toString());
        assertEquals("2021-05-24 03:01:47,1_0_100_table_1_test_node_id,234,null,2,3.1,4.4", out.get(1).f1.toString());
    }

    private static byte[] generateTestAvroKey() {
        MessageHead messageHead = new MessageHead();
        messageHead.setTag("local|avro|1621825380");
        List<Row> value = new ArrayList<>();
        value.add(Row.of(123, "abc", 1621829147));
        value.add(Row.of(234, "bcd", 1621829148));
        return serializeKeyForTest(new Tuple2<>(messageHead, value));
    }

    private static byte[] serializeKeyForTest(Tuple2<MessageHead, List<Row>> value) {

        List<Row> rows = value.f1;
        for (Row row : rows) {
            if (row.getField(row.getArity() - 1) != null) {
                Long dtEventTimeStamp = Long.parseLong(row.getField(row.getArity() - 1).toString());
                String dtEventTime = dateFormat.format(new Date(dtEventTimeStamp));
                Long minStr = Long.valueOf(dtEventTime.replaceAll("[- :]", "").substring(0, 12) + "00");
                try {
                    return (String.valueOf(minStr) + System.currentTimeMillis()).getBytes("UTF8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Failed to serialize key.", e);
                }
            }
        }
        throw new RuntimeException();
    }

    private static byte[] generateTestAvroValue() {
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
        value.add(Row.of(123, "abc", 1L, 2.1F, 3.4D, 1621825307000L));
        value.add(Row.of(234, null, 2L, 3.1F, 4.4D, 1621825307000L));
        return serializeValueForTest(new Tuple2<>(messageHead, value), spySinkNode);
    }

    private static byte[] serializeValueForTest(Tuple2<MessageHead, List<Row>> value, SinkNode node) {
        List<Row> rows = value.f1;
        try {
            DatumWriter<GenericRecord> datumWriter;
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream(524288);
            //Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);

            List<Schema> schemas = prepareSchema(node);
            //convert to record
            final Object record = convertToAvroRecord(schemas.get(1), schemas.get(0), rows, node.getFields(),
                    value.f0.getTag());

            datumWriter = new SpecificDatumWriter<>(schemas.get(1));
            // write
            //arrayOutputStream.reset();
            //datumWriter.write((GenericRecord) record, encoder);
            //encoder.flush();
            ////
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.setCodec(CodecFactory.snappyCodec());
            dataFileWriter.create(schemas.get(1), arrayOutputStream);
            dataFileWriter.append((GenericRecord) record);
            dataFileWriter.flush();
            dataFileWriter.close();

            return arrayOutputStream.toString("ISO-8859-1").getBytes("UTF8");
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Row.", e);
        }
    }

    private static List<Schema> prepareSchema(SinkNode node) {
        Map<String, Object> schemaMap = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        // 根据rt 获取对应的avro schema
        schemaMap.put("type", "record");
        schemaMap.put("name", "kafka");
        List<Object> fieldsList = new ArrayList<>();
        for (NodeField nodeField : node.getFields()) {
            setField(nodeField, fieldsList);
        }
        generateSystemFieldSchema(fieldsList);
        schemaMap.put("fields", fieldsList);
        String schemaStr = null;
        try {
            schemaStr = objectMapper.writeValueAsString(schemaMap);
        } catch (Exception e) {
            throw new RuntimeException(MessageFormat.format("Convert node {0} to avro failed.", node.getNodeId()), e);
        }
        Schema recordSchema = new Schema.Parser().parse(schemaStr);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_" + node.getNodeName())
                .fields()
                .name("_metricTag_").type().stringType().stringDefault("")
                .name("_tagTime_").type().longType().longDefault(0)
                .name("_value_").type().array().items(recordSchema).noDefault()
                .endRecord();
        List<Schema> schemas = new ArrayList<>();
        schemas.add(recordSchema);
        schemas.add(msgSchema);
        return schemas;
    }

    private static void setField(NodeField nodeField, List<Object> fieldsList) {

        String name = nodeField.getField();
        String type = nodeField.getType();
        if ("text".equals(type)) {
            type = "string";
        }
        if ("short".equals(type)) {
            type = "int";
        }
        String isNull = "null";
        if (ConstantVar.BKDATA_PARTITION_OFFSET.equals(name) || "timestamp".equals(name)) {
            return;
        }
        if (ConstantVar.EVENT_TIMESTAMP.equals(name) || ConstantVar.EVENT_TIME.equals(name)
                || ConstantVar.LOCAL_TIME.equals(name)) {
            return;
        }
        Map<String, Object> fieldMap = new HashMap<String, Object>();
        List<String> fieldTypeList = new ArrayList<String>();
        fieldTypeList.add(0, isNull);
        fieldTypeList.add(1, type);
        fieldMap.put("type", fieldTypeList);
        fieldMap.put("name", name);
        fieldsList.add(fieldMap);
    }

    private static void generateSystemFieldSchema(List<Object> fieldsList) {
        final ArrayList<String> listTmpStr = new ArrayList<String>() {
            {
                add(0, "null");
                add(1, "string");
            }
        };
        final ArrayList<String> listTmpLong = new ArrayList<String>() {
            {
                add(0, "null");
                add(1, "long");
            }
        };
        fieldsList.add(new HashMap<String, Object>() {
            {
                put("name", ConstantVar.EVENT_TIMESTAMP);
                put("type", listTmpLong);
            }
        });
        fieldsList.add(new HashMap<String, Object>() {
            {
                put("name", ConstantVar.EVENT_TIME);
                put("type", listTmpStr);
            }
        });
        fieldsList.add(new HashMap<String, Object>() {
            {
                put("name", ConstantVar.LOCAL_TIME);
                put("type", listTmpStr);
            }
        });
    }

    private static Object convertToAvroRecord(Schema schema, Schema recordSchema,
            List<Row> rows, List<NodeField> nodeFields,
            String metricTag) throws ParseException {
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

    private static void setNodeField(
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
                String dtEventTime = dateFormat.format(new Date(dtEventTimeStamp));
                String localTime = dateFormat.format(new Date(System.currentTimeMillis()));

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
}
