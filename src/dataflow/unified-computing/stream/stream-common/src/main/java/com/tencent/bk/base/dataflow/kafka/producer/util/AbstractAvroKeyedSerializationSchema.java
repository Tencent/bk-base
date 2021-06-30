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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.JobRegister;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAvroKeyedSerializationSchema
        implements KeyedSerializationSchema<Tuple2<MessageHead, List<Row>>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAvroKeyedSerializationSchema.class);

    /**
     * utc 时间格式
     */
    public final SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("UTC"));
        }
    };
    /**
     * 上海时间格式
     */
    public SimpleDateFormat dateFormat;
    private String timeZone;
    private SinkNode node;

    public AbstractAvroKeyedSerializationSchema(StreamTopology topology, SinkNode node) {
        this.node = node;
        timeZone = topology.getTimeZone();
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            {
                setTimeZone(TimeZone.getTimeZone(timeZone));
            }
        };
        JobRegister.register(topology.getJobId());
    }

    @Override
    public byte[] serializeValue(Tuple2<MessageHead, List<Row>> value) {
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

    @Override
    public String getTargetTopic(Tuple2<MessageHead, List<Row>> value) {
        return NodeUtils.generateKafkaTopic(node.getNodeId());
    }

    /**
     * Converts a (nested) Flink Row into Avro's {@link GenericRecord}. Strings are converted into Avro's {@link Utf8}
     * fields.
     *
     * @param schema message schema
     * @param recordSchema record schema
     * @param rows rows
     * @param nodeFields rt fileds
     * @return output
     * @throws ParseException exception
     */
    public abstract Object convertToAvroRecord(Schema schema, Schema recordSchema, List<Row> rows,
            List<NodeField> nodeFields, String metricTag) throws ParseException;

    private List<Schema> prepareSchema(SinkNode node) {
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
            LOGGER.error("node schema is avro error!");
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

    private void setField(NodeField nodeField, List<Object> fieldsList) {

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

    private void generateSystemFieldSchema(List<Object> fieldsList) {
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
}
