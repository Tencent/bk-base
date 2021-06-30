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

package com.tencent.bk.base.dataflow.flink.streaming.sink;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.kafka.producer.util.AbstractAvroKeyedSerializationSchema;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLAbstractAvroKeyedSerializationSchema extends AbstractAvroKeyedSerializationSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLAbstractAvroKeyedSerializationSchema.class);

    public SQLAbstractAvroKeyedSerializationSchema(FlinkStreamingTopology topology, SinkNode node) {
        super(topology, node);
    }

    @Override
    public byte[] serializeKey(Tuple2<MessageHead, List<Row>> value) {
        try {
            String messageKey = null;
            List<Row> rows = value.f1;
            for (Row row : rows) {
                if (row.getField(0) != null) {
                    Long dtEventTimeStamp = null;
                    try {
                        dtEventTimeStamp = utcFormat.parse(row.getField(0).toString()).getTime();
                    } catch (ParseException e) {
                        throw new RuntimeException("Failed to parse dtEventTimeStamp.", e);
                    }
                    String dtEventTime = dateFormat.format(new Date(dtEventTimeStamp));
                    Long minStr = Long.valueOf(dtEventTime.replaceAll("[- :]", "").substring(0, 12) + "00");
                    messageKey = String.valueOf(minStr) + System.currentTimeMillis();
                    break;
                }
            }
            if (messageKey != null) {
                return messageKey.getBytes("UTF8");
            }
            throw new RuntimeException("Failed to serialize kafka message key, and the message key is null.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize key.", e);
        }
    }

    /**
     * Converts a (nested) Flink Row into Avro's {@link GenericRecord}.
     * Strings are converted into Avro's {@link Utf8} fields.
     *
     * @param schema message schema
     * @param recordSchema record schema
     * @param rows rows
     * @param nodeFields rt fileds
     * @return output
     * @throws ParseException exception
     */
    @Override
    public Object convertToAvroRecord(Schema schema, Schema recordSchema, List<Row> rows, List<NodeField> nodeFields,
            String metricTag) throws ParseException {
        GenericRecord msgRecord = new GenericData.Record(schema);

        List<GenericRecord> records = new ArrayList<>();

        for (Row row : rows) {
            GenericRecord avroRecord = new GenericData.Record(recordSchema);
            for (int i = 0; i < nodeFields.size(); i++) {
                String type = nodeFields.get(i).getType();
                String name = nodeFields.get(i).getField();
                switch (name) {
                    // 不存储系统字段
                    case ConstantVar.BKDATA_PARTITION_OFFSET:
                        break;
                    case ConstantVar.EVENT_TIME:
                        Long dtEventTimeStamp = utcFormat.parse(row.getField(i).toString()).getTime();
                        String dtEventTime = dateFormat.format(new Date(dtEventTimeStamp));
                        String localTime = dateFormat.format(new Date(System.currentTimeMillis()));

                        avroRecord.put(ConstantVar.EVENT_TIME, dtEventTime);
                        avroRecord.put(ConstantVar.EVENT_TIMESTAMP, dtEventTimeStamp);
                        avroRecord.put(ConstantVar.LOCAL_TIME, localTime);
                        break;
                    case ConstantVar.WINDOW_START_TIME:
                    case ConstantVar.WINDOW_END_TIME:
                        avroRecord.put(name, row.getField(i) != null ? row.getField(i).toString() : "");
                        break;
                    default:
                        setUserField(i, row, avroRecord, name, type);
                }
            }
            records.add(avroRecord);
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

    private void setUserField(int fieldIndex, Row row, GenericRecord avroRecord, String name, String type) {
        if (fieldIndex > row.getArity() - 1 || null == row.getField(fieldIndex)) {
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
                    throw new RuntimeException(
                            "Currently we only support schemas of the following "
                                    + "form [int/long/double/float/string]. Given: "
                                    + type);

            }
        }
    }
}
