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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.types.AvroMessage;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析avro数据
 */
public final class AvroParser implements FlatMapFunction<AvroMessage, Tuple2<MessageHead, Row>>, CheckpointedFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroParser.class);

    /**
     * utc 时间格式
     */
    private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("UTC"));
        }
    };

    private Node node;
    private Topology topology;

    private DatumReader<GenericRecord> reader;
    private GenericRecord inputAvroRecord = null;
    private GenericArray avroValues;
    private Row sourceRow;
    private GenericRecord rec;
    private int fieldIndex;
    private String fieldType;
    private String fieldName;
    private Long dtEventTimeStamp = null;
    private String dtEventTime;


    public AvroParser(Node node, Topology topology) {
        this.node = node;
        this.topology = topology;
    }

    @Override
    public void flatMap(AvroMessage avroMessage, Collector<Tuple2<MessageHead, Row>> out)
            throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(
                new String(avroMessage.getMessage(), "utf8").getBytes("ISO-8859-1"));
        reader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> dataReader;
        try {
            dataReader = new DataFileStream<>(input, reader);
        } catch (IOException e) {
            String key = new String(avroMessage.getKey(), "utf8");
            String value = new String(avroMessage.getMessage(), "utf8");
            // 当kafka中数据的key以DatabusEvent开头时，这条数据为标记数据，不做数据处理
            if (key.startsWith("DatabusEvent") || ("".equals(key) && "".equals(value))) {
                try {
                    LOGGER.info(String.format("The %s value is empty, the key is %s,  and skip it.",
                            avroMessage.toString(),
                            new String(avroMessage.getKey(), "utf8")));
                } catch (Exception e1) {
                    LOGGER.warn("value is empty, and convert key failed.");
                }
                return;
            }
            LOGGER.error(MessageFormat.format(
                    "Convert avro data failed, and the key is {0}, "
                            + "topic is {1}, partition is {2}, offset is {3}",
                    key, avroMessage.getTopic(), avroMessage.getPartition(), avroMessage.getOffset()), e);
            throw new RuntimeException(e);
        }

        //GenericRecord inputAvroRecord = null;
        long tagTestTime = System.currentTimeMillis() / 60000 * 60;
        while (dataReader.hasNext()) {
            inputAvroRecord = dataReader.next(inputAvroRecord);
            String metricTag = inputAvroRecord.get("_metricTag_") == null ? ("local|avro|" + tagTestTime)
                    : inputAvroRecord.get("_metricTag_").toString();
            avroValues = (GenericArray) inputAvroRecord.get("_value_");
            if (null == avroValues) {
                throw new RuntimeException(MessageFormat
                        .format("The message value is null, and the topic is {0}, partition is {1}, offset is {2}",
                                avroMessage.getTopic(), avroMessage.getPartition(), avroMessage.getOffset()));
            }
            for (int i = 0; i < avroValues.size(); i++) {
                rec = (GenericRecord) avroValues.get(i);
                fieldIndex = 0;
                this.sourceRow = new Row(node.getFieldsSize());
                for (NodeField nodeField : node.getFields()) {
                    setField(nodeField, avroMessage, i);
                }

                MessageHead messageHead = new MessageHead();
                messageHead.setTag(metricTag);

                out.collect(new Tuple2<>(messageHead, sourceRow));
            }
        }
    }

    private void setField(NodeField nodeField, AvroMessage avroMessage, int avroIndex) {
        fieldType = nodeField.getType();
        fieldName = nodeField.getField();
        switch (nodeField.getField()) {
            case ConstantVar.EVENT_TIME:
                // 根据timestamp获取时间
                Object dtEventTimeStampObject = rec.get(ConstantVar.EVENT_TIMESTAMP);
                if (null == dtEventTimeStampObject) {
                    throw new RuntimeException(MessageFormat.format(
                            "Topic {0}, partition {1}, offset is {2} has data with empty dt event time",
                            avroMessage.getTopic(), avroMessage.getPartition(), avroMessage.getOffset()));
                }
                dtEventTimeStamp = Long.parseLong(dtEventTimeStampObject.toString());
                dtEventTime = utcFormat.format(new Date(dtEventTimeStamp));
                sourceRow.setField(fieldIndex, dtEventTime);
                break;
            case ConstantVar.BKDATA_PARTITION_OFFSET:
                //bkdata_par_offset字段内容变更： [avroIndex]_[partition]_[offset]_[topic]
                sourceRow.setField(fieldIndex, avroMessage.getBkdataPartitionOffsetField(avroIndex));
                break;
            default:
                Object obj = rec.get(fieldName);
                if (null == obj) {
                    sourceRow.setField(fieldIndex, null);
                } else {
                    switch (DataType.valueOf(fieldType.toUpperCase())) {
                        case INT:
                            sourceRow.setField(fieldIndex, Integer.parseInt(obj.toString()));
                            break;
                        case LONG:
                            sourceRow.setField(fieldIndex, Long.parseLong(obj.toString()));
                            break;
                        case DOUBLE:
                            sourceRow.setField(fieldIndex, Double.parseDouble(obj.toString()));
                            break;
                        case FLOAT:
                            sourceRow.setField(fieldIndex, Float.parseFloat(obj.toString()));
                            break;
                        case STRING:
                            sourceRow.setField(fieldIndex, obj.toString());
                            break;
                        default:
                            throw new IllegalArgumentException("Not support the filed type " + fieldType);
                    }
                }
        }
        fieldIndex++;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }


}