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

package com.tencent.bk.base.datahub.databus.commons.utils;

import com.tencent.bk.base.datahub.databus.commons.Consts;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TransformUtils {

    private static final Logger log = LoggerFactory.getLogger(TransformUtils.class);

    public static final int DEFAULT_DECIMAL_SCALE = 6;
    public static final int DEFAULT_PRECISION = 50;


    /**
     * 根据清洗配置中包含的字段列表生成avro格式的schema字符串，并返回。
     *
     * @param columns 字段列表
     * @return avro schema字符串
     */
    public static String getAvroSchema(Map<String, String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\", \"name\":\"msg\", \"fields\":[{\"name\": \"localTime\", \"type\":\"string\"}");
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            if (null == entry.getValue()) {
                continue;
            }
            String colType = entry.getValue().toLowerCase();
            // 数据可以为null，在字段schema的type里指定为union类型，包含null值
            sb.append(",{\"name\":\"").append(entry.getKey()).append("\", \"type\":[\"null\",");
            // 目前系统仅仅支持 int/long/double/string/text,其中avro中string和text都属于string类型
            if (colType.equals("int") || colType.equals("long") || colType.equals("double")) {
                sb.append("\"").append(colType).append("\"]}");
            } else if (colType.equals("timestamp")) {
                sb.append("\"int\"]}");
            } else if (colType.equals("bigint")) {
                sb.append("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": ")
                        .append(DEFAULT_PRECISION).append(", \"scale\": 0}]}");
            } else if (colType.equals("bigdecimal")) {
                sb.append("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": ")
                        .append(DEFAULT_PRECISION).append(", \"scale\": ").append(DEFAULT_DECIMAL_SCALE).append("}]}");
            } else {
                sb.append("\"string\"]}");
            }
        }
        sb.append(
                ",{\"name\": \"dtEventTime\", \"type\":\"string\"},{\"name\": \"dtEventTimeStamp\", "
                        + "\"type\":\"long\"} ]}");
        String schema = sb.toString();
        LogUtils.debug(log, "avro schema: {}", schema);

        return schema;
    }

    /**
     * 将avro record对象转换为二进制数据，用string表示
     *
     * @return 字符串，编码为ISO-8859-1
     */
    public static String getAvroBinaryString(GenericRecord record, DataFileWriter<GenericRecord> dataFileWriter) {
        String result = "";
        try {
            // 将avro格式记录转换为String  todo 为啥设置5KB大小
            ByteArrayOutputStream output = new ByteArrayOutputStream(5120); // 5KB
            dataFileWriter.create(record.getSchema(), output);
            dataFileWriter.append(record);
            dataFileWriter.flush();

            //这里需要注意，avro格式二进制数据必须使用ISO-8859-1编码转换为String
            result = output.toString(StandardCharsets.ISO_8859_1.toString());
        } catch (IOException | AvroRuntimeException e) {
            // 处理avro格式的数据时，发生异常，将数据写入到日志中
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_AVRO_DATA,
                    "convert avro record to binary data failed! " + record.toString(), e);
        } finally {
            try {
                dataFileWriter.close();
            } catch (Exception ignore) {
                // ignore
            }
        }

        return result;
    }

    /**
     * 构建Avro记录的数组
     *
     * @param tagTime 时间戳
     * @param records 记录对象列表
     * @param colsInOrder 书序排列的字段名称列表
     * @param recordSchema 单条记录的schema
     * @param recordArrSchema 数组的schema
     * @param dteventtimeDateFormat 时间格式化对象
     * @return avro记录的数组
     */
    public static GenericArray<GenericRecord> composeAvroRecordArr(long tagTime, List<List<Object>> records,
            String[] colsInOrder, Schema recordSchema, Schema recordArrSchema, SimpleDateFormat dteventtimeDateFormat) {
        GenericArray<GenericRecord> arr = new GenericData.Array<>(records.size(), recordArrSchema);

        // todo 清洗出来的记录,一个包中的多条record的时间是否在同一分钟内??
        for (List<Object> value : records) {
            GenericRecord avroRecord = new GenericData.Record(recordSchema);
            for (int i = 0; i < colsInOrder.length; i++) {
                String col = colsInOrder[i];
                try {
                    // 构造dtEventTimeStamp、dtEventTime、localTime等字段
                    if (col.equals(Consts.TIMESTAMP)) {
                        long tsSec = (long) value.get(i);
                        avroRecord.put(Consts.DTEVENTTIME, dteventtimeDateFormat.format(new Date(tsSec * 1000)));
                        avroRecord.put(Consts.DTEVENTTIMESTAMP, tsSec * 1000);
                        avroRecord.put(Consts.LOCALTIME, dteventtimeDateFormat.format(new Date(tagTime * 1000)));
                    }
                    // 根据数据的类型将其转换为不同的类型
                    if (value.get(i) instanceof BigDecimal) {
                        BigDecimal bigDecimal = (BigDecimal) value.get(i);
                        bigDecimal = bigDecimal.setScale(DEFAULT_DECIMAL_SCALE, BigDecimal.ROUND_HALF_UP);
                        avroRecord.put(col, ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()));
                    } else if (value.get(i) instanceof BigInteger) {
                        BigInteger bigInteger = (BigInteger) value.get(i);
                        avroRecord.put(col, ByteBuffer.wrap(bigInteger.toByteArray()));
                    } else {
                        avroRecord.put(col, value.get(i));
                    }

                } catch (AvroRuntimeException ignore) {
                    // 字段和schema不匹配，或者设置值的时候发生异常，记录一下，不影响整体流程
                    LogUtils.warn(log, "failed to set value for avro record! name: {} value: {} schema: {} errMsg: {}",
                            col, value.get(i), avroRecord.getSchema().toString(false), ignore.getMessage());
                }
            }
            arr.add(avroRecord);
        }

        return arr;
    }

    /**
     * 从指定kafka,topic,partition的位置拉取最后一条数据
     */
    public static Long getLastCheckpoint(String bootStrapServer, String topic, int partition) {
        Long result = null;
        Map<String, String> map = getLastCheckpointKey(bootStrapServer, topic, partition);
        if (map.containsKey(Consts.OFFSET)) {
            result = Long.parseLong(map.get(Consts.OFFSET)) + 1;
        }
        return result;
    }

    /**
     * 从指定kafka,topic,partition的位置拉取最后一条数据
     */
    public static Map<String, String> getLastCheckpointKey(String bootStrapServer, String topic, int partition) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServer);
        props.put("group.id", "lastMsgFetcher");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(tp));
        consumer.seekToEnd(Arrays.asList(tp));

        //seekToEnd is lazy, the callling of position is used to force it xian
        long offset = consumer.position(tp);
        if (offset - 1 >= 0) {
            consumer.seek(tp, offset - 1);

            ConsumerRecords<String, String> records = consumer.poll(100);
            ConsumerRecord<String, String> lastRecord = null;
            for (ConsumerRecord<String, String> record : records) {
                lastRecord = record;
            }
            if (lastRecord != null) {
                // 兼容key为null的场景
                String key = lastRecord.key() == null ? "" : lastRecord.key();
                return Utils.readUrlTags(key);
            }
        }

        try {
            consumer.close();
        } catch (Exception e) {
            LogUtils.warn(log, "failed to close kafka consumer {}", e.getMessage());
        }
        return Collections.EMPTY_MAP;
    }

    /**
     * 从指定kafka,topic中获取partition
     */
    public static List<PartitionInfo> getPartitions(String bootStrapServer, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServer);
        props.put("group.id", "lastMsgFetcher");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        try {
            consumer.close();
        } catch (Exception e) {
            LogUtils.warn(log, "failed to close kafka consumer {}", e.getMessage());
        }

        return partitions;
    }

}
