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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class AvroUtils {

    private static final Logger log = LoggerFactory.getLogger(AvroUtils.class);

    public static final int DEFAULT_DECIMAL_SCALE = 6;
    public static final int DEFAULT_PRECISION = 38;
    public static final int BUFFER_SIZE = 10240; // 10KB

    /**
     * 根据清洗配置中包含的字段列表生成avro格式的schema字符串，并返回。
     *
     * @param columns 字段和字段类型的Map
     * @return avro schema
     */
    public static Schema getRecordSchema(Map<String, String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\", \"name\":\"msg\", \"fields\":[");
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            String colType = entry.getValue().toLowerCase();
            // 数据可以为null，在字段schema的type里指定为union类型，包含null值
            sb.append("{\"name\":\"").append(entry.getKey()).append("\", \"type\":[\"null\",");
            // 目前系统仅仅支持 int/long/double/string/text,其中avro中string和text都属于string类型
            if (colType.equals("int") || colType.equals("long")) {
                sb.append("\"int\", \"long\"]}");
            } else if (colType.equals("double")) {
                sb.append("\"float\", \"double\"]}");
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
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]}");
        String schema = sb.toString();
        LogUtils.trace(log, "avro schema: {}", schema);

        return new Schema.Parser().parse(schema);
    }

    /**
     * 获取发送到inner kafka中的消息体的schema，包含tagtime、metrictag等标签信息
     *
     * @param rtId result table id
     * @param recordSchema 单条记录的schema
     * @return kafka消息体的schema
     */
    public static Schema getMsgSchema(String rtId, Schema recordSchema) {
        return SchemaBuilder.record("msg_" + rtId)
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
    }

    /**
     * 构建avro记录的数组，并作为avro array返回
     *
     * @param records 待组装的记录数据列表
     * @param colsInOrder 顺序排列的字段名称数组
     * @param recordSchema 记录的schema信息
     * @return 包含多条avro记录的avro array对象
     */
    public static GenericArray<GenericRecord> composeAvroRecordArray(List<List<Object>> records, String[] colsInOrder,
            Schema recordSchema) {
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericArray<GenericRecord> arr = new GenericData.Array<>(records.size(), recordArrSchema);

        for (List<Object> value : records) {
            GenericRecord avroRecord = new GenericData.Record(recordSchema);
            for (int i = 0; i < colsInOrder.length; i++) {
                String col = colsInOrder[i];
                try {
                    // 根据数据的类型将其转换为不同的类型
                    if (value.get(i) instanceof BigDecimal) {
                        BigDecimal bigDecimal = (BigDecimal) value.get(i);
                        bigDecimal = bigDecimal.setScale(DEFAULT_PRECISION, BigDecimal.ROUND_HALF_UP);
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
     * 构建kafka inner中的avro记录
     *
     * @param msgSchema 消息的schema信息
     * @param recordArr 消息中的avro array对象
     * @param tagTime tagTime时间戳
     * @param logicalTag 逻辑标签
     * @return avro记录
     */
    public static GenericRecord composeInnerAvroRecord(Schema msgSchema, GenericArray<GenericRecord> recordArr,
            long tagTime, String logicalTag) {
        GenericRecord msgRecord = new GenericData.Record(msgSchema);

        msgRecord.put(Consts._VALUE_, recordArr);
        msgRecord.put(Consts._TAGTIME_, tagTime);
        msgRecord.put(Consts._METRICTAG_, logicalTag);

        return msgRecord;
    }

    /**
     * 将avro记录转换为二进制字符串
     *
     * @return 二进制字符串（编码ISO-8859）
     */
    public static String getAvroBinaryString(GenericRecord record, DataFileWriter<GenericRecord> dataFileWriter) {
        String result = "";
        try {
            LogUtils.trace(log, record.toString());
            // 将avro格式记录转换为String
            ByteArrayOutputStream output = new ByteArrayOutputStream(BUFFER_SIZE); // 10KB
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
            closeResources(dataFileWriter);
        }

        return result;
    }

    /**
     * 资源释放
     *
     * @param resources 资源列表
     */
    private static void closeResources(Closeable... resources) {
        if (resources != null) {
            for (Closeable resource : resources) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (IOException e) {
                        log.error("close resource error!");
                    }
                }
            }
        } else {
            log.warn("resources is null!");
        }
    }

    /**
     * 根据msg schema构建avro的文件写入对象
     *
     * @param msgSchema avro msg schema
     * @return avro的文件写入对象
     */
    public static DataFileWriter<GenericRecord> getDataFileWriter(Schema msgSchema) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(msgSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());

        return dataFileWriter;
    }

}
