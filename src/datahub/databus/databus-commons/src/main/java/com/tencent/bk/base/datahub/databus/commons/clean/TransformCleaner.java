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

package com.tencent.bk.base.datahub.databus.commons.clean;

import com.tencent.bk.base.datahub.databus.commons.bean.DataBusResult;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.commons.BkDatabusContext;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.connector.sink.BkSinkRecord;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.EtlConverter;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformCleaner {

    private static final Logger log = LoggerFactory.getLogger(TransformCleaner.class);

    private final ThreadLocal<SimpleDateFormat> dateFormat = ThreadLocal.withInitial(() -> {
        String tz = System.getProperty(EtlConsts.DISPLAY_TIMEZONE);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        format.setTimeZone(TimeZone.getTimeZone(tz));
        return format;
    });
    private final ThreadLocal<SimpleDateFormat> dteventtimeDateFormat = ThreadLocal.withInitial(() -> {
        String tz = System.getProperty(EtlConsts.DISPLAY_TIMEZONE);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(tz));
        return format;
    });

    private String[] colsInOrder;
    private Schema recordSchema;
    private Schema recordArrSchema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private GenericRecord msgRecord;

    private final BkDatabusContext context;

    /**
     * 通过解析配置，构建数据转换处理的对象，生成写入kafka的producer
     */
    public TransformCleaner(BkDatabusContext context) {
        this.context = context;
        initParser();
        LogUtils.info(log, "created converter and columns schema! {}", context.getRtId());
    }


    /**
     * 从kafka topic上获取到消息，通过数据转换，提取用户所需的数据记录， 将数据记录打包，然后转换为Avro格式的数据，写到kafka sink topic中
     */
    public DataBusResult<String> clean(BkSinkRecord<byte[]> record) {

        LogUtils.debug(log, "{} got msg of partition {} recordSequence {} value {} . start transforming them",
                context.getName(), record.getPartition(), record.getRecordSequence(), new String(record.getValue(),
                        StandardCharsets.UTF_8));

        final long now = System.currentTimeMillis() / 1000;  // 取秒
        final long tagTime = now / 60 * 60; // 取分钟对应的秒数

        byte[] val = record.getValue();
        if (val == null) {
            return null;
        }
        // 获取key
        String keyStr = record.getKey();
        ConvertResult result = context.getConverter().getListObjects(keyStr, val, colsInOrder);
        List<List<Object>> listResult = result.getObjListResult();
        DataBusResult<String> dataBusResult = new DataBusResult<>(result, listResult.size(), val.length);

        // 当buffer中数据条数达到阈值,或者当前处理的是最后一条msg时,将消息发送到kafka中
        try {
            dataBusResult.setIntermediateResult(
                    generateCompactMsg(tagTime, listResult, record.getRecordSequence(), record.getPartition()));
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_AVRO_DATA,
                    context.getRtId() + " exception during compose and send result to kafka: " + listResult, e);
            throw new ConnectException(e);
        }

        return dataBusResult;

    }


    /**
     * 初始化清洗的converter,以及需要使用的字段
     */
    public void initParser() {
        Map<String, String> columns = new HashMap<>(context.getTaskContext().getRtColumns());
        // 屏蔽用户的dtEventTime、dtEventTimeStamp、localTime字段,屏蔽rt里的offset字段
        columns.remove(Consts.DTEVENTTIME);
        columns.remove(Consts.DTEVENTTIMESTAMP);
        columns.remove(Consts.LOCALTIME);
        columns.remove(Consts.OFFSET);
        columns.remove(Consts.ITERATION_IDX);

        colsInOrder = columns.keySet().toArray(new String[columns.size()]);
        // 生成avro schema和一些处理的对象。创建record schema时,在columns的基础上增加了localTime、dtEventTime、dtEventTimestamp等字段
        String schemaStr = TransformUtils.getAvroSchema(columns);
        recordSchema = new Schema.Parser().parse(schemaStr);
        recordArrSchema = Schema.createArray(recordSchema);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_" + context.getDataId())
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(msgSchema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        msgRecord = new GenericData.Record(msgSchema);
        LogUtils.debug(log, "msgSchema {}", msgSchema.toString(true));
    }


    /**
     * 将清洗出的结果集发送到kafka inner中
     *
     * @param tagTime tagTime时间戳
     * @param records 结果记录
     * @param offset kafka记录offset
     * @param partitionId kafka记录partitionId
     */
    public BkSourceRecord<String> generateCompactMsg(long tagTime, List<List<Object>> records, long offset,
            int partitionId) {
        if (records.size() == 0) {
            return null;
        }
        GenericArray<GenericRecord> arr = new GenericData.Array<>(records.size(), recordArrSchema);
        long tsMs = tagTime * 1000;

        // MARK 清洗出来的记录,一个包中的多条record的时间是否在同一分钟内??
        for (List<Object> value : records) {
            GenericRecord avroRecord = new GenericData.Record(recordSchema);
            for (int i = 0; i < colsInOrder.length; i++) {
                String col = colsInOrder[i];
                try {
                    // 构造dtEventTimeStamp、dtEventTime、localTime等字段
                    if (col.equals(Consts.TIMESTAMP)) {
                        tsMs = (long) value.get(i);
                        avroRecord.put(Consts.DTEVENTTIME, dteventtimeDateFormat.get().format(new Date(tsMs)));
                        avroRecord.put(Consts.DTEVENTTIMESTAMP, tsMs);
                        avroRecord.put(Consts.LOCALTIME, dteventtimeDateFormat.get().format(new Date(tagTime * 1000)));
                        // TIMESTAMP单位为秒, avro 类型为int
                        avroRecord.put(Consts.TIMESTAMP, (int) (tsMs / 1000));
                        continue;
                    }
                    // 根据数据的类型将其转换为不同的类型
                    Object colVal = value.get(i);
                    if (colVal instanceof BigDecimal) {
                        BigDecimal bigDecimal = (BigDecimal) colVal;
                        bigDecimal = bigDecimal
                                .setScale(TransformUtils.DEFAULT_DECIMAL_SCALE, BigDecimal.ROUND_HALF_UP);
                        avroRecord.put(col, ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()));
                    } else if (colVal instanceof BigInteger) {
                        BigInteger bigInteger = (BigInteger) colVal;
                        avroRecord.put(col, ByteBuffer.wrap(bigInteger.toByteArray()));
                    } else if (colVal instanceof Map) {
                        // 这种情况colVal类型应该为json字符串，使用HashMap来表示，这里需要做一次转换
                        avroRecord.put(col, JsonUtils.toJsonWithoutException(colVal));
                    } else {
                        avroRecord.put(col, colVal);
                    }

                } catch (AvroRuntimeException ignore) {
                    // 字段和schema不匹配，或者设置值的时候发生异常，记录一下，不影响整体流程
                    LogUtils.warn(log, "failed to set value for avro record! name: {} value: {} schema: {} errMsg: {}",
                            col, value.get(i), avroRecord.getSchema().toString(false), ignore.getMessage());
                }
            }
            arr.add(avroRecord);
        }

        msgRecord.put(Consts._VALUE_, arr);
        msgRecord.put(Consts._TAGTIME_, tagTime);
        msgRecord.put(Consts._METRICTAG_, getMetricTag(tagTime));

        // MARK dateStr 和 tagTime为啥设成一样的?
        String dateStr = dateFormat.get().format(new Date(tsMs));
        String keyStr = dateStr + "&tagTime=" + dateStr + "&offset=" + offset + "&partition=" + partitionId;

        String msgValue = TransformUtils.getAvroBinaryString(msgRecord, dataFileWriter);

        if (StringUtils.isNotBlank(msgValue)) {
            // 发送消息到kafka中，使用转换结束时的时间（秒）作为msg key
            return new BkSourceRecord(keyStr, msgValue, msgValue.length());
        } else {
            LogUtils.warn(log, "the avro msg is empty, something is wrong!");
            return null;
        }
    }

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    protected String getMetricTag(long tagTime) {
        if (context.getConverter() instanceof EtlConverter) {
            return context.getName() + "|" + tagTime;
        }
        return String.valueOf(tagTime);
    }

    /**
     * 获取清洗字段
     *
     * @return 清洗字段列表
     */
    public String[] getColsInOrder() {
        return this.colsInOrder;
    }

}
