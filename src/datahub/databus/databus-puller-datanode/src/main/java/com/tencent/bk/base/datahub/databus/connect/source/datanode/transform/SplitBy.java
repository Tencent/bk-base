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

package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.exceptons.TransformException;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.util.EventMetaTools;

import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
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
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Split固化算子清洗逻辑
 */
@SuppressWarnings("ALL")
public class SplitBy extends BaseTransform {

    private static final Logger log = LoggerFactory.getLogger(SplitBy.class);
    private EventMetaTools eventMetaTools;
    private Map<String, Expression> splitLogicMap = null;

    public SplitBy(String connector, String config, Converter converter, TaskContext ctx) {
        super(connector, config, converter, ctx);
    }

    @Override
    public void doConfigure() {
        LogUtils.debug(log, "begin to doConfigure()...");
        if (StringUtils.isNotBlank(config)) {
            LogUtils.info(log, "begin to doConfigure() config:{}", config);
            //用户自定义配置
            String logicConfig = config;
            try {
                Set<String> columnsSet = ctx.getDbTableColumns();
                columnsSet.remove(Consts.THEDATE);
                colsInOrder = columnsSet.toArray(new String[columnsSet.size()]);
                eventMetaTools = new EventMetaTools(colsInOrder);
                //获取变量值
                splitLogicMap = new HashMap<>(16);

                //split 逻辑解析
                Map<String, Object> logicMap = (Map<String, Object>) JsonUtils.toMap(logicConfig);
                List<Map<String, Object>> logicArray = (List<Map<String, Object>>) logicMap
                        .get(TransformConstants.SPLITLOGIC);
                if (logicArray == null || logicArray.size() == 0) {
                    return;
                }
                //获取变量值
                for (Map<String, Object> logicObj : logicArray) {
                    Object logic = logicObj.get(TransformConstants.SPLITLOGIC_LOGIC);
                    Object bidObject = logicObj.get(TransformConstants.SPLITLOGIC_BID);
                    if (logic == null || bidObject == null) {
                        continue;
                    }
                    String exp = logic.toString();
                    String bid = bidObject.toString();
                    Expression compiledExp = AviatorEvaluator.compile(exp);
                    splitLogicMap.put(bid, compiledExp);
                    LogUtils.info(log, "splitLogicMap.put(bid:{}, compiledExp:{});", bid, compiledExp);
                }
            } catch (Exception e) {
                LogUtils.error("", log, "SplitBy doConfigure error!", e);
            }
        }
    }

    /**
     * 对于分拆节点，需要对数据进行处理，按照业务id对数据进行拆分
     *
     * @param record kafka中一条记录
     * @return TransformResult 转换结果封装
     */
    @Override
    public TransformResult transform(ConsumerRecord<byte[], byte[]> record) throws TransformException {
        TransformResult transformResult = new TransformResult();
        if (null == record.value()) {
            return null;
        }
        try {
            byte[] val = new String(record.value(), StandardCharsets.UTF_8).getBytes(StandardCharsets.ISO_8859_1);
            String keyStr = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : "";
            ConvertResult convertResult = converter.getListObjects(keyStr, val, colsInOrder);
            Map<String, List<List<Object>>> msgGroupMap = filterAndGroup(convertResult);
            RecordSchema extractRecordSchema = extractRecordSchema(convertResult);
            //对分组消息进行avro序列化
            // 取秒
            final long now = System.currentTimeMillis() / 1000;
            // 取分钟对应的秒数
            final long tagTime = now / 60 * 60;
            //分组构建comsumerrecord
            for (Entry<String, List<List<Object>>> entry : msgGroupMap.entrySet()) {
                List<List<Object>> records = entry.getValue();
                if (records == null || records.size() == 0) {
                    continue;
                }
                ConsumerRecord<byte[], byte[]> tmpRecord = packAvroPackage(records, extractRecordSchema, tagTime,
                        record.offset(), record.topic());
                if (tmpRecord != null) {
                    transformResult.putRecord(entry.getKey(), tmpRecord);
                }

            }
        } catch (Exception e) {
            throw new TransformException(e);
        }
        return transformResult;
    }

    /**
     * 将多条记录打包成一个avro包
     *
     * @param records 需要打包的数据
     * @param extractRecordSchema 原始数据对应的schema 信息
     * @param tagTime 标签时间
     * @param offset 原始数据的offset
     * @return 打包后的avro包
     */
    private ConsumerRecord<byte[], byte[]> packAvroPackage(List<List<Object>> records, RecordSchema extractRecordSchema,
            long tagTime, long offset, String topic) {
        GenericArray<GenericRecord> arr = new GenericData.Array<>(records.size(), extractRecordSchema.recordArrSchema);
        long tsSec = tagTime;
        for (List<Object> value : records) {
            GenericRecord avroRecord = new GenericData.Record(extractRecordSchema.recordSchema);
            for (int i = 0; i < colsInOrder.length; i++) {
                String col = colsInOrder[i];
                try {
                    if (col.equals(Consts.DTEVENTTIMESTAMP)) {
                        tsSec = (long) value.get(i) / 1000 / 60 * 60;
                    }
                    // 根据数据的类型将其转换为不同的类型
                    Object colVal = value.get(i);
                    if (colVal instanceof BigDecimal) {
                        BigDecimal bigDecimal = (BigDecimal) colVal;
                        bigDecimal = bigDecimal
                                .setScale(AvroUtils.DEFAULT_DECIMAL_SCALE, BigDecimal.ROUND_HALF_UP);
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
                    LogUtils.warn(log,
                            "failed to set value for avro record! name: {} value: {} schema: {} errMsg: {}",
                            col, value.get(i), avroRecord.getSchema().toString(false), ignore.getMessage());
                }
            }
            arr.add(avroRecord);
        }
        extractRecordSchema.msgRecord.put(Consts._VALUE_, arr);
        extractRecordSchema.msgRecord.put(Consts._TAGTIME_, tagTime);
        extractRecordSchema.msgRecord.put(Consts._METRICTAG_, connector + "|" + tagTime);
        // MARK dateStr 和 tagTime为啥设成一样的?
        String dateStr = dateFormat.format(new Date(tsSec * 1000));
        String keyStr = dateStr + "&tagTime=" + dateStr + "&offset=" + offset;
        String msgValue = AvroUtils
                .getAvroBinaryString(extractRecordSchema.msgRecord, extractRecordSchema.dataFileWriter);
        ConsumerRecord<byte[], byte[]> tmpRecord = null;
        if (StringUtils.isNotBlank(msgValue)) {
            tmpRecord = new ConsumerRecord<>(topic, 0, offset, keyStr.getBytes(StandardCharsets.UTF_8),
                    msgValue.getBytes(StandardCharsets.UTF_8));
        }
        return tmpRecord;

    }


    private RecordSchema extractRecordSchema(ConvertResult convertResult) {
        return new RecordSchema(convertResult.getAvroSchema(), this.ctx.getDataId());
    }

    /**
     * avro数据反序列化和按照每个业务id的逻辑分组
     *
     * @param record avro序列化数据
     */
    private Map<String, List<List<Object>>> filterAndGroup(ConvertResult convertResult) {
        Map<String, List<List<Object>>> msgGroupMap = new HashMap<>(splitLogicMap.size());
        try {
            List<List<Object>> listResult = convertResult.getObjListResult();
            //对每条消息都进行逻辑校验,然后进行消息分组（此时消息还未进行avro序列化）
            String bid;
            Expression logicExp = null;
            for (List<Object> columnValArray : listResult) {
                for (Entry<String, Expression> splitLogic : splitLogicMap.entrySet()) {
                    bid = splitLogic.getKey();
                    logicExp = splitLogic.getValue();
                    Map<String, Object> parmMap = generateParmMap(columnValArray, logicExp.getVariableNames());
                    //逻辑校验
                    Boolean checkResult = (Boolean) logicExp.execute(parmMap);
                    LogUtils.debug(log, "checkResult:{} logicExp:{} parmMap:{}",
                            new Object[]{checkResult, logicExp, parmMap});
                    //校验通过，按照bid分组
                    if (checkResult) {
                        List<List<Object>> objList = msgGroupMap.computeIfAbsent(bid, (id) -> new ArrayList<>());
                        objList.add(columnValArray);
                        msgGroupMap.put(bid, objList);
                    }
                }
            }
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, "", "split datanode processing failure!", e);
        }
        return msgGroupMap;
    }

    private Map<String, Object> generateParmMap(List<Object> columnValArray, List<String> varNameList) {
        Map<String, Object> parmMap = new HashMap<>(varNameList.size());
        for (String varName : varNameList) {
            if ("true".equals(varName.toLowerCase()) || "false".equals(varName.toLowerCase())) {
                continue;
            }
            Object columnValue = eventMetaTools.getColumnValue(columnValArray, varName);
            if (null != columnValue) {
                parmMap.put(varName, columnValue);
            }
        }
        return parmMap;
    }

    protected static class RecordSchema {

        public Schema recordSchema;
        public Schema recordArrSchema;
        public DataFileWriter<GenericRecord> dataFileWriter;
        public GenericRecord msgRecord;

        protected RecordSchema(Schema resultSchme, int dataId) {
            try {
                recordArrSchema = resultSchme.getField(Consts._VALUE_).schema();
                recordSchema = recordArrSchema.getElementType();
                Schema msgSchema = SchemaBuilder.record("kafkamsg_" + dataId)
                        .fields()
                        .name(Consts._TAGTIME_).type().longType().longDefault(0)
                        .name(Consts._METRICTAG_).type().stringType().noDefault()
                        .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                        .endRecord();
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(msgSchema);
                dataFileWriter = new DataFileWriter<>(datumWriter);
                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                msgRecord = new GenericData.Record(msgSchema);
            } catch (Exception e) {
                LogUtils.reportExceptionLog(log, "", "Split datanode extract record schema failure!", e);
            }
        }
    }
}
