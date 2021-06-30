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

package com.tencent.bk.base.datahub.databus.commons.convert;


import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(AvroConverter.class);

    private static final Conversion<BigDecimal> DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    // objects to re-use
    private DatumReader<GenericRecord> reader = null;
    private GenericRecord avroRecord = null;

    protected AvroConverter() {
        this.reader = new GenericDatumReader<GenericRecord>();
    }

    /**
     * 从avro格式的kafka msg中，解析出每条数据，并将每条数据放入Object的数组， 多条数据放入一个list结构中返回。
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @param keysInOrder Object数组中字段排列的顺序
     * @return 包含一到多条object数组记录的结果
     */
    @Override
    public ConvertResult getListObjects(String msgKey, String msgValue, String[] keysInOrder) {
        ConvertResult result = getAvroArrayResult(msgKey, msgValue);
        GenericArray<GenericRecord> values = result.getAvroValues();

        if (values != null) {
            // 解析数据，按照顺序排列成object[]，放入list中
            List<List<Object>> recordList = new ArrayList<>(values.size());
            for (GenericRecord rec : values) {
                List<Object> objList = new ArrayList<>();

                for (String col : keysInOrder) {
                    Object obj = rec.get(col);
                    // tspider中多了 thedate 字段，需在此兼容。写入数据库中的字段以数据库表字段类型为准。
                    if (StringUtils.equals(col, Consts.THEDATE)) { // thedate字段为int类型，且必须为有效的yyyymmdd，这样才能保证插入到数据库中
                        obj = (obj == null ? result.getTheDate() : obj);
                    }
                    // avro中的string为org.apache.avro.util.Utf8类，并非java的string类，需要进行转换
                    if (obj instanceof Utf8) {
                        obj = obj.toString();
                    } else if (obj instanceof Double) {
                        Double val = (Double) obj;
                        if (Double.isInfinite(val) || Double.isNaN(val)) {
                            LogUtils.warn(log, "bad values for Double. column: {}, value: {} {}", col, keysInOrder,
                                    rec.toString());
                            obj = null;
                        }
                    } else if (obj instanceof Float) {
                        Float val = (Float) obj;
                        if (Float.isInfinite(val) || Float.isNaN(val)) {
                            LogUtils.warn(log, "bad values for Float. column: {}, value: {} {}", col, keysInOrder,
                                    rec.toString());
                            obj = null;
                        }
                    } else if (obj instanceof ByteBuffer) {
                        obj = convertAvroBytesToObject(rec.getSchema().getField(col).schema(), obj);
                    }
                    objList.add(obj);
                }
                recordList.add(objList);
            }
            // 将record列表放入结果中
            result.setObjListResult(recordList);
        }

        return result;
    }


    /**
     * 将清洗的结果转换为Object列表,放入结果集中返回。
     *
     * @param msgKey kafka msg key,字符串
     * @param msgValue kafka msg value,字节数组
     * @param keysInOrder 书序排列的column列表
     * @return 结果集对象
     */
    public ConvertResult getListObjects(String msgKey, byte[] msgValue, String[] keysInOrder) {
        String valueStr = new String(msgValue, StandardCharsets.ISO_8859_1);
        return getListObjects(msgKey, valueStr, keysInOrder);
    }

    /**
     * 从avro格式的kafka msg中解析出多条记录，每条记录转换成一个json字符串， 放入list结构里返回。
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @return 包含多条json记录的结果
     */
    @Override
    public ConvertResult getJsonList(String msgKey, String msgValue) {
        ConvertResult result = getAvroArrayResult(msgKey, msgValue);
        GenericArray<GenericRecord> values = result.getAvroValues();
        if (values != null) {
            result.setJsonResult(convertAvroArrayToJsonList(values));
        }

        return result;
    }

    @Override
    public ConvertResult getJsonList(String msgKey, byte[] msgValue) {
        String valueStr = new String(msgValue, StandardCharsets.ISO_8859_1);
        return getJsonList(msgKey, valueStr);
    }

    /**
     * 从avro格式的kafka msg中解析出多条记录，每条记录转换成一个key-value字符串， 放入list结构里返回。
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @return 包含多条key-value记录的结果
     */
    @Override
    public ConvertResult getKeyValueList(String msgKey, String msgValue, char kvSplitter, char fieldSplitter) {
        ConvertResult result = getAvroArrayResult(msgKey, msgValue);
        GenericArray<GenericRecord> values = result.getAvroValues();

        try {
            if (values != null) {
                result.setKvResult(convertAvroArrayToKvList(values, kvSplitter, fieldSplitter));
            }
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_AVRO_DATA, log, "bad avro data: " + msgValue, e);
            result.addFailedResult("bad avro data. " + e.getMessage());
            result.addErrors(e.getClass().getSimpleName());
        }

        return result;
    }

    /**
     * 从数据中解析出avro的GenericArray对象，包含多条记录
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @return 包含多条记录的结果
     */
    @Override
    public ConvertResult getAvroArrayResult(String msgKey, String msgValue) {
        ConvertResult result = new ConvertResult();
        if (msgValue.length() == 0 && msgKey.startsWith(Consts.DATABUS_EVENT)) {
            result.setIsDatabusEvent(true);
            return result;
        }
        parseMsgGetRecords(msgKey, msgValue, result);
        return result;
    }

    /**
     * 从数据中解析出avro的GenericArray对象，包含多条记录
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @return 包含多条记录的结果
     */
    @Override
    public ConvertResult getAvroArrayResultExtendThedate(String msgKey, String msgValue) {
        ConvertResult result = getAvroArrayResult(msgKey, msgValue);
        addTheDate(result);
        return result;
    }

    /**
     * 扩展thedate 字段
     *
     * @param result 包含多条记录的结果
     */
    private void addTheDate(ConvertResult result) {
        GenericArray<GenericRecord> values = result.getAvroValues();
        if (values == null || values.isEmpty() || values.get(0).getSchema().getField(Consts.THEDATE) != null) {
            return;
        }
        Schema newSchema = generateSchema(values.get(0));
        GenericArray<GenericRecord> newAvroValues = new GenericData.Array<>(values.size(), values.getSchema());
        for (GenericRecord record : values) {
            GenericRecord newRecord = new GenericData.Record(newSchema);
            List<Schema.Field> fields = record.getSchema().getFields();
            for (Schema.Field field : fields) {
                Object obj = record.get(field.name());
                newRecord.put(field.name(), obj);
            }
            newRecord.put(Consts.THEDATE, result.getTheDate());
            newAvroValues.add(newRecord);
        }
        result.setAvroValues(newAvroValues);
    }

    private Schema generateSchema(GenericRecord record) {
        Schema oldSchema = record.getSchema();
        Schema newSchema = Schema
                .createRecord(oldSchema.getName(), oldSchema.getDoc(), oldSchema.getNamespace(), oldSchema.isError());
        ArrayList<Schema.Field> newFieldList = new ArrayList();
        List<Schema.Field> fieldList = record.getSchema().getFields();
        for (Schema.Field f : fieldList) {
            Schema.Field ff = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
            newFieldList.add(ff);
        }
        newFieldList.add(new Schema.Field(Consts.THEDATE, Schema.create(Type.INT), Consts.THEDATE, "null"));
        newSchema.setFields(newFieldList);
        return newSchema;
    }

    /**
     * 解析avro msg，将外层信息写入result中，内层的records放到array里返回。
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @param result 结果对象
     */
    private void parseMsgGetRecords(String msgKey, String msgValue, ConvertResult result) {
        result.setMsgSize(msgKey.length() + msgValue.length());
        GenericArray<GenericRecord> values = null;
        // 获取年月日时分秒信息
        long dateTime = getDateTimeFromKey(msgKey);
        if (dateTime != 0) { // 正常数据
            result.setDateTime(dateTime);
            GenericRecord record = getRecordFromAvro(msgValue);
            if (record != null) {
                result.setAvroSchema(record.getSchema());
                // 获取外层结构数据
                long tagTime = (record.get(Consts._TAGTIME_) == null) ? 0 : (Long) record.get(Consts._TAGTIME_);
                String metricTag =
                        (record.get(Consts._METRICTAG_) == null) ? "" : record.get(Consts._METRICTAG_).toString();
                result.setTagTime(tagTime);
                result.setMetricTag(metricTag);
                values = (GenericArray<GenericRecord>) record.get(Consts._VALUE_);
            } else { // 数据解析异常
                result.addFailedResult("bad msg value, unable to parse. ");
                result.addErrors(Consts.BAD_KAFKA_MSG_VALUE);
            }
        } else {
            result.addFailedResult("bad msg key, unable to get datetime from msg key. " + msgKey);
            result.addErrors(Consts.BAD_KAFKA_MSG_KEY);
        }

        result.setAvroValues(values);
    }

    /**
     * 从msg key中获取到数据的时间并返回，如果日期非法，则返回0
     *
     * @param msgKey kafka msg key
     * @return 时间yyyyMMddHHmmss格式
     */
    private long getDateTimeFromKey(String msgKey) {
        long dateTime = 0; // 数据日期，必须为有效的yyyyMMdd
        try {
            dateTime = Long.parseLong(msgKey.substring(0, 14)); // msgKey前8位应该是yyyyMMdd的日期
            if (dateTime < 20000101000000L || dateTime > 21000101000000L) {
                dateTime = 0; // 如果日期超出2000年~2100年，则认为异常
            }
        } catch (NumberFormatException | IndexOutOfBoundsException ignore) {
            LogUtils.warn(log, "get exception! msgKey: " + msgKey, ignore);
        }
        return dateTime;
    }

    /**
     * 解析avro格式的数据，将解析结果返回。
     *
     * @param value avro格式的数据（包含schema）
     * @return 解析为AvroRecord的记录，或者null（当解析发生异常时）
     */
    private GenericRecord getRecordFromAvro(String value) {
        try {
            byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            DataFileStream<GenericRecord> dataReader = new DataFileStream<>(input,
                    new GenericDatumReader<GenericRecord>());
            if (dataReader.hasNext()) {
                // 一条kafka msg中只含有一个avro记录，每个avro记录包含多条实际的数据记录
                return dataReader.next(avroRecord);
            }
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_AVRO_DATA, "bad avro data!", e);
        }

        return null;
    }

    /**
     * 将avro record转换为json字符串
     *
     * @param record avro的一条记录
     * @return 转换为json字符串的avro记录
     */
    public static String getJsonString(GenericRecord record) {
        List<Schema.Field> fields = record.getSchema().getFields();
        Map<String, Object> res = new HashMap<>(fields.size());
        for (Schema.Field field : fields) {
            Object obj = record.get(field.name());
            if (obj instanceof Utf8) {
                obj = obj.toString();
            } else if (obj instanceof ByteBuffer) {
                obj = convertAvroBytesToObject(field.schema(), obj);
                // json精度有限，对于bigint和bigdecimal可能出现精度丢失，这里转换为字符串
                obj = obj.toString();
            }

            // 放入map中，等待转为json字符串
            res.put(field.name(), obj);
        }

        return JsonUtils.toJsonWithoutException(res);
    }

    /**
     * 将avro record转换为key-value字符串
     *
     * @param record avro的一条记录
     * @return 转换为key-value字符串的avro记录
     */
    public static String getKvString(GenericRecord record, char kvSplitter, char fieldSplitter) {
        List<Schema.Field> fields = record.getSchema().getFields();
        StringBuilder sb = new StringBuilder();
        for (Schema.Field field : fields) {
            Object obj = record.get(field.name());
            if (obj instanceof ByteBuffer) {
                // json精度有限，对于bigint和bigdecimal可能出现精度丢失，这里转换为字符串
                obj = convertAvroBytesToObject(field.schema(), obj);
            }

            sb.append(field.name())
                    .append(fieldSplitter)
                    .append(obj)
                    .append(kvSplitter);
        }

        return sb.toString();
    }

    /**
     * 根据字段的schema，将字段的值由bytes转换为对应的数据类型（目前支持BigDecimal）
     *
     * @param fieldSchema 字段的avro schema
     * @param obj 字段的值
     * @return 转换为BigDecimal的字段的值
     */
    public static Object convertAvroBytesToObject(Schema fieldSchema, Object obj) {
        if (fieldSchema.getType().equals(Schema.Type.UNION)) {
            for (Schema s : fieldSchema.getTypes()) {
                // TODO 如果需要支持别的类型，在这里扩展，仅仅匹配一个类型
                if (s.getType().equals(Schema.Type.BYTES)) {
                    LogicalType type = s.getLogicalType();
                    if (type instanceof LogicalTypes.Decimal) {
                        // 调用duplicate来多次复用obj中的内容，避免第一次读取后，后续的读取都为空值
                        return DECIMAL_CONVERSION.fromBytes(((ByteBuffer) obj).duplicate(), s, type);
                    }
                }
            }
        }

        // schema不匹配，无需转换对象，这里返回原始对象
        return obj;
    }

    /**
     * 将avro格式的数组转换为json字符串列表
     *
     * @param values avro格式的记录组成的数组
     * @return json格式的字符串列表
     */
    public static List<String> convertAvroArrayToJsonList(GenericArray<GenericRecord> values) {
        // 对于bytes类型，无法直接通过toString转换为字符串，需要解析实际的数据类型（logicalType）
        boolean containsBytes = values.getSchema().toString().contains("\"bytes\"");
        // 将数据转换为json格式的，放入列表中
        List<String> jsonList = new ArrayList<>(values.size());
        for (GenericRecord record : values) {
            // 针对BigDecimal单独处理（byte[]）
            if (containsBytes) {
                jsonList.add(getJsonString(record));
            } else {
                jsonList.add(record.toString());
            }
        }

        return jsonList;
    }

    /**
     * 将avro格式的数组转换为key-value字符串列表
     *
     * @param values avro格式的记录组成的数组
     * @return key-value格式的字符串列表
     */
    public static List<String> convertAvroArrayToKvList(GenericArray<GenericRecord> values, char kvSplitter,
            char fieldSplitter) {
        // 将数据转换为key-value格式的，放入列表中
        List<String> kvList = new ArrayList<>(values.size());
        for (GenericRecord record : values) {
            kvList.add(getKvString(record, kvSplitter, fieldSplitter));
        }

        return kvList;
    }

}
