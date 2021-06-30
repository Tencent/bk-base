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

package com.tencent.bk.base.datahub.iceberg.parser;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import com.tencent.bk.base.datahub.iceberg.C;
import com.tencent.bk.base.datahub.iceberg.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InnerMsgParser {

    private static final Logger log = LoggerFactory.getLogger(InnerMsgParser.class);

    private final String id;
    private final Types.StructType struct;
    private final String[] fieldsInOrder;
    private final String[] lowerCaseFields;
    private final ZoneId zoneId;
    private final boolean addEtField;
    private final boolean hasThedateField;

    private GenericRecord avro = null;  // 可复用

    /**
     * 构造函数
     *
     * @param id id，表名称，或者topic名称等具备唯一性的字符串
     * @param fieldsInOrder 需要解析的字段名称数组
     * @param schema 表的schema信息
     */
    public InnerMsgParser(String id, String[] fieldsInOrder, Schema schema) {
        this(id, fieldsInOrder, schema, ZoneOffset.systemDefault());
    }

    /**
     * 构造函数
     *
     * @param id id，表名称，或者topic名称等具备唯一性的字符串
     * @param fieldsInOrder 需要解析的字段名称数组
     * @param schema 表的schema信息
     * @param zone 时区信息，用于计算本地日期对于的thedate值
     */
    public InnerMsgParser(String id, String[] fieldsInOrder, Schema schema, ZoneId zone) {
        this.id = id;
        this.fieldsInOrder = fieldsInOrder;
        this.struct = schema.asStruct();
        this.lowerCaseFields = Utils.lowerCaseArray(this.fieldsInOrder);
        this.zoneId = zone;
        // ET 为默认分区字段，内部使用，用户不可能使用此字段
        addEtField = schema.findField(C.ET) != null;
        // 记录是否包含thedate字段，当msg中此字段为null时，使用dtEventTimeStamp的值计算thedate
        hasThedateField = Utils.getIndex(fieldsInOrder, C.THEDATE) >= 0;
    }


    /**
     * 解析kafka的消息内容
     *
     * @param key kafka消息的key值
     * @param value kafka消息的value值
     * @return 解析结果对象，如果发生异常，返回空对象。
     */
    public Optional<ParseResult> parse(String key, String value) {
        if (!isDatabusEvent(key, value)) {
            // reader 重新定义，防止schema变更时导致数据转换不准确
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
            try (ByteArrayInputStream in = new ByteArrayInputStream(value.getBytes(ISO_8859_1));
                    DataFileStream<GenericRecord> dr = new DataFileStream<>(in, reader)) {
                if (dr.hasNext()) {
                    // 一条kafka msg中只含有一个avro记录，每个avro记录包含多条实际的数据记录
                    avro = dr.next(avro);
                    if (avro.get(C.VALUE) == null) {
                        return Optional.empty();
                    }

                    // 遍及记录，转换为iceberg中的Record对象
                    List<Record> records = ((GenericArray<GenericRecord>) avro.get(C.VALUE))
                            .stream()
                            .map(this::convertAvroRecord)
                            .collect(Collectors.toList());
                    long tagTime = (avro.get(C.TAGTIME) == null) ? 0 : (Long) avro.get(C.TAGTIME);
                    long dateTime = getDateTimeFromKey(key);
                    String metricTag = (avro.get(C.METRICTAG) == null) ? "" + tagTime
                            : avro.get(C.METRICTAG).toString();

                    return Optional.of(new ParseResult(records, tagTime, dateTime, metricTag));
                }
            } catch (IOException e) {
                // 打印异常日志
                log.warn(String.format("%s got bad msg key/value: %s - %s", id, key,
                        value.substring(0, Math.min(100, value.length()))), e);
            }
        }

        return Optional.empty();
    }

    /**
     * 判断此条kafka msg是否为事件消息
     *
     * @param key kafka的msg的key值
     * @param value kafka的msg的value值
     * @return True/False
     */
    private boolean isDatabusEvent(String key, String value) {
        return value.length() == 0 && key.startsWith(C.DATABUSEVENT);
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
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.warn(String.format("%s got bad msg key: %s", id, msgKey), e);
        }
        return dateTime;
    }

    /**
     * 将avro格式的记录转换为iceberg中的Record对象，并将dtEventTime的值用iceberg中的timestamp类型替换。
     *
     * @param avro avro格式的数据
     * @return iceberg中的Record对象
     */
    private Record convertAvroRecord(GenericRecord avro) {
        Record record = org.apache.iceberg.data.GenericRecord.create(struct);
        for (int i = 0; i < fieldsInOrder.length; i++) {
            Object obj = avro.get(fieldsInOrder[i]);
            if (obj == null) {
                continue;  // 跳过null值的处理
            }

            if (C.DTETS.equals(fieldsInOrder[i])) {
                Instant instant = Instant.ofEpochMilli((Long) obj);
                if (addEtField) {
                    // 用dtEventTimeStamp的时间戳构建OffsetDateTime字段作为分区字段的值存储
                    record.setField(C.ET, instant.atOffset(ZoneOffset.UTC));
                }

                if (hasThedateField && avro.get(C.THEDATE) == null) {
                    // 对于thedate，计算本地的日期，然后组成年月日的数值
                    LocalDateTime time = LocalDateTime.ofInstant(instant, zoneId);
                    int thedate = time.getYear() * 10000 + time.getMonthValue() * 100 + time.getDayOfMonth();
                    record.setField(C.THEDATE, thedate);
                }
            } else if (obj instanceof Utf8) {
                obj = obj.toString();  // avro中的string为org.apache.avro.util.Utf8类，并非java的string类，需要进行转换
            }

            record.setField(lowerCaseFields[i], obj);
        }

        return record;
    }
}