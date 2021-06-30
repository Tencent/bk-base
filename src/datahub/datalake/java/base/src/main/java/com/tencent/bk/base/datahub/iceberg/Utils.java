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

package com.tencent.bk.base.datahub.iceberg;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    public static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    public static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();
    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        MAPPER.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
        MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    /**
     * 解析commit msg字符串，转换为CommitMsg对象返回。
     *
     * @param msg 字符串，json格式
     * @return CommitMsg对象，或者空对象
     */
    public static Optional<CommitMsg> parseCommitMsg(String msg) {
        try {
            CommitMsg m = MAPPER.readValue(msg, CommitMsg.class);
            return Optional.of(m);
        } catch (IOException e) {
            log.warn("failed to parse commit msg: " + msg, e);
            return Optional.empty();
        }
    }

    /**
     * 将java对象转换为json字符串。当转换失败时，返回空的json字符串"{}"
     *
     * @param obj java对象
     * @return json字符串
     */
    public static String toJsonString(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            log.warn("failed to write object to json string: " + obj, e);
            return "{}";
        }
    }

    /**
     * 按照表字段的名称和类型构建表的schema，要求字段名称和类型均为小写字母
     *
     * @param fields 表字段和类型信息
     * @return 表的schema
     */
    public static Schema buildSchema(List<TableField> fields) {
        AtomicInteger id = new AtomicInteger(1);
        List<Types.NestedField> cols = fields.stream()
                .map(e -> e.buildField(id.getAndIncrement()))
                .collect(Collectors.toList());

        return new Schema(cols);
    }

    /**
     * 构建表的分区信息
     *
     * @param schema 表的schema
     * @param methods 表的分区字段
     * @return 表的分区信息
     */
    public static PartitionSpec buildPartitionSpec(Schema schema, List<PartitionMethod> methods) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        methods.forEach(k -> k.addPartitionMethod(builder));

        return builder.build();
    }

    /**
     * 类型转换
     *
     * @param type 将数据平台的数据类型转换为iceberg的数据类型，小写字符
     * @return 对应iceberg的数据类型
     */
    public static Type convertType(String type) {
        switch (type) {
            case C.INT:
                return Types.IntegerType.get();
            case C.LONG:
                return Types.LongType.get();
            case C.DOUBLE:
                return Types.DoubleType.get();
            case C.FLOAT:
                return Types.FloatType.get();
            case C.TIMESTAMP:
                return Types.TimestampType.withZone();
            case C.STRING:
            case C.TEXT:
            default:
                return Types.StringType.get();
        }
    }

    /**
     * 将集合中的字符串都转换为小写字符
     *
     * @param fields 字段名称集合
     * @return 转为小写的字段名称集合
     */
    public static Set<String> lowerCaseSet(Set<String> fields) {
        return fields.stream().map(String::toLowerCase).collect(Collectors.toSet());
    }

    /**
     * 将字符串数组中的字符都转为小写字符
     *
     * @param fields 字符串数组
     * @return 小写字符的字符串数组
     */
    public static String[] lowerCaseArray(String[] fields) {
        String[] result = new String[fields.length];
        IntStream.range(0, fields.length).forEach(i -> result[i] = fields[i].toLowerCase());
        return result;
    }

    /**
     * 寻找字符串在数组中的索引值，如果不存在，则返回-1
     *
     * @param array 字符串数组
     * @param toFind 待查找的字符串
     * @return 字符串的索引值
     */
    public static int getIndex(String[] array, String toFind) {
        return IntStream.range(0, array.length).filter(i -> array[i].equals(toFind)).findFirst().orElse(-1);
    }

    /**
     * 将对象数组转换为iceberg中的record列表
     *
     * @param schema 表的schema
     * @param objList 对象数组
     * @param lowercaseFields 对象列表对应的字段名称数组
     * @return record列表
     */
    public static List<Record> convertRecords(Schema schema, List<List<Object>> objList,
            String[] lowercaseFields) {
        return objList.stream()
                .map(list -> {
                    GenericRecord record = GenericRecord.create(schema);
                    for (int i = 0; i < list.size(); i++) {
                        record.setField(lowercaseFields[i], list.get(i));
                    }
                    return record;
                }).collect(Collectors.toList());
    }

    /**
     * 将对象数组转换为iceberg中的record列表，并且将原有的dtEventTime字段由字符串转为为timestamp类型。
     *
     * @param schema 表的schema
     * @param objList 对象数组
     * @param lowercaseFields 对象列表对应的字段名称数组
     * @param tsIdx dtEventTimeStamp字段所在的索引
     * @param eventTimeIdx dtEventTime字段所在的索引
     * @return record列表
     */
    public static List<Record> convertRecordsWithEventTime(Schema schema,
            List<List<Object>> objList, String[] lowercaseFields, int tsIdx, int eventTimeIdx) {
        return objList.stream()
                .map(list -> {
                    GenericRecord record = GenericRecord.create(schema);
                    for (int i = 0; i < list.size(); i++) {
                        if (i == eventTimeIdx) {
                            long ts = (Long) list.get(tsIdx);
                            OffsetDateTime dt = Instant.ofEpochMilli(ts).atOffset(ZoneOffset.UTC);
                            record.setField(lowercaseFields[i], dt);
                        } else {
                            record.setField(lowercaseFields[i], list.get(i));
                        }
                    }
                    return record;
                }).collect(Collectors.toList());
    }


    /**
     * 获取指定位置的数据
     *
     * @param data 数据结构
     * @param pos 数据位置
     * @param javaClass 数据的java类型
     * @param <T> 返回的数据类型
     * @return 返回的数据
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(StructLike data, int pos, Class<?> javaClass) {
        return data.get(pos, (Class<T>) javaClass);
    }

    /**
     * 解析存放在map中树形结构的表达式，转换为iceberg中的表达式
     *
     * @param exprs 包含表达式结构的map
     * @return iceberg的Expression对象
     */
    public static Expression parseExpressions(Map<Object, Object> exprs) {
        String op = exprs.get("operation").toString().toUpperCase();
        Object left = exprs.get("left");
        Object right = exprs.get("right");

        Expression.Operation operation = Expression.Operation.valueOf(op);
        // 不支持NOT/True/False操作符
        switch (operation) {
            case GT:
                return Expressions.greaterThan(left.toString(), right);
            case LT:
                return Expressions.lessThan(left.toString(), right);
            case EQ:
                return Expressions.equal(left.toString(), right);
            case GT_EQ:
                return Expressions.greaterThanOrEqual(left.toString(), right);
            case LT_EQ:
                return Expressions.lessThanOrEqual(left.toString(), right);
            case NOT_EQ:
                return Expressions.notEqual(left.toString(), right);
            case IN:
                Object[] v1 = ((List) right).toArray();
                return Expressions.in(left.toString(), v1);
            case NOT_IN:
                Object[] v2 = ((List) right).toArray();
                return Expressions.notIn(left.toString(), v2);
            case NOT_NULL:
                return Expressions.notNull(left.toString());
            case IS_NULL:
                return Expressions.isNull(left.toString());
            case AND:
                return Expressions.and(parseExpressions((Map) left), parseExpressions((Map) right));
            case OR:
                return Expressions.or(parseExpressions((Map) left), parseExpressions((Map) right));
            default:
                throw new IllegalArgumentException("unsupported expressions: " + exprs);
        }
    }

    /**
     * 将字符串value转换为int值，如果失败，则返回默认值
     *
     * @param value 字符串值
     * @param dft 默认值
     * @return int数值
     */
    public static int getOrDefaultInt(String value, int dft) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignore) {
            return dft;
        }
    }

    /**
     * 将字符串value转化为long值，如果失败，则返回默认值
     *
     * @param value 字符串值
     * @param dft 默认值
     * @return long数值
     */
    public static long getOrDefaultLong(String value, long dft) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignore) {
            return dft;
        }
    }
}