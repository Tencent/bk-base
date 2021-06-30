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

import static java.time.temporal.ChronoUnit.MICROS;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class RandomGenericData {

    public static final long ONE_DAY_MS = 24 * 60 * 60 * 1_000_000L;
    public static final String CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";
    // 时间起点设置为2020-01-01，相对UTC起点1970-01-01多50年
    public static final OffsetDateTime BASE_TIME = Instant
            .ofEpochSecond((50L * (365 * 3 + 366) * 24 * 60 * 60) / 4).atOffset(ZoneOffset.UTC);
    public static final LocalDate BASE_DAY = BASE_TIME.toLocalDate();
    public static final String DIGITS = "0123456789";
    public static final float[] INIT_FLOATS = {Float.MIN_VALUE, -Float.MIN_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE,
            Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 0.0F, Float.NaN};
    public static final double[] INIT_DOUBLES = {Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE,
            -Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.0F, Double.NaN};

    private RandomGenericData() {
    }

    public static List<Record> generate(Schema schema, int numRecords, long seed) {
        RandomDataGenerator generator = new RandomDataGenerator(seed);
        List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
        for (int i = 0; i < numRecords; i += 1) {
            records.add((Record) TypeUtil.visit(schema, generator));
        }

        return records;
    }

    @SuppressWarnings("RandomModInteger")
    protected static Object generatePrimitive(Type.PrimitiveType primitive, Random random) {
        int choice = random.nextInt(20);

        switch (primitive.typeId()) {
            case BOOLEAN:
                return choice < 10;

            case INTEGER:
                return choice < 3 ? 0 : random.nextInt();

            case LONG:
                return choice < 3 ? 0L : random.nextLong();

            case FLOAT:
                return choice < 8 ? INIT_FLOATS[choice] : random.nextFloat();

            case DOUBLE:
                return choice < 8 ? INIT_DOUBLES[choice] : random.nextDouble();

            case DATE:
                return BASE_DAY.plusDays(random.nextInt(24) % 30);

            case TIME:
                return LocalTime.ofNanoOfDay(((random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_MS) * 1000);

            case TIMESTAMP:
                Types.TimestampType ts = (Types.TimestampType) primitive;
                if (ts.shouldAdjustToUTC()) {
                    return BASE_TIME.plus(random.nextLong() % ONE_DAY_MS, MICROS);
                } else {
                    return BASE_TIME.plus(random.nextLong() % ONE_DAY_MS, MICROS).toLocalDateTime();
                }

            case STRING:
                return randomString(random);

            case DECIMAL:
                Types.DecimalType type = (Types.DecimalType) primitive;
                BigInteger unscaled = randomUnscaled(type.precision(), random);
                return new BigDecimal(unscaled, type.scale());

            default:
                throw new IllegalArgumentException( "Cannot generate random value for unknown type: " + primitive);
        }
    }

    protected static String randomString(Random random) {
        int length = random.nextInt(50);
        byte[] buffer = new byte[length];

        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) CHARS.charAt(random.nextInt(CHARS.length()));
        }

        return new String(buffer, StandardCharsets.UTF_8);
    }

    protected static BigInteger randomUnscaled(int precision, Random random) {
        int length = random.nextInt(precision);
        if (length == 0) {
            return BigInteger.ZERO;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i += 1) {
            sb.append(DIGITS.charAt(random.nextInt(DIGITS.length())));
        }

        return new BigInteger(sb.toString());
    }

    private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {

        private final Random random;

        private RandomDataGenerator(long seed) {
            this.random = new Random(seed);
        }

        @Override
        public Record schema(Schema schema, Supplier<Object> structResult) {
            return (Record) structResult.get();
        }

        @Override
        public Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
            Record rec = GenericRecord.create(struct);

            List<Object> values = Lists.newArrayList(fieldResults);
            for (int i = 0; i < values.size(); i += 1) {
                rec.set(i, values.get(i));
            }

            return rec;
        }

        @Override
        public Object field(Types.NestedField field, Supplier<Object> fieldResult) {
            if (field.isOptional() && random.nextInt(20) == 1) {
                return null;
            }

            return fieldResult.get();
        }

        @Override
        public Object primitive(Type.PrimitiveType primitive) {
            return generatePrimitive(primitive, random);
        }
    }
}