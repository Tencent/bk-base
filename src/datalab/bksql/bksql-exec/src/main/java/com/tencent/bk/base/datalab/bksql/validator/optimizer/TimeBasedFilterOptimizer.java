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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import static com.tencent.bk.base.datalab.bksql.util.SqlNodeUtil.and;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

public class TimeBasedFilterOptimizer extends ExpressionReplacementOptimizer {

    public static final String DEFAULT_MILLI_KEY = "dtEventTimeStamp";
    public static final String THEDATE = "thedate";
    public static final String DTEVENTTIME = "dteventtime";
    private static final String BK_TIMEZONE = "BK_TIMEZONE";
    private static final String ASIA = "Asia/Shanghai";

    @Override
    protected SqlBasicCall enterBetweenExpression(SqlBasicCall between) {
        SqlNode[] gteParams = new SqlNode[2];
        gteParams[0] = between.operand(0);
        gteParams[1] = between.operand(1);
        SqlBasicCall gte = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, gteParams,
                SqlParserPos.ZERO);
        SqlCall convertGteCall = asymmetricallyConvert(gte, gte, (z, c) -> z.toInstant()
                .toEpochMilli());

        SqlNode[] lteParams = new SqlNode[2];
        lteParams[0] = between.operand(0);
        lteParams[1] = between.operand(2);
        SqlBasicCall lte = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, lteParams,
                SqlParserPos.ZERO);
        SqlCall convertLteCall = asymmetricallyConvert(
                lte, lte, (z, c) -> z.toInstant()
                        .plus(c.precision)
                        .minusMillis(1L)
                        .toEpochMilli());
        if (convertGteCall == null || convertLteCall == null) {
            return between;
        }
        return and(and(gte, lte), between);
    }

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall andExp = new SqlBasicCall(SqlStdOperatorTable.AND, initOperands,
                SqlParserPos.ZERO);
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        SqlIdentifier col;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            col = (SqlIdentifier) left;
            value = right;
        } else if (right instanceof SqlIdentifier) {
            col = (SqlIdentifier) right;
            value = left;
        } else {
            return super.enterEqualsExpression(call);
        }
        SqlNumericLiteral lowerBound =
                createLongInstant((z, c) -> z.toInstant()
                        .toEpochMilli(), col, value, false);
        SqlNumericLiteral upperBound =
                createLongInstant((z, c) -> z.toInstant()
                        .plus(c.precision)
                        .toEpochMilli(), col, value, false);
        if (lowerBound != null && upperBound != null) {
            SqlNode[] gteParams = new SqlNode[2];
            String columnNamePrefix = Joiner.on(".")
                    .join(col.names.subList(0, col.names.size() - 1));
            String replaceColumnName = DEFAULT_MILLI_KEY;
            if (org.apache.commons.lang3.StringUtils.isNotBlank(columnNamePrefix)) {
                replaceColumnName = columnNamePrefix + "." + DEFAULT_MILLI_KEY;
            }
            gteParams[0] = new SqlIdentifier(replaceColumnName, SqlParserPos.ZERO);
            gteParams[1] = lowerBound;
            SqlBasicCall gte = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    gteParams, SqlParserPos.ZERO);
            SqlNode[] ltParams = new SqlNode[2];
            ltParams[0] = new SqlIdentifier(replaceColumnName, SqlParserPos.ZERO);
            ltParams[1] = upperBound;
            SqlBasicCall lt = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, ltParams,
                    SqlParserPos.ZERO);
            andExp.setOperand(0, gte);
            andExp.setOperand(1, lt);
            return and(call, andExp);
        }
        return super.enterEqualsExpression(call);
    }

    @Override
    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall orExp = new SqlBasicCall(SqlStdOperatorTable.OR, initOperands,
                SqlParserPos.ZERO);
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        SqlIdentifier col;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            col = (SqlIdentifier) left;
            value = right;
        } else if (right instanceof SqlIdentifier) {
            col = (SqlIdentifier) right;
            value = left;
        } else {
            return super.enterNotEqualsExpression(call);
        }
        SqlNumericLiteral lowerBound =
                createLongInstant((z, c) -> z.toInstant()
                        .toEpochMilli(), col, value, false);
        SqlNumericLiteral upperBound =
                createLongInstant((z, c) -> z.toInstant()
                        .plus(c.precision)
                        .toEpochMilli(), col, value, false);
        if (lowerBound != null && upperBound != null) {
            SqlNode[] gtParams = new SqlNode[2];
            String columnNamePrefix = Joiner.on(".")
                    .join(col.names.subList(0, col.names.size() - 1));
            String replaceColumnName = DEFAULT_MILLI_KEY;
            if (org.apache.commons.lang3.StringUtils.isNotBlank(columnNamePrefix)) {
                replaceColumnName = columnNamePrefix + "." + DEFAULT_MILLI_KEY;
            }
            gtParams[0] = new SqlIdentifier(replaceColumnName, SqlParserPos.ZERO);
            gtParams[1] = upperBound;
            SqlBasicCall gt = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, gtParams,
                    SqlParserPos.ZERO);
            SqlNode[] ltParams = new SqlNode[2];
            ltParams[0] = new SqlIdentifier(replaceColumnName, SqlParserPos.ZERO);
            ltParams[1] = lowerBound;
            SqlBasicCall lt = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, ltParams,
                    SqlParserPos.ZERO);
            orExp.setOperand(0, gt);
            orExp.setOperand(1, lt);
            return and(call, orExp);
        }
        return super.enterNotEqualsExpression(call);
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall gtExp = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, initOperands,
                SqlParserPos.ZERO);
        SqlBasicCall convertCall = asymmetricallyConvert(
                call, gtExp, (z, c) -> z.toInstant()
                        .plus(c.precision)
                        .minusMillis(1L)
                        .toEpochMilli());
        if (convertCall != null) {
            return and(call, convertCall);
        }
        return call;
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall gteExp = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                initOperands, SqlParserPos.ZERO);
        SqlBasicCall convertCall = asymmetricallyConvert(call, gteExp, (z, c) -> z.toInstant()
                .toEpochMilli());
        if (convertCall != null) {
            return and(call, convertCall);
        }
        return call;
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall ltExp = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, initOperands,
                SqlParserPos.ZERO);
        SqlBasicCall convertCall = asymmetricallyConvert(call, ltExp, (z, c) -> z.toInstant()
                .toEpochMilli());
        if (convertCall != null) {
            return and(call, convertCall);
        }
        return call;
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall lteExp = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, initOperands,
                SqlParserPos.ZERO);
        SqlBasicCall convertCall = asymmetricallyConvert(
                call, lteExp, (z, c) -> z.toInstant()
                        .plus(c.precision)
                        .minusMillis(1L)
                        .toEpochMilli());
        if (convertCall != null) {
            return and(call, convertCall);
        }
        return call;
    }

    private SqlBasicCall asymmetricallyConvert(
            SqlBasicCall bin, SqlBasicCall copyBin,
            BiFunction<ZonedDateTime, TimeStampConvention, Long> timeFn) {
        SqlNode left = bin.operand(0);
        SqlNode right = bin.operand(1);
        SqlIdentifier col;
        String columnName;
        SqlNode value;
        boolean antonymous;
        Consumer<List<String>> columnNameSetter;
        Consumer<Long> valueSetter;
        if (left instanceof SqlIdentifier) {
            col = (SqlIdentifier) left;
            columnName = col.names.get(col.names.size() - 1);
            value = right;
            antonymous = false;
            valueSetter = l -> copyBin
                    .setOperand(1, SqlLiteral.createExactNumeric(l + "", SqlParserPos.ZERO));
            columnNameSetter = c -> copyBin.setOperand(0, new SqlIdentifier(c, SqlParserPos.ZERO));
        } else if (right instanceof SqlIdentifier) {
            col = (SqlIdentifier) right;
            columnName = col.names.get(col.names.size() - 1);
            value = left;
            antonymous = true;
            valueSetter = l -> copyBin
                    .setOperand(0, SqlLiteral.createExactNumeric(l + "", SqlParserPos.ZERO));
            columnNameSetter = c -> copyBin.setOperand(1, new SqlIdentifier(c, SqlParserPos.ZERO));
        } else {
            return null;
        }
        boolean isConvertColumn = StringUtils.equalsIgnoreCase(THEDATE, columnName) || StringUtils
                .equalsIgnoreCase(DTEVENTTIME, columnName);
        if (!isConvertColumn) {
            return null;
        }
        SqlNumericLiteral instant = createLongInstant(timeFn, col, value, antonymous);
        if (instant == null) {
            return null;
        }
        List<String> repNames = Lists.newArrayList();
        List<String> orgNamePrefix = col.names.subList(0, col.names.size() - 1);
        if (orgNamePrefix != null && !orgNamePrefix.isEmpty()) {
            repNames.addAll(orgNamePrefix);
        }
        repNames.add(DEFAULT_MILLI_KEY);
        valueSetter.accept(instant.longValue(true));
        columnNameSetter.accept(repNames);
        return copyBin;
    }

    private SqlNumericLiteral createLongInstant(
            BiFunction<ZonedDateTime, TimeStampConvention, Long> timeFn,
            SqlIdentifier col,
            SqlNode value,
            boolean antonymous) {
        String keyString = col.names.get(col.names.size() - 1);
        String valueString;
        if (value instanceof SqlCharStringLiteral) {
            valueString = ((SqlCharStringLiteral) value).toValue();
        } else if (value instanceof SqlNumericLiteral) {
            valueString = String.valueOf(((SqlNumericLiteral) value).getValue());
        } else {
            return null;
        }
        valueString = RegExUtils.replaceAll(valueString, "\r|\n", "");
        if (StringUtils.isBlank(valueString)) {
            return null;
        }
        for (TimeStampConvention convention : TimeStampConvention.values()) {
            if (keyString.equalsIgnoreCase(convention.key) && valueString.length() == convention.length) {
                DateTimeFormatter formatter =
                        new DateTimeFormatterBuilder()
                                .appendPattern(convention.pattern)
                                .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
                                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                                .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                                .parseDefaulting(ChronoField.YEAR_OF_ERA, 2008)
                                .toFormatter();
                LocalDateTime localDateTime = null;
                try {
                    localDateTime = LocalDateTime.parse(valueString, formatter);
                } catch (DateTimeParseException e) {
                    if (keyString.equalsIgnoreCase(DTEVENTTIME)) {
                        fail("illegal.dteventtime.format");
                    } else if (keyString.equalsIgnoreCase(THEDATE)) {
                        fail("illegal.thedate.format");
                    } else {
                        throw new RuntimeException("failed to parse time column");
                    }

                }
                ZoneId zoneId = ZoneId.of(System.getProperty(BK_TIMEZONE, ASIA));
                ZonedDateTime zonedDateTime = ZonedDateTime
                        .of(localDateTime, zoneId);
                long truncated = timeFn.apply(zonedDateTime, convention);
                if (antonymous) {
                    truncated =
                            zonedDateTime.toInstant()
                                    .toEpochMilli() * 2
                                    + convention.precision.toMillis()
                                    - 1L
                                    - truncated;
                }
                return SqlLiteral.createExactNumeric(truncated + "", SqlParserPos.ZERO);
            }
        }
        return null;
    }

    /**
     * 日期时间枚举
     */
    private enum TimeStampConvention {
        /**
         * thedate日期格式 yyyyMMdd
         */
        THE_DATE("thedate", 8, "yyyyMMdd", Duration.ofDays(1L)),
        /**
         * dtEventTime日期格式 yyyy-MM-dd
         */
        DT_EVENT_TIME_DAY("dtEventTime", 10, "yyyy-MM-dd", Duration.ofDays(1L)),
        /**
         * dtEventTime日期格式 yyyy-MM-dd HH
         */
        DT_EVENT_TIME_HOUR("dtEventTime", 13, "yyyy-MM-dd HH", Duration.ofHours(1L)),

        /**
         * dtEventTime日期格式 yyyy-MM-dd HH:
         */
        DT_EVENT_TIME_HOUR2("dtEventTime", 14, "yyyy-MM-dd HH:", Duration.ofHours(1L)),

        /**
         * dtEventTime日期格式 yyyy-MM-dd HH:mm:
         */
        DT_EVENT_TIME_MINUTE("dtEventTime", 16, "yyyy-MM-dd HH:mm:", Duration.ofMinutes(1L)),

        /**
         * dtEventTime日期格式 yyyy-MM-dd HH:mm:
         */
        DT_EVENT_TIME_MINUTE2("dtEventTime", 17, "yyyy-MM-dd HH:mm:", Duration.ofMinutes(1L)),

        /**
         * dtEventTime日期格式 yyyy-MM-dd HH:mm:ss
         */
        DT_EVENT_TIME_SECOND("dtEventTime", 19, "yyyy-MM-dd HH:mm:ss", Duration.ofSeconds(1L));

        private final String key;
        private final int length;
        private final String pattern;
        private final Duration precision;

        TimeStampConvention(String key, int length, String pattern, Duration precision) {
            this.key = key;
            this.length = length;
            this.pattern = pattern;
            this.precision = precision;
        }
    }
}
