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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import com.beust.jcommander.internal.Lists;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class DruidTimeIntervalOptimizer extends ExpressionReplacementOptimizer {

    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    public static final String TIME_FLOOR = "TIME_FLOOR";
    private static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d+)([dhm])");
    private static final String TIME = "time";
    private static final String TODAY = "today";

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    @Override
    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        return convertTimeToFunc(call);
    }

    private SqlBasicCall convertTimeToFunc(SqlBasicCall call) {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        SqlIdentifier column;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            column = (SqlIdentifier) left;
            value = right;
        } else if (right instanceof SqlIdentifier) {
            column = (SqlIdentifier) right;
            value = left;
        } else {
            return super.enterEqualsExpression(call);
        }
        String columnName = column.names.get(column.names.size() - 1);
        boolean skipConvert = (!TIME
                .equals(QuoteUtil.trimQuotes(columnName))
                || !(value instanceof SqlCharStringLiteral));
        if (skipConvert) {
            return super.enterEqualsExpression(call);
        }
        String timeValue = QuoteUtil.trimQuotes(((SqlCharStringLiteral) value).toValue());
        Matcher matcher = INTERVAL_PATTERN.matcher(timeValue);
        if (TODAY.equals(timeValue)) {
            return getTimeFloorFunc(call);
        } else if (matcher.matches()) {
            return getCurrentTimeStampExp(call, matcher);
        }
        return super.enterEqualsExpression(call);
    }

    /**
     * 生成 CURRENT_TIMESTAMP 表达式
     *
     * @param call 原始 SqlBasicCall 实例
     * @param matcher Matcher 实例
     * @return CURRENT_TIMESTAMP 表达式
     */
    private SqlBasicCall getCurrentTimeStampExp(SqlBasicCall call, Matcher matcher) {
        String length = matcher.group(1);
        String unit = matcher.group(2);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = new SqlIdentifier(CURRENT_TIMESTAMP, SqlParserPos.ZERO);
        SqlBasicCall timeExp;
        switch (unit) {
            case "d":
                sqlNodes[1] = SqlLiteral.createInterval(1, length,
                        new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO),
                        SqlParserPos.ZERO);
                timeExp = new SqlBasicCall(SqlStdOperatorTable.MINUS, sqlNodes,
                        SqlParserPos.ZERO);
                call.setOperand(1, timeExp);
                return call;
            case "h":
                sqlNodes[1] = SqlLiteral.createInterval(1, length,
                        new SqlIntervalQualifier(TimeUnit.HOUR, null,
                                SqlParserPos.ZERO), SqlParserPos.ZERO);
                timeExp = new SqlBasicCall(SqlStdOperatorTable.MINUS, sqlNodes,
                        SqlParserPos.ZERO);
                call.setOperand(1, timeExp);
                return call;
            case "m":
                sqlNodes[1] = SqlLiteral.createInterval(1, length,
                        new SqlIntervalQualifier(TimeUnit.MINUTE, null,
                                SqlParserPos.ZERO), SqlParserPos.ZERO);
                timeExp = new SqlBasicCall(SqlStdOperatorTable.MINUS, sqlNodes,
                        SqlParserPos.ZERO);
                call.setOperand(1, timeExp);
                return call;
            default:
                throw new IllegalStateException(unit);
        }
    }

    /**
     * 生成 TimeFloor 函数表达式
     *
     * @param call 原始 SqlBasicCall 实例
     * @return TimeFloor 函数表达式
     */
    private SqlBasicCall getTimeFloorFunc(SqlBasicCall call) {
        final SqlUnresolvedFunction timeFloorFunction = new SqlUnresolvedFunction(
                new SqlIdentifier(Lists.newArrayList(TIME_FLOOR), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        SqlNode[] sqlNodes = new SqlNode[4];
        sqlNodes[0] = new SqlIdentifier(CURRENT_TIMESTAMP, SqlParserPos.ZERO);
        sqlNodes[1] = SqlLiteral.createCharString("P1D", SqlParserPos.ZERO);

        SqlNode[] castSqlNodes = new SqlNode[2];
        castSqlNodes[0] = SqlLiteral.createNull(SqlParserPos.ZERO);
        SqlBasicTypeNameSpec timestampType = new SqlBasicTypeNameSpec(
                SqlTypeName.TIMESTAMP, -1, -1, null, SqlParserPos.ZERO);
        castSqlNodes[1] = new SqlDataTypeSpec(timestampType, SqlParserPos.ZERO);
        sqlNodes[2] = new SqlBasicCall(SqlStdOperatorTable.CAST, castSqlNodes,
                SqlParserPos.ZERO);

        sqlNodes[3] = SqlLiteral.createCharString("+0800", SqlParserPos.ZERO);
        SqlBasicCall todayExp = new SqlBasicCall(timeFloorFunction, sqlNodes,
                SqlParserPos.ZERO);
        call.setOperand(1, todayExp);
        return call;
    }
}