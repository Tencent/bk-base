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

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class MinutexOptimizer extends ExpressionReplacementOptimizer {

    protected static final String DTEVENTTIMESTAMP = "dtEventTimestamp";
    protected static final Pattern MINUTE_X_PATTERN = Pattern.compile("mini?ute(\\d+)");
    protected static String DATE_FORMAT;
    protected static String CONVERT_FUNC;

    @JsonCreator
    public MinutexOptimizer(@JsonProperty("dateFormat") String dateFormat,
            @JsonProperty("convertFunc") String convertFunc) {
        DATE_FORMAT = dateFormat;
        CONVERT_FUNC = convertFunc;
    }

    @Override
    protected SqlNode enterSelectColumn(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        Matcher matcher = MINUTE_X_PATTERN.matcher(columnName);
        if (matcher.matches()) {
            SqlBasicCall asCall = convertMinuteXToFunc(column, matcher, true);
            return asCall;
        }
        return column;
    }

    @Override
    protected SqlNode enterColumnExpression(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        Matcher matcher = MINUTE_X_PATTERN.matcher(columnName);
        if (matcher.matches()) {
            SqlBasicCall function = convertMinuteXToFunc(column, matcher, false);
            return function;
        }
        return column;
    }

    @Override
    protected SqlBasicCall enterAsExpression(SqlBasicCall asExpression) {
        SqlNode leftAs = asExpression.operand(0);
        SqlNode rightAs = asExpression.operand(1);
        if (leftAs instanceof SqlIdentifier) {
            SqlIdentifier column = (SqlIdentifier) leftAs;
            String columnName = column.names.get(column.names.size() - 1);
            Matcher matcher = MINUTE_X_PATTERN.matcher(columnName);
            if (matcher.matches()) {
                SqlBasicCall asCall = convertMinuteXToFunc(rightAs, matcher, true);
                return asCall;
            }
        }
        return asExpression;
    }

    private SqlBasicCall convertMinuteXToFunc(SqlNode asParam, Matcher matcher,
            boolean convertToAs) {
        SqlUnresolvedFunction function = new SqlUnresolvedFunction(
                new SqlIdentifier(Lists.newArrayList(CONVERT_FUNC), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        SqlBasicCall fromUnixFunc = new SqlBasicCall(function,
                getFuncParams(Integer.parseInt(matcher.group(1))), SqlParserPos.ZERO);
        if (!convertToAs) {
            return fromUnixFunc;
        } else {
            SqlNode[] asParams = new SqlNode[2];
            asParams[0] = fromUnixFunc;
            asParams[1] = asParam;
            return new SqlBasicCall(SqlStdOperatorTable.AS, asParams, SqlParserPos.ZERO);
        }
    }

    /**
     * 获取函数参数列表
     *
     * @param minutes 分钟数
     * @return 函数参数列表
     */
    protected SqlNode[] getFuncParams(int minutes) {
        final SqlNode[] params = new SqlNode[2];
        SqlIdentifier modLeft = new SqlIdentifier(Lists.newArrayList(DTEVENTTIMESTAMP),
                SqlParserPos.ZERO);
        SqlNumericLiteral modRight = SqlLiteral
                .createExactNumeric(Integer.toString(minutes * 60000), SqlParserPos.ZERO);
        SqlBasicCall modExp = new SqlBasicCall(SqlStdOperatorTable.PERCENT_REMAINDER,
                SqlNodeList.of(modLeft, modRight)
                        .toArray(), SqlParserPos.ZERO);
        SqlNode minusLeft = new SqlIdentifier(Lists.newArrayList(DTEVENTTIMESTAMP),
                SqlParserPos.ZERO);
        SqlNode minusRight = modExp;
        SqlBasicCall minusExp = new SqlBasicCall(SqlStdOperatorTable.MINUS,
                SqlNodeList.of(minusLeft, minusRight)
                        .toArray(), SqlParserPos.ZERO);
        SqlNode divideLeft = minusExp;
        SqlNode divideRight = SqlLiteral.createExactNumeric("1000", SqlParserPos.ZERO);

        params[0] = new SqlBasicCall(SqlStdOperatorTable.DIVIDE,
                SqlNodeList.of(divideLeft, divideRight)
                        .toArray(), SqlParserPos.ZERO);
        params[1] = SqlLiteral.createCharString(DATE_FORMAT, SqlParserPos.ZERO);
        return params;
    }
}
