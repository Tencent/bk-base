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

import static com.tencent.bk.base.datalab.bksql.util.SqlNodeUtil.convertToString;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

/**
 * Where中minuteX转成dteventtime WHERE minute5 = '20190704104500' ---> WHERE dteventtime = '2019-07-04
 * 10:45:00'
 */
public class WhereMinutexOptimizer extends ExpressionReplacementOptimizer {

    private static final String DTENEVTTIME = "dteventtime";
    private static final String DTENEVTTIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final Pattern MINUTE_X_PATTERN = Pattern.compile("mini?ute(\\d+)");
    private static final Map<Integer, String> MINUTE_X_FORMAT = ImmutableMap.of(
            14, "yyyyMMddHHmmss",
            12, "yyyyMMddHHmm",
            10, "yyyyMMddHH",
            8, "yyyyMMdd"
    );

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.EQUALS);
    }

    @Override
    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.NOT_EQUALS);
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.GREATER_THAN);
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.LESS_THAN);
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        return minuteXConversion(call, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    }

    private SqlBasicCall minuteXConversion(SqlBasicCall call, SqlBinaryOperator sqlBinaryOperator) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(sqlBinaryOperator, initOperands, SqlParserPos.ZERO);
        SqlBasicCall toConvert = convertToDtEventTimeCall(call, (c, v, antonymous) -> {
            String columnNamePrefix = Joiner.on(".").join(c.names.subList(0, c.names.size() - 1));
            String replaceColumnName = DTENEVTTIME;
            if (StringUtils.isNotBlank(columnNamePrefix)) {
                replaceColumnName = columnNamePrefix + "." + DTENEVTTIME;
            }

            try {
                Date date = new SimpleDateFormat(MINUTE_X_FORMAT.get(v.length())).parse(v);
                String dteventtime = new SimpleDateFormat(DTENEVTTIME_FORMAT).format(date);

                SqlNode[] operands = new SqlNode[2];
                SqlIdentifier column = new SqlIdentifier(replaceColumnName, SqlParserPos.ZERO);
                SqlCharStringLiteral value = SqlLiteral
                        .createCharString(dteventtime, SqlParserPos.ZERO);
                operands[0] = column;
                operands[1] = value;
                for (int i = 0; i < copyExp.getOperands().length; i++) {
                    copyExp.setOperand(i, operands[i]);
                }
                return copyExp;
            } catch (Exception e) {
                throw new IllegalArgumentException("minuteX字段格式错误，例子：20190703104500");
            }
        });
        return toConvert;
    }

    /**
     * 将包含minutex的表达式转换为dteventtime表达式
     *
     * @param copyExp sql表达式副本
     * @param convertFun 转换函数
     * @return 转换后的sql表达式
     */
    private SqlBasicCall convertToDtEventTimeCall(SqlBasicCall copyExp,
            TriFunction<SqlIdentifier, String, Boolean, SqlBasicCall> convertFun) {
        SqlNode left = copyExp.getOperands()[0];
        SqlNode right = copyExp.getOperands()[1];
        SqlIdentifier column;
        String columnName;
        SqlNode value;
        boolean antonymous;
        if (left instanceof SqlIdentifier) {
            column = (SqlIdentifier) left;
            columnName = column.names.get(column.names.size() - 1);
            value = right;
            antonymous = false;
        } else if (right instanceof SqlIdentifier) {
            column = (SqlIdentifier) right;
            columnName = column.names.get(column.names.size() - 1);
            value = left;
            antonymous = true;
        } else {
            return super.enterBinaryExpression(copyExp);
        }
        boolean isTheDate = MINUTE_X_PATTERN.matcher(columnName).matches();
        if (!isTheDate) {
            return super.enterBinaryExpression(copyExp);
        }
        String strVal = convertToString(value);
        return convertFun.apply(column, strVal, antonymous);
    }

    /**
     * 四参数转换函数
     *
     * @param <F> 参数一
     * @param <S> 参数二
     * @param <T> 参数三
     * @param <R> 返回值
     */
    private interface TriFunction<F, S, T, R> {

        R apply(F f, S s, T t);
    }
}