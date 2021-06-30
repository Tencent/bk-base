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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DruidTimeColumnOptimizer extends ExpressionReplacementOptimizer {

    private static final String TIME = "time";
    private boolean isLowVersion;

    @JsonCreator
    public DruidTimeColumnOptimizer(@JsonProperty("isLowVersion") boolean lowVersion) {
        isLowVersion = lowVersion;
    }

    @Override
    protected SqlNode enterSelectColumn(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        if (TIME.equals(columnName)) {
            return convertTimeToFunc(column, true);
        }
        return column;
    }

    @Override
    protected SqlNode enterColumnExpression(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        if (TIME.equals(columnName)) {
            return convertTimeToFunc(column, false);
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
            if (TIME.equals(columnName)) {
                return (SqlBasicCall) convertTimeToFunc(rightAs, true);
            }
        }
        return asExpression;
    }

    private SqlNode convertTimeToFunc(SqlNode asParam, boolean convertToAs) {
        SqlNode[] asParams = new SqlNode[2];
        asParams = getBeforeAsParam(asParams);
        if (!convertToAs) {
            return asParams[0];
        } else {
            asParams[1] = asParam;
            return new SqlBasicCall(SqlStdOperatorTable.AS, asParams, SqlParserPos.ZERO);
        }
    }

    /**
     * 获取 as 表达式的第一个参数
     *
     * @param asParams as 表达式参数列表
     * @return as 表达式的第一个参数
     */
    private SqlNode[] getBeforeAsParam(SqlNode[] asParams) {
        if (isLowVersion) {
            SqlUnresolvedFunction millsFunc = new SqlUnresolvedFunction(
                    new SqlIdentifier(Lists.newArrayList("MILLIS_TO_TIMESTAMP"), SqlParserPos.ZERO),
                    null,
                    null,
                    null,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
            SqlNode[] params = new SqlNode[1];
            params[0] = new SqlIdentifier("dtEventTimeStamp", SqlParserPos.ZERO);
            SqlBasicCall func = new SqlBasicCall(millsFunc, params, SqlParserPos.ZERO);
            asParams[0] = func;
        } else {
            asParams = new SqlNode[2];
            SqlIdentifier column = new SqlIdentifier("__time", SqlParserPos.ZERO);
            asParams[0] = column;
        }
        return asParams;
    }
}