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

import static com.tencent.bk.base.datalab.bksql.util.SqlNodeUtil.convertToString;
import static com.tencent.bk.base.datalab.queryengine.constants.PrestoConstants.THEDATE;

import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.math.NumberUtils;

public class PrestoTheDateConvertOptimizer extends ExpressionReplacementOptimizer {

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        return convertSimpleComparableExpression(call);
    }

    @Override
    protected SqlBasicCall enterBetweenExpression(SqlBasicCall call) {
        final SqlNode columnNode = call.operand(0);
        final SqlNode startNode = call.operand(1);
        final SqlNode endNode = call.operand(2);
        if (!(columnNode instanceof SqlIdentifier)) {
            return call;
        }
        SqlIdentifier column = (SqlIdentifier) columnNode;
        if (checkColumnName(column)) {
            return call;
        }
        if (startNode instanceof SqlCharStringLiteral) {
            SqlNode operand = startNode;
            SqlNumericLiteral convertedThedate = convertTheDate(operand);
            call.setOperand(1, convertedThedate);
        }
        if (endNode instanceof SqlCharStringLiteral) {
            SqlNode operand = endNode;
            SqlNumericLiteral convertedThedate = convertTheDate(operand);
            call.setOperand(2, convertedThedate);
        }
        return call;
    }

    private SqlNumericLiteral convertTheDate(SqlNode operand) {
        String theDateStr = RegExUtils
                .replaceAll(convertToString(operand), "\r|\n", "");
        if (!NumberUtils.isDigits(theDateStr)) {
            fail("illegal.thedate.format");
        }
        return SqlLiteral
                .createExactNumeric(theDateStr, SqlParserPos.ZERO);
    }

    private SqlBasicCall convertSimpleComparableExpression(SqlBasicCall call) {
        final SqlIdentifier column;
        final SqlNode value;
        SqlNode left = call.getOperands()[0];
        SqlNode right = call.getOperands()[1];
        boolean leftRightShouldConvert =
                left instanceof SqlIdentifier && right instanceof SqlCharStringLiteral;
        boolean rightLeftShouldConvert =
                right instanceof SqlIdentifier && left instanceof SqlCharStringLiteral;
        if (leftRightShouldConvert) {
            column = (SqlIdentifier) left;
            value = right;
        } else if (rightLeftShouldConvert) {
            column = (SqlIdentifier) right;
            value = left;
        } else {
            return call;
        }
        if (checkColumnName(column)) {
            return call;
        }
        SqlNumericLiteral convertedThedate = convertTheDate(value);
        if (leftRightShouldConvert) {
            call.setOperand(1, convertedThedate);
        } else {
            call.setOperand(0, convertedThedate);
        }
        return call;
    }

    private boolean checkColumnName(SqlIdentifier column) {
        String columnName;
        columnName = column.names.get(column.names.size() - 1);
        boolean isTheDate = THEDATE.equalsIgnoreCase(columnName);
        if (!isTheDate) {
            return true;
        }
        return false;
    }
}
