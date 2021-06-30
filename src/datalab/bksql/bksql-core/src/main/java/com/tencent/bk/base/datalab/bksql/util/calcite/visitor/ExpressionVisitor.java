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

package com.tencent.bk.base.datalab.bksql.util.calcite.visitor;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlWithinGroupOperator;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlJsonArrayFunction;
import org.apache.calcite.sql.fun.SqlJsonQueryFunction;
import org.apache.calcite.sql.fun.SqlJsonValueFunction;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlPosixRegexOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlRegexpReplaceFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public interface ExpressionVisitor<R> extends DefaultSqlVisitor<R> {

    default R visit(SqlIntervalLiteral intervalValue) {
        return null;
    }

    default R visit(SqlNumericLiteral numericLiteral) {
        return null;
    }

    default R visit(SqlBinaryStringLiteral binaryString) {
        return null;
    }

    default R visit(SqlCharStringLiteral charString) {
        return null;
    }

    default R visit(SqlDateLiteral dateLiteral) {
        return null;
    }

    default R visit(SqlTimeLiteral timeLiteral) {
        return null;
    }

    default R visit(SqlTimestampLiteral timestampLiteral) {
        return null;
    }

    default R visit(SqlBinaryOperator binaryOperator) {
        return null;
    }

    default R visit(SqlBetweenOperator between) {
        return null;
    }

    default R visit(SqlInOperator inOperator) {
        return null;
    }

    default R visit(SqlPostfixOperator postfixOperator) {
        return null;
    }

    default R visit(SqlLikeOperator likeOperator) {
        return null;
    }

    default R visit(SqlSelect subSelect) {
        return null;
    }

    default R visit(SqlCase sqlCase) {
        return null;
    }

    default R visit(SqlStdOperatorTable exists) {
        return null;
    }

    default R visit(SqlQuantifyOperator all) {
        return null;
    }

    default R visit(SqlOverOperator overOperator) {
        return null;
    }

    default R visit(SqlWithinGroupOperator withinGroupOperator) {
        return null;
    }

    default R visit(SqlExtractFunction extractFunction) {
        return null;
    }

    default R visit(SqlInternalOperator sqlInternalOperator) {
        return null;
    }

    default R visit(SqlRegexpReplaceFunction regexpReplaceFunction) {
        return null;
    }

    default R visit(SqlJsonArrayFunction jsonArrayFunction) {
        return null;
    }

    default R visit(SqlJsonValueFunction jsonValueFunction) {
        return null;
    }

    default R visit(SqlJsonQueryFunction jsonQueryFunction) {
        return null;
    }

    default R visit(SqlPosixRegexOperator posixRegexOperator) {
        return null;
    }

    default R visit(SqlRowOperator rowOperator) {
        return null;
    }

    /**
     * 默认SqlBasicCall访问逻辑
     *
     * @param call SqlBasicCall实例
     * @return null 无返回
     */
    default R visit(SqlBasicCall call) {
        SqlKind kind = call.getKind();
        switch (kind) {
            case IN:
                visitInExpression(call);
                break;
            case LIKE:
                visitLikeExpression(call);
                break;
            case AS:
                visitAsExpression(call);
                break;
            case OTHER_FUNCTION:
            case CREATE_FUNCTION:
            case DROP_FUNCTION:
                visitFunction(call);
                break;
            case BETWEEN:
                visitBetweenExpression(call);
                break;
            case OVER:
                visitOverExpression(call);
                break;
            default:
                SqlOperator operator = call.getOperator();
                if (operator instanceof SqlPrefixOperator) {
                    visitPrefixExpression(call);
                } else if (operator instanceof SqlPostfixOperator) {
                    visitPostfixExpression(call);
                } else if (operator instanceof SqlBinaryOperator) {
                    visitBinaryExpression(call);
                } else if (operator instanceof SqlFunction) {
                    visitFunction(call);
                }
        }
        return null;
    }

    default R visitNull(SqlLiteral nullValue) {
        return null;
    }

    default R visitSignedExpression(SqlBasicCall function) {
        return null;
    }

    default R visitFunction(SqlBasicCall function) {
        return null;
    }

    default R visitOverExpression(SqlBasicCall call) {
        return null;
    }

    default R visitInExpression(SqlBasicCall inExpression) {
        return null;
    }

    default R visitNotInExpression(SqlBasicCall inExpression) {
        return null;
    }

    default R visitLikeExpression(SqlBasicCall likeExpression) {
        return null;
    }

    default R visitBetweenExpression(SqlBasicCall call) {
        return null;
    }

    default R visitPrefixExpression(SqlBasicCall call) {
        return null;
    }

    default R visitPostfixExpression(SqlBasicCall call) {
        return null;
    }

    default R visitAsExpression(SqlBasicCall call) {
        return null;
    }

    default R visitBinaryExpression(SqlBasicCall call) {
        return null;
    }

    default R visitPlusExpression(SqlBasicCall call) {
        return null;
    }

    default R visitMinusExpression(SqlBasicCall call) {
        return null;
    }

    default R visitTimesExpression(SqlBasicCall call) {
        return null;
    }

    default R visitDivideExpression(SqlBasicCall call) {
        return null;
    }

    default R visitModExpression(SqlBasicCall call) {
        return null;
    }

    default R visitAndExpression(SqlBasicCall call) {
        return null;
    }

    default R visitOrExpression(SqlBasicCall call) {
        return null;
    }

    default R visitNotExpression(SqlBasicCall call) {
        return null;
    }

    default R visitEqualsExpression(SqlBasicCall call) {
        return null;
    }

    default R visitNotEqualsExpression(SqlBasicCall call) {
        return null;
    }

    default R visitGreaterThanExpression(SqlBasicCall call) {
        return null;
    }

    default R visitGreaterThanEqualsExpression(SqlBasicCall call) {
        return null;
    }

    default R visitLessThanExpression(SqlBasicCall call) {
        return null;
    }

    default R visitLessThanEqualsExpression(SqlBasicCall call) {
        return null;
    }
}
