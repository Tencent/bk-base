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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class BaseColumnReplacementOptimizer extends ExpressionReplacementOptimizer {

    private String replacedColumn;

    @JsonCreator
    protected BaseColumnReplacementOptimizer(
            @JsonProperty("replacedColumn") String replacedColumn) {
        this.replacedColumn = replacedColumn;
    }

    @Override
    protected SqlBasicCall enterBetweenExpression(SqlBasicCall between) {
        SqlBasicCall copyExp = (SqlBasicCall) between.clone(between.getParserPosition());
        SqlNode[] betweenOperands = copyExp.getOperands();
        if (betweenOperands[0] instanceof SqlIdentifier) {
            SqlIdentifier column = (SqlIdentifier) betweenOperands[0];
            String columnName = column.names.get(column.names.size() - 1);
            if (replacedColumn.equals(columnName)) {
                if (betweenOperands[1] instanceof SqlLiteral
                        && betweenOperands[2] instanceof SqlLiteral) {
                    final SqlLiteral start = (SqlLiteral) betweenOperands[1];
                    final SqlLiteral end = (SqlLiteral) betweenOperands[2];
                    SqlNode[] gteOperands = new SqlNode[2];
                    SqlIdentifier gteColumn = new SqlIdentifier(column.names,
                            copyExp.operands[0].getParserPosition());
                    gteOperands[0] = gteColumn;
                    gteOperands[1] = start;
                    SqlBasicCall gteExpression = new SqlBasicCall(
                            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, gteOperands,
                            between.getParserPosition());
                    SqlNode newGte = this.enterGreaterThanEqualsExpression(gteExpression);

                    SqlNode[] lteOperands = new SqlNode[2];
                    SqlIdentifier lteColumn = new SqlIdentifier(column.names,
                            copyExp.operands[0].getParserPosition());
                    lteOperands[0] = lteColumn;
                    lteOperands[1] = end;
                    SqlBasicCall lteExpression = new SqlBasicCall(
                            SqlStdOperatorTable.LESS_THAN_OR_EQUAL, lteOperands,
                            between.getParserPosition());
                    SqlNode newLte = this.enterLessThanEqualsExpression(lteExpression);
                    SqlNode[] andOperands = new SqlNode[2];
                    andOperands[0] = newGte;
                    andOperands[1] = newLte;
                    SqlBasicCall andExpression = new SqlBasicCall(SqlStdOperatorTable.AND,
                            andOperands, between.getParserPosition());
                    return andExpression;
                }
            }
        }
        return copyExp;
    }

    /**
     * 校验是否存在分区列
     *
     * @param binaryExpression 二元表达式
     * @return 校验结果
     */
    protected boolean checkReplacementColumn(SqlBasicCall binaryExpression) {
        SqlNode[] operands = binaryExpression.getOperands();
        SqlNode left = operands[0];
        SqlNode right = operands[1];
        SqlIdentifier column;
        String columnName;
        if (left instanceof SqlIdentifier) {
            column = (SqlIdentifier) left;
        } else if (right instanceof SqlIdentifier) {
            column = (SqlIdentifier) right;
        } else {
            return false;
        }
        columnName = column.names.get(column.names.size() - 1);
        boolean isPartitionColumn = replacedColumn.equalsIgnoreCase(columnName);
        return isPartitionColumn;
    }

    protected interface TriFunction<F, S, T, R> {

        /**
         * 四参数转换函数
         *
         * @param f 参数一
         * @param s 参数二
         * @param t 参数三
         * @return R 返回值
         */
        R apply(F f, S s, T t);
    }
}
