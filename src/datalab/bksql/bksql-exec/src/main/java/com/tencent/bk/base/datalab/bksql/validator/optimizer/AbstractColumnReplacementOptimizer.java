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
import static com.tencent.bk.base.datalab.bksql.util.SqlNodeUtil.convertToString;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RegExUtils;

public abstract class AbstractColumnReplacementOptimizer extends BaseColumnReplacementOptimizer {

    private String replaceToColumn;
    private boolean keepReplacedColumn;
    private boolean containCompareBounday;

    protected AbstractColumnReplacementOptimizer(String replacedColumn,
            String replaceToColumn, boolean keepReplacedColumn, boolean containCompareBounday) {
        super(replacedColumn);
        this.replaceToColumn = replaceToColumn;
        this.keepReplacedColumn = keepReplacedColumn;
        this.containCompareBounday = containCompareBounday;
    }

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        boolean isDtEventTimeColumn = checkReplacementColumn(call);
        if (!isDtEventTimeColumn) {
            return call;
        }
        SqlBasicCall toConvert = replaceColumn(call, (c, v, antonymous) ->
                getSimpleConvertNode(call, c, v, antonymous, SqlStdOperatorTable.EQUALS));
        if (toConvert == call) {
            return call;
        }
        return keepReplacedColumn ? and(toConvert, call) : toConvert;
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        boolean isDtEventTimeColumn = checkReplacementColumn(call);
        if (!isDtEventTimeColumn) {
            return call;
        }
        SqlOperator toConvertOperator =
                containCompareBounday ? GREATER_THAN_OR_EQUAL : GREATER_THAN;
        SqlBasicCall toConvert = replaceColumn(call, (c, v, antonymous) ->
                getSimpleConvertNode(call, c, v, antonymous,
                        toConvertOperator));
        if (toConvert == call) {
            return call;
        }
        return keepReplacedColumn ? and(toConvert, call) : toConvert;
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        boolean isDtEventTimeColumn = checkReplacementColumn(call);
        if (!isDtEventTimeColumn) {
            return call;
        }
        SqlBasicCall toConvert = replaceColumn(call, (c, v, antonymous) ->
                getSimpleConvertNode(call, c, v, antonymous,
                        GREATER_THAN_OR_EQUAL));
        if (toConvert == call) {
            return call;
        }
        return keepReplacedColumn ? and(toConvert, call) : toConvert;
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        boolean isDtEventTimeColumn = checkReplacementColumn(call);
        if (!isDtEventTimeColumn) {
            return call;
        }
        SqlOperator toConvertOperator =
                containCompareBounday ? LESS_THAN_OR_EQUAL : LESS_THAN;
        SqlBasicCall toConvert = replaceColumn(call, (c, v, antonymous) ->
                getSimpleConvertNode(call, c, v, antonymous,
                        toConvertOperator));
        if (toConvert == call) {
            return call;
        }
        return keepReplacedColumn ? and(toConvert, call) : toConvert;
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        boolean isDtEventTimeColumn = checkReplacementColumn(call);
        if (!isDtEventTimeColumn) {
            return call;
        }
        SqlBasicCall toConvert = replaceColumn(call, (c, v, antonymous) ->
                getSimpleConvertNode(call, c, v, antonymous,
                        LESS_THAN_OR_EQUAL));
        if (toConvert == call) {
            return call;
        }
        return keepReplacedColumn ? and(toConvert, call) : toConvert;
    }

    protected abstract String getReplacementColumnValue(String strVal);

    protected SqlNode getConvertToValueNode(String v) {
        return SqlLiteral
                .createExactNumeric(v, SqlParserPos.ZERO);
    }

    /**
     * 分区转换方法
     *
     * @param call sql表达式
     * @param convertFun 转换函数
     * @return 转换后的表达式
     */
    private SqlBasicCall replaceColumn(SqlBasicCall call,
            TriFunction<SqlIdentifier, String, Boolean, SqlBasicCall> convertFun) {
        SqlNode left = call.getOperands()[0];
        SqlNode right = call.getOperands()[1];
        SqlIdentifier column;
        SqlNode value;
        boolean antonymous;
        boolean noneIsLiteral = !(left instanceof SqlLiteral)
                && !(right instanceof SqlLiteral);
        if (noneIsLiteral) {
            return super.enterBinaryExpression(call);
        }
        if (left instanceof SqlIdentifier) {
            column = (SqlIdentifier) left;
            value = right;
            antonymous = false;
        } else if (right instanceof SqlIdentifier) {
            column = (SqlIdentifier) right;
            value = left;
            antonymous = true;
        } else {
            return super.enterBinaryExpression(call);
        }
        String originColumnValue = RegExUtils.replaceAll(convertToString(value), "\r|\n", "");
        String replacementColumnValue = getReplacementColumnValue(originColumnValue);
        if (replacementColumnValue == null) {
            return null;
        }
        return convertFun.apply(column, replacementColumnValue, antonymous);
    }

    private SqlBasicCall getSimpleConvertNode(SqlBasicCall call, SqlIdentifier c, String v,
            Boolean antonymous, SqlOperator operator) {
        SqlNode[] operands = new SqlNode[2];
        List<String> repNames = Lists.newArrayList();
        List<String> orgNamePrefix = c.names.subList(0, c.names.size() - 1);
        if (orgNamePrefix != null && !orgNamePrefix.isEmpty()) {
            repNames.addAll(orgNamePrefix);
        }
        repNames.add(replaceToColumn);
        if (antonymous) {
            SqlNode column = new SqlIdentifier(repNames, call.operands[1].getParserPosition());
            SqlNode value = getConvertToValueNode(v);
            operands[0] = value;
            operands[1] = column;
        } else {
            SqlNode column = new SqlIdentifier(repNames, call.operands[0].getParserPosition());
            SqlNode value = getConvertToValueNode(v);
            operands[0] = column;
            operands[1] = value;
        }
        SqlBasicCall convertExp = new SqlBasicCall(operator, operands, call.getParserPosition());
        return convertExp;
    }
}
