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

import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.bksql.constant.Constants;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.math.NumberUtils;

public abstract class AbstractTheDatePartitionOptimizer extends BaseColumnReplacementOptimizer {

    protected static final String CHAR_STRING = "charString";
    protected static final String NUMERIC = "numeric";
    private String partitionColumn;
    private String partitionDateType;

    public AbstractTheDatePartitionOptimizer(String partitionColumn, String partitionDateType) {
        super(Constants.THEDATE);
        this.partitionColumn = partitionColumn;
        this.partitionDateType = partitionDateType;
    }

    /**
     * 获取分区开始值
     *
     * @param v thedate 字段值
     * @return 分区值
     */
    protected abstract String getPartitionStartValue(String v);

    /**
     * 获取分区结束值
     *
     * @param v thedate 字段值
     * @return 分区值
     */
    protected abstract String getPartitionEndValue(String v);

    @Override
    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        SqlBasicCall toConvert = convertToPartition(call, (c, v, antonymous) -> {
            List<String> repNames = Lists.newArrayList();
            List<String> orgNamePrefix = c.names.subList(0, c.names.size() - 1);
            if (orgNamePrefix != null && !orgNamePrefix.isEmpty()) {
                repNames.addAll(orgNamePrefix);
            }
            repNames.add(partitionColumn);
            String ltePartitionVal =
                    getPartitionEndValue(v);
            String gtePartitionVal =
                    getPartitionStartValue(v);
            SqlNode[] gteOperands = new SqlNode[2];
            SqlIdentifier gteColumn = new SqlIdentifier(repNames, SqlParserPos.ZERO);
            SqlNode gteValue = getPartitionNode(gtePartitionVal);
            gteOperands[0] = gteColumn;
            gteOperands[1] = gteValue;
            SqlNode gteExpression = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    gteOperands, SqlParserPos.ZERO);
            SqlNode[] lteOperands = new SqlNode[2];
            SqlIdentifier lteColumn = new SqlIdentifier(repNames, SqlParserPos.ZERO);
            SqlNode lteValue = getPartitionNode(ltePartitionVal);
            lteOperands[0] = lteColumn;
            lteOperands[1] = lteValue;
            SqlNode lteExpression = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    lteOperands, SqlParserPos.ZERO);
            SqlNode[] andOperands = new SqlNode[2];
            andOperands[0] = gteExpression;
            andOperands[1] = lteExpression;
            SqlBasicCall andExpression = new SqlBasicCall(SqlStdOperatorTable.AND, andOperands,
                    SqlParserPos.ZERO);
            return andExpression;
        });
        return toConvert;
    }

    @Override
    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        SqlBasicCall toConvert = convertToPartition(call, (c, v, antonymous) -> {
            List<String> repNames = Lists.newArrayList();
            List<String> orgNamePrefix = c.names.subList(0, c.names.size() - 1);
            if (orgNamePrefix != null && !orgNamePrefix.isEmpty()) {
                repNames.addAll(orgNamePrefix);
            }
            repNames.add(partitionColumn);
            String ltePartitionVal =
                    getPartitionStartValue(v);
            String gtePartitionVal =
                    getPartitionEndValue(v);
            SqlNode[] gteOperands = new SqlNode[2];
            SqlIdentifier gteColumn = new SqlIdentifier(repNames, SqlParserPos.ZERO);
            SqlNode gteValue = getPartitionNode(gtePartitionVal);
            gteOperands[0] = gteColumn;
            gteOperands[1] = gteValue;
            SqlNode gteExpression = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, gteOperands,
                    SqlParserPos.ZERO);
            SqlNode[] lteOperands = new SqlNode[2];
            SqlIdentifier lteColumn = new SqlIdentifier(repNames, SqlParserPos.ZERO);
            SqlNode lteValue = getPartitionNode(ltePartitionVal);
            lteOperands[0] = lteColumn;
            lteOperands[1] = lteValue;
            SqlNode lteExpression = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, lteOperands,
                    SqlParserPos.ZERO);
            SqlNode[] andOperands = new SqlNode[2];
            andOperands[0] = gteExpression;
            andOperands[1] = lteExpression;
            SqlBasicCall andExpression = new SqlBasicCall(SqlStdOperatorTable.OR, andOperands,
                    SqlParserPos.ZERO);
            return andExpression;
        });
        return toConvert;
    }

    @Override
    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, initOperands,
                SqlParserPos.ZERO);
        SqlBasicCall toConvert = convertComparitionExp(SqlStdOperatorTable.GREATER_THAN, call,
                copyExp);
        return toConvert;
    }

    @Override
    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                initOperands, SqlParserPos.ZERO);
        SqlBasicCall toConvert = convertComparitionExp(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                call,
                copyExp);
        return toConvert;
    }

    @Override
    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, initOperands,
                SqlParserPos.ZERO);
        SqlBasicCall toConvert = convertComparitionExp(SqlStdOperatorTable.LESS_THAN, call,
                copyExp);
        return toConvert;
    }

    @Override
    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                initOperands, SqlParserPos.ZERO);
        SqlBasicCall toConvert = convertComparitionExp(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, call,
                copyExp);
        return toConvert;
    }

    /**
     * presto 分区转换函数
     *
     * @param copyExp sql表达式副本
     * @param convertFun 转换函数
     * @return 转换后的sql表达式
     */
    private SqlBasicCall convertToPartition(SqlBasicCall copyExp,
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
        boolean isAllColumn = left instanceof SqlIdentifier && right instanceof SqlIdentifier;
        if (isAllColumn) {
            return super.enterBinaryExpression(copyExp);
        }
        boolean isTheDate = Constants.THEDATE.equalsIgnoreCase(columnName);
        if (!isTheDate) {
            return super.enterBinaryExpression(copyExp);
        }
        String strVal = RegExUtils.replaceAll(convertToString(value), "\r|\n", "");
        if (!NumberUtils.isDigits(strVal)) {
            fail("illegal.thedate.format");
        }
        return convertFun.apply(column, strVal, antonymous);
    }

    /**
     * > >= < <=通用转换逻辑
     *
     * @param operator 操作符
     * @param call 原始比较节点
     * @param copyExp 原始比较节点拷贝
     * @return 转换后的比较节点
     */
    private SqlBasicCall convertComparitionExp(SqlBinaryOperator operator, SqlBasicCall call,
            SqlBasicCall copyExp) {
        return convertToPartition(call, (c, v, antonymous) -> {
            List<String> repNames = Lists.newArrayList();
            List<String> orgNamePrefix = c.names.subList(0, c.names.size() - 1);
            if (orgNamePrefix != null && !orgNamePrefix.isEmpty()) {
                repNames.addAll(orgNamePrefix);
            }
            repNames.add(partitionColumn);
            String antonymousVal;
            String notAntonymousVal;
            switch (operator.kind) {
                case GREATER_THAN:
                case LESS_THAN_OR_EQUAL:
                    antonymousVal =
                            getPartitionStartValue(v);
                    notAntonymousVal =
                            getPartitionEndValue(v);
                    break;
                case LESS_THAN:
                case GREATER_THAN_OR_EQUAL:
                    antonymousVal =
                            getPartitionEndValue(v);
                    notAntonymousVal =
                            getPartitionStartValue(v);
                    break;
                default:
                    return copyExp;
            }

            SqlNode[] operands = new SqlNode[2];
            SqlIdentifier column = new SqlIdentifier(repNames, SqlParserPos.ZERO);
            if (antonymous) {
                SqlNode value = getPartitionNode(antonymousVal);
                operands[0] = value;
                operands[1] = column;
            } else {
                SqlNode value = getPartitionNode(notAntonymousVal);
                operands[0] = column;
                operands[1] = value;
            }
            for (int i = 0; i < copyExp.getOperands().length; i++) {
                copyExp.setOperand(i, operands[i]);
            }
            return copyExp;
        });
    }

    /**
     * 获取分区字段值节点
     *
     * @param partitionVal 分区字段值(字符串形式)
     * @return 分区字段值节点
     */
    protected SqlNode getPartitionNode(String partitionVal) {
        SqlNode partitionNode;
        switch (partitionDateType) {
            case NUMERIC:
                partitionNode = SqlLiteral
                        .createExactNumeric(partitionVal, SqlParserPos.ZERO);
                break;
            case CHAR_STRING:
                partitionNode = SqlLiteral
                        .createCharString(partitionVal, SqlParserPos.ZERO);
                break;
            default:
                throw new RuntimeException(
                        String.format("Not support partition dataType:%s", partitionDateType));
        }
        return partitionNode;
    }
}
