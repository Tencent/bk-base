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

package com.tencent.bk.base.datalab.bksql.util.calcite.deparser;

import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.SelectVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitor;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.commons.lang.StringUtils;

public class ExpressionDeParser implements ExpressionVisitor<Void> {

    protected StringBuilder buffer = new StringBuilder();
    private SelectVisitor selectVisitor;

    public ExpressionDeParser() {
    }

    public ExpressionDeParser(SelectVisitor selectVisitor, StringBuilder buffer) {
        this.selectVisitor = selectVisitor;
        this.buffer = buffer;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public SelectVisitor getSelectVisitor() {
        return selectVisitor;
    }

    public void setSelectVisitor(SelectVisitor selectVisitor) {
        this.selectVisitor = selectVisitor;
    }

    @Override
    public Void visitFunction(SqlBasicCall function) {
        SqlOperator operator = function.getOperator();
        String funcName = operator.getName();
        if (function.getOperands() != null && function.operandCount() != 0) {
            this.visitFunctionOperands(operator, function.getOperandList(),
                    function.getFunctionQuantifier());
        } else {
            this.buffer.append(funcName);
            this.buffer.append("()");
        }
        return null;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        return null;
    }

    @Override
    public Void visit(SqlIdentifier identifier) {
        buffer.append(identifier.toString());
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
        return null;
    }

    @Override
    public Void visit(SqlDynamicParam dynamicParam) {
        return null;
    }

    @Override
    public Void visit(SqlNumericLiteral numericLiteral) {
        buffer.append(numericLiteral.getValue());
        return null;
    }

    @Override
    public Void visit(SqlBinaryStringLiteral binaryString) {
        buffer.append(binaryString.getValue());
        return null;
    }

    @Override
    public Void visit(SqlCharStringLiteral charString) {
        if (charString.toString().startsWith("u&")) {
            buffer.append("'" + charString.getNlsString().getValue() + "'");
        } else {
            buffer.append(charString.toString());
        }
        return null;
    }

    @Override
    public Void visit(SqlDateLiteral dateLiteral) {
        buffer.append(dateLiteral.toString());
        return null;
    }

    @Override
    public Void visit(SqlTimeLiteral timeLiteral) {
        buffer.append(timeLiteral.toString());
        return null;
    }

    @Override
    public Void visit(SqlTimestampLiteral timestampLiteral) {
        buffer.append(timestampLiteral.toString());
        return null;
    }

    @Override
    public Void visit(SqlBinaryOperator binaryOperator) {
        return null;
    }

    @Override
    public Void visit(SqlBetweenOperator between) {
        return null;
    }

    @Override
    public Void visit(SqlInOperator inOperator) {
        return null;
    }

    @Override
    public Void visit(SqlPostfixOperator postfixOperator) {
        return null;
    }

    @Override
    public Void visit(SqlLikeOperator likeOperator) {
        this.buffer.append(likeOperator.toString());
        return null;
    }

    @Override
    public Void visit(SqlSelect subSelect) {
        return null;
    }

    @Override
    public Void visit(SqlCase sqlCase) {
        buffer.append("CASE ");
        SqlNode value = sqlCase.getValueOperand();
        if (value != null) {
            value.accept(this);
            buffer.append(" ");
        }
        SqlNodeList whenOperands = sqlCase.getWhenOperands();
        SqlNodeList thenOperands = sqlCase.getThenOperands();
        if (whenOperands != null && thenOperands != null) {
            if (whenOperands.size() == thenOperands.size()) {
                for (int i = 0; i < whenOperands.size(); i++) {
                    visitWhenClause(whenOperands.get(i), thenOperands.get(i));
                }
            }
        }
        SqlNode elseOp = sqlCase.getElseOperand();
        if (elseOp != null) {
            boolean elseAddParenth = false;
            if (elseOp instanceof SqlCall) {
                SqlCall thenCall = (SqlCall) elseOp;
                if (!(thenCall.getOperator() instanceof SqlFunction)) {
                    elseAddParenth = true;
                }
            }
            buffer.append("ELSE ");
            if (elseAddParenth) {
                buffer.append("(");
            }
            elseOp.accept(selectVisitor);
            if (elseAddParenth) {
                buffer.append(")");
            }
            buffer.append(" ");
        }
        buffer.append("END");
        return null;
    }

    @Override
    public Void visit(SqlIntervalLiteral intervalValue) {
        this.buffer
                .append(intervalValue.toSqlString(SqlDialect.DatabaseProduct.MYSQL.getDialect()));
        return null;
    }

    @Override
    public Void visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }

    @Override
    public Void visit(SqlLiteral literal) {
        if (literal instanceof SqlCharStringLiteral) {
            visit((SqlCharStringLiteral) literal);
        } else if (literal instanceof SqlBinaryStringLiteral) {
            visit((SqlBinaryStringLiteral) literal);
        } else if (literal instanceof SqlNumericLiteral) {
            visit((SqlNumericLiteral) literal);
        } else if (literal instanceof SqlDateLiteral) {
            visit((SqlDateLiteral) literal);
        } else if (literal instanceof SqlTimeLiteral) {
            visit((SqlTimeLiteral) literal);
        } else if (literal instanceof SqlTimestampLiteral) {
            visit((SqlTimestampLiteral) literal);
        } else if (literal instanceof SqlIntervalLiteral) {
            visit((SqlIntervalLiteral) literal);
        } else {
            buffer.append(literal.getValue());
        }
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            visit((SqlBasicCall) call);
        } else if (call instanceof SqlCase) {
            visit((SqlCase) call);
        }
        return null;
    }

    /**
     * when-then 节点访问
     *
     * @param when when-clause
     * @param then then-clause
     */
    private void visitWhenClause(SqlNode when, SqlNode then) {
        boolean thenAddParenth = false;
        if (then instanceof SqlCall) {
            SqlCall thenCall = (SqlCall) then;
            if (!(thenCall.getOperator() instanceof SqlFunction)) {
                thenAddParenth = true;
            }
        }

        buffer.append("WHEN ");
        when.accept(selectVisitor);
        buffer.append(" THEN ");

        if (thenAddParenth) {
            buffer.append("(");
        }
        then.accept(selectVisitor);
        if (thenAddParenth) {
            buffer.append(")");
        }
        buffer.append(" ");
    }

    /**
     * deparse 函数里面的参数
     *
     * @param operator 函数操作符
     * @param operands 函数操作数
     * @param quantifier 函数量词
     */
    private void visitFunctionOperands(SqlOperator operator, List<SqlNode> operands,
            SqlLiteral quantifier) {
        String funcName = operator.getName();
        if (StringUtils.isNotBlank(funcName)) {
            buffer.append(funcName);
            buffer.append("(");
            if (quantifier != null) {
                buffer.append(quantifier.getValue()).append(" ");
            }
            for (Iterator<SqlNode> iter = operands.iterator(); iter.hasNext(); ) {
                SqlNode item = iter.next();
                item.accept(this);
                if (iter.hasNext()) {
                    if ("CAST".equals(funcName.toUpperCase())) {
                        buffer.append(" AS ");
                    } else {
                        buffer.append(", ");
                    }
                }
            }
            buffer.append(")");
        }
    }
}
