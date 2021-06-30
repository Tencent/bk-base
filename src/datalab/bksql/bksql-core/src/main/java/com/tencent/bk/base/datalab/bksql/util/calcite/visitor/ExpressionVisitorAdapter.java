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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

public class ExpressionVisitorAdapter implements ExpressionVisitor<Void> {

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            visit((SqlBasicCall) call);
        }
        return null;
    }

    @Override
    public Void visitInExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitLikeExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitBetweenExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitPrefixExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitPostfixExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitAsExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitBinaryExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitPlusExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitMinusExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitTimesExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitDivideExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitModExpression(SqlBasicCall expression) {
        return null;
    }

    @Override
    public Void visitAndExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitOrExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitNotExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitEqualsExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitNotEqualsExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitGreaterThanExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitGreaterThanEqualsExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitLessThanExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    @Override
    public Void visitLessThanEqualsExpression(SqlBasicCall expression) {
        iterate(expression.operands);
        return null;
    }

    private void iterate(SqlNode[] operands) {
        if (operands != null) {
            for (SqlNode node : operands) {
                node.accept(this);
            }
        }
    }
}
