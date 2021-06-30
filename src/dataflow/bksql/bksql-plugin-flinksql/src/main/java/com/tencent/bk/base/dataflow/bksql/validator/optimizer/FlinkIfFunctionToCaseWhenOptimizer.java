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

package com.tencent.bk.base.dataflow.bksql.validator.optimizer;

import com.tencent.blueking.bksql.validator.ExpressionReplacementOptimizer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.WhenClause;

public class FlinkIfFunctionToCaseWhenOptimizer extends ExpressionReplacementOptimizer {

    @Override
    protected Expression enterExpressionNode(Function function) {
        if ("if".equalsIgnoreCase(function.getName())) {
            List<Expression> expressions = function.getParameters().getExpressions();
            if (expressions.size() != 3) {
                fail("params.error", "if");
            }
            CaseExpression caseExpression = new CaseExpression();
            WhenClause whenClause = new WhenClause();
            whenClause.setWhenExpression(expressions.get(0));
            whenClause.setThenExpression(castToCaseWhen(expressions.get(1)));
            caseExpression.setWhenClauses(Stream.of(whenClause).collect(Collectors.toList()));
            caseExpression.setElseExpression(castToCaseWhen(expressions.get(2)));
            return caseExpression;
        }
        return super.enterExpressionNode(function);
    }

    private Expression castToCaseWhen(Expression expression) {
        if (expression instanceof Function) {
            return enterExpressionNode((Function) expression);
        } else {
            return expression;
        }
    }
}
