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

import com.tencent.blueking.bksql.validator.SimpleListenerBasedWalker;
import java.util.List;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.create.table.ColDataType;

public class FlinkConcatFunctionResultTypeOptimizer extends SimpleListenerBasedWalker {

    public static final String CONCAT = "concat";
    public static final String CONCAT_WS = "concat_ws";
    public static final String VARCHAR = "VARCHAR";

    public FlinkConcatFunctionResultTypeOptimizer() {
        super();
    }

    @Override
    public void enterNode(Function function) {
        if (isConcatFunction(function) || isConcatWsFunction(function)) {
            addCastFunction(function);
        }
    }

    private boolean isConcatFunction(Function function) {
        String functionName = function.getName();
        ExpressionList functionParams = function.getParameters();
        if (CONCAT.equalsIgnoreCase(functionName)
                && null != functionParams
                && functionParams.getExpressions().size() > 1) {
            return true;
        }
        return false;
    }

    private boolean isConcatWsFunction(Function function) {
        String functionName = function.getName();
        ExpressionList functionParams = function.getParameters();
        if (CONCAT_WS.equalsIgnoreCase(functionName)
                && null != functionParams
                && functionParams.getExpressions().size() > 2) {
            return true;
        }
        return false;
    }

    private void addCastFunction(Function function) {
        List<Expression> expressions = function.getParameters().getExpressions();
        for (int i = 0; i < expressions.size(); i++) {
            CastExpression cast = new CastExpression();
            cast.setLeftExpression(expressions.get(i));
            ColDataType colDataType1 = new ColDataType();
            colDataType1.setDataType(VARCHAR);
            cast.setType(colDataType1);
            function.getParameters().getExpressions().set(i, cast);
        }
    }
}
