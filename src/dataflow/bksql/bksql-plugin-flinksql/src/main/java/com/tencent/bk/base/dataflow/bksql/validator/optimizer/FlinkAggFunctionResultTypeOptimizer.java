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
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.ColDataType;

public class FlinkAggFunctionResultTypeOptimizer extends SimpleListenerBasedWalker {

    public FlinkAggFunctionResultTypeOptimizer() {
        super();
    }

    @Override
    public void enterNode(Function function) {
        String name = function.getName();
        if (null != function.getParameters() && function.getParameters().getExpressions().size() == 1) {
            if ("avg".equalsIgnoreCase(name)) {
                addCastFunction(function, "double");
            }
        }
    }

    private void addCastFunction(Function function, String toDataType) {
        if (null != function.getParameters() && function.getParameters().getExpressions().size() == 1) {
            CastExpression cast = new CastExpression();
            cast.setLeftExpression(function.getParameters().getExpressions().get(0));
            ColDataType colDataType1 = new ColDataType();
            colDataType1.setDataType(toDataType);
            cast.setType(colDataType1);
            function.getParameters().getExpressions().set(0, cast);
        }
    }
}
