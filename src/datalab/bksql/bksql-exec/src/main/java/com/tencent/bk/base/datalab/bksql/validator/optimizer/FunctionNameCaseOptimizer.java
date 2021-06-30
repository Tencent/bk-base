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
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;

public class FunctionNameCaseOptimizer extends SimpleListenerWalker {

    public static final String RULE_TO_UPPER = "ToUpper";
    public static final String RULE_TO_LOWER = "ToLower";

    private final Set<String> functionNames;
    private final String rule;

    @JsonCreator
    public FunctionNameCaseOptimizer(@JsonProperty("names") List<String> functionNames,
            @JsonProperty("rule") String rule) {
        super();
        this.functionNames = new HashSet<>();
        for (String methodName : functionNames) {
            this.functionNames.add(methodName.toUpperCase());
        }
        if (!new HashSet<>(Arrays.asList(RULE_TO_LOWER, RULE_TO_UPPER)).contains(rule)) {
            throw new IllegalArgumentException("rule definition does not exist: " + rule);
        }
        this.rule = rule;
    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {
        super.enterFunctionNode(function);
        SqlOperator orgOperator = function.getOperator();
        String funName = orgOperator.toString();
        if (functionNames.contains(funName.toUpperCase())) {
            switch (rule) {
                case RULE_TO_UPPER:
                    funName = funName.toUpperCase();
                    break;
                case RULE_TO_LOWER:
                    funName = funName.toLowerCase();
                    break;
                default:
                    throw new IllegalStateException();
            }
            function.setOperator(new SqlFunction(funName,
                    orgOperator.getKind(),
                    orgOperator.getReturnTypeInference(),
                    orgOperator.getOperandTypeInference(),
                    orgOperator.getOperandTypeChecker(),
                    ((SqlFunction) orgOperator).getFunctionType()));
        }
    }
}
