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

package com.tencent.bk.base.datalab.bksql.deparser;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;

public class SelectFunctionsDeparser extends SimpleListenerDeParser {

    private Set<String> functionNames = new HashSet<>();

    private void addToFunctionNames(SqlNode node) {
        if (node instanceof SqlCall) {
            SqlOperator operator = ((SqlCall) node).getOperator();
            boolean isSqlFunction = operator instanceof SqlFunction;
            boolean isSqlAs = operator instanceof SqlAsOperator;
            if (!isSqlFunction && !isSqlAs) {
                return;
            }
            if (isSqlFunction) {
                functionNames.add(operator.getName());
            }
            if (isSqlAs) {
                addToFunctionNames(((SqlCall) node).operand(0));
            }
        }
    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        super.exitPlainSelectNode(select);
        select.getSelectList().forEach(node -> addToFunctionNames(node));
    }

    @Override
    protected Object getRetObj() {
        return functionNames;
    }
}
