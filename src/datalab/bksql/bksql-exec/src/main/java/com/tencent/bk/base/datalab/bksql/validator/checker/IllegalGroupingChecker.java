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

package com.tencent.bk.base.datalab.bksql.validator.checker;

import com.beust.jcommander.internal.Sets;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Set;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;

public class IllegalGroupingChecker extends SimpleListenerWalker {

    public IllegalGroupingChecker() {
        super();
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        SqlNodeList groupByColumnRefs = select.getGroup();
        if (groupByColumnRefs == null || groupByColumnRefs.size() == 0) {
            return;
        }
        Set<String> groupByColumnSet = Sets.newHashSet();
        groupByColumnRefs.forEach(groupRef -> {
            if (groupRef instanceof SqlIdentifier) {
                SqlIdentifier column = (SqlIdentifier) groupRef;
                groupByColumnSet.add(column.toString());
            }
        });

        SqlNodeList selectItems = select.getSelectList();
        for (SqlNode item : selectItems) {
            if (item instanceof SqlIdentifier) {
                SqlIdentifier column = (SqlIdentifier) item;
                boolean colVerified = groupByColumnSet.remove(column.toString());
                if (!colVerified) {
                    fail("missing.column.ref", column.toString());
                }
            } else if (item instanceof SqlBasicCall) {
                SqlBasicCall selectExp = (SqlBasicCall) item;
                if (selectExp.getOperator() instanceof SqlAsOperator) {
                    boolean colVerified;
                    boolean aliasVerified;
                    SqlNode beforeAs = selectExp.operand(0);
                    SqlIdentifier column;
                    if (beforeAs instanceof SqlIdentifier) {
                        column = (SqlIdentifier) beforeAs;
                        colVerified = groupByColumnSet.remove(column.toString());

                        SqlIdentifier alias = selectExp.operand(1);
                        aliasVerified = groupByColumnSet.remove(alias.toString());
                        if (!colVerified && !aliasVerified) {
                            fail("missing.column.ref", column);
                        }
                    } else if (beforeAs instanceof SqlBasicCall) {
                        SqlOperator op = ((SqlBasicCall) beforeAs).getOperator();
                        if (op instanceof SqlFunction) {
                            continue;
                        }
                        SqlIdentifier alias = selectExp.operand(1);
                        column = alias;
                        aliasVerified = groupByColumnSet.remove(alias.toString());
                        if (!aliasVerified) {
                            fail("missing.column.ref", column);
                        }
                    }
                }
            }
        }
        if (!groupByColumnSet.isEmpty()) {
            fail("missing.column.ref", groupByColumnSet.toString());
        }
    }
}
