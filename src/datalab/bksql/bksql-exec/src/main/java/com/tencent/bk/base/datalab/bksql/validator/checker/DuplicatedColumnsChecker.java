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
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Iterator;
import java.util.Set;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

public class DuplicatedColumnsChecker extends SimpleListenerWalker {

    public DuplicatedColumnsChecker() {
        super();
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        SqlNodeList selectItems = select.getSelectList();
        Set<String> refSet = Sets.newHashSet();
        for (Iterator<SqlNode> iter = selectItems.iterator(); iter.hasNext(); ) {
            SqlNode item = iter.next();
            if (item instanceof SqlIdentifier) {
                SqlIdentifier column = (SqlIdentifier) item;
                ImmutableList<String> names = column.names;
                if (names == null) {
                    return;
                }
                if (names.size() == 1) {
                    addAndCheck(refSet, names.get(0));
                } else if (names.size() == 2) {
                    addAndCheck(refSet, names.get(1));
                }
            } else if (item instanceof SqlBasicCall) {
                SqlBasicCall selectExp = (SqlBasicCall) item;
                if (selectExp.getOperator() instanceof SqlAsOperator) {
                    SqlIdentifier alias = (SqlIdentifier) selectExp.operands[1];
                    addAndCheck(refSet, alias.toString());
                }
            }


        }
    }

    private void addAndCheck(Set<String> refSet, String name) {
        boolean added = refSet.add(name);
        if (!added) {
            fail("duplicated.column", name);
        }
    }
}
