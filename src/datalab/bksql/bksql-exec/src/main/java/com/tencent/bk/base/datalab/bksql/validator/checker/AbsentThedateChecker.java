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

import com.google.common.collect.ImmutableSet;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitorAdapter;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Set;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

public class AbsentThedateChecker extends SimpleListenerWalker {

    private static final Set<String> TIME_BASED_FILTER_NAMES = ImmutableSet
            .of("dteventtimestamp", "dteventtime", "thedate");

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        if (select != null) {
            if (!(select.getFrom() instanceof SqlIdentifier)) {
                return;
            }
            SqlNode where = select.getWhere();
            if (where == null) {
                fail("absent.thedate");
            }
            final boolean[] found = new boolean[]{false};
            where.accept(new ExpressionVisitorAdapter() {
                @Override
                public Void visit(SqlIdentifier column) {
                    super.visit(column);
                    if (TIME_BASED_FILTER_NAMES
                            .contains(QuoteUtil.trimQuotes(column.toString()).toLowerCase())) {
                        found[0] = true;
                    }
                    return null;
                }
            });
            if (!found[0]) {
                fail("absent.thedate");
            }
            super.exitPlainSelectNode(select);
        }
    }
}
