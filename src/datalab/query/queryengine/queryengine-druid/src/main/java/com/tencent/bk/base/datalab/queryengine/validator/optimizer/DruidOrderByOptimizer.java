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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableSet;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * 如果 sql 中包含 group by，支持以任意字段进行 order by，如果 sql 中不包含 group by，只能以 __time 进行 order by
 */
public class DruidOrderByOptimizer extends SimpleListenerWalker {

    private static final String TIME_FIELD = "__time";
    private static final Set<String> TIMESTAMP_AWARE_COLUMNS = ImmutableSet
            .of("time", "dteventtime", "dteventtimestamp", "thedate");

    @Override
    public void enterQueryNode(SqlCall call) {
        super.enterQueryNode(call);
        if (!(call instanceof SqlOrderBy)) {
            return;
        }
        SqlOrderBy orderBySelect = (SqlOrderBy) call;
        SqlSelect selectBody = (SqlSelect) orderBySelect.query;
        if (selectBody.getGroup() != null) {
            return;
        }
        Optional.ofNullable(orderBySelect.orderList).ifPresent(orderBy -> {
            orderBy.forEach(item -> {
                switch (item.getKind()) {
                    case IDENTIFIER:
                        reNameToTime((SqlIdentifier) item);
                        break;
                    case DESCENDING:
                        SqlBasicCall orderCall = (SqlBasicCall) item;
                        reNameToTime((SqlIdentifier) orderCall.operands[0]);
                        break;
                    default:
                        break;
                }
            });
        });
    }

    /**
     * column 节点改名为 __time
     *
     * @param node column 节点
     */
    private void reNameToTime(SqlIdentifier node) {
        SqlIdentifier column = node;
        String columnName = column.names.get(column.names.size() - 1);
        if (TIMESTAMP_AWARE_COLUMNS.contains(columnName.toLowerCase())) {
            column.setNames(Lists.newArrayList(TIME_FIELD),
                    Lists.newArrayList(SqlParserPos.ZERO));
        }
    }
}