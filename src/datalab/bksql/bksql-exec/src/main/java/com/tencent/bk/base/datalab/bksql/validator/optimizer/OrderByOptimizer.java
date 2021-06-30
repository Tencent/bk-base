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

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParserPos;

public class OrderByOptimizer extends SimpleListenerWalker {

    private final List<String> replacedColumnList;
    private final String orderByColumn;

    @JsonCreator
    public OrderByOptimizer(@JsonProperty("replacedColumnList") List<String> replacedColumnList,
            @JsonProperty("orderByColumn") String orderByColumn) {
        super();
        this.replacedColumnList = replacedColumnList;
        this.orderByColumn = orderByColumn;
    }

    @Override
    public void enterOrderByNode(SqlOrderBy call) {
        super.enterOrderByNode(call);
        SqlNodeList orderByList = call.orderList;
        if (orderByList == null || orderByList.size() == 0) {
            return;
        }
        for (SqlNode orderItem : orderByList) {
            SqlKind nodeKind = orderItem.getKind();
            switch (nodeKind) {
                case IDENTIFIER:
                    processIdentifier((SqlIdentifier) orderItem);
                    break;
                case DESCENDING:
                    processDescending(((SqlBasicCall) orderItem).operands[0]);
                    break;
                case NULLS_FIRST:
                case NULLS_LAST:
                    processNullsFirstOrLast(((SqlBasicCall) orderItem).operands[0]);
                    break;
                default:
                    break;
            }
        }
    }

    private void processNullsFirstOrLast(SqlNode operand) {
        if (operand instanceof SqlBasicCall) {
            SqlNode nullsCallOp0 =
                    ((SqlBasicCall) operand).operands[0];
            if (nullsCallOp0 instanceof SqlIdentifier) {
                replaceDtEventTime((SqlIdentifier) nullsCallOp0);
            }
        }
    }

    private void processDescending(SqlNode operand) {
        if (operand instanceof SqlIdentifier) {
            replaceDtEventTime((SqlIdentifier) operand);
        } else if (operand instanceof SqlBasicCall) {
            SqlBasicCall itemCallOp = (SqlBasicCall) operand;
            SqlKind opKind = itemCallOp.getKind();
            if (opKind == SqlKind.IS_NULL || opKind == SqlKind.IS_NOT_NULL) {
                if (itemCallOp.operands[0] instanceof SqlIdentifier) {
                    replaceDtEventTime((SqlIdentifier) itemCallOp.operands[0]);
                }
            }
        }
    }

    private void processIdentifier(SqlIdentifier orderItem) {
        String columnName = orderItem.names.get(orderItem.names.size() - 1);
        if (isReplacedColumn(columnName)) {
            orderItem.setNames(Lists.newArrayList(orderByColumn),
                    Lists.newArrayList(SqlParserPos.ZERO));
        }
    }

    private void replaceDtEventTime(SqlIdentifier sqlIdentifier) {
        SqlIdentifier column;
        column = sqlIdentifier;
        String columnName = column.names.get(column.names.size() - 1);
        if (isReplacedColumn(columnName)) {
            column.setNames(Lists.newArrayList(orderByColumn),
                    Lists.newArrayList(SqlParserPos.ZERO));
        }
    }

    private boolean isReplacedColumn(String columnName) {
        return replacedColumnList.stream().anyMatch(c -> c.equalsIgnoreCase(columnName));
    }
}
