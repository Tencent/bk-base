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

import static com.tencent.bk.base.datalab.bksql.constant.Constants.DOT_JOINER;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

public class QueryAnalyzer extends SimpleListenerDeParser {

    public static final String JOIN = "JOIN";
    public static final String GROUP_BY = "GROUP_BY";
    public static final String ORDER_BY = "ORDER_BY";
    public static final String AGG = "AGG";
    public static final String DISTINCT = "DISTINCT";
    private static final List<String> AGG_FUNCTION_LIST = ImmutableList
            .of("MAX", "MIN", "SUM", "AVG", "COUNT");
    private final Map<String, List<String>> queryAttribute =
            ImmutableMap.of(JOIN, Lists.newArrayList(), GROUP_BY, Lists.newArrayList(), ORDER_BY,
                    Lists.newArrayList(),
                    AGG, Lists.newArrayList(), DISTINCT, Lists.newArrayList());

    @Override
    public void enterGroupByNode(SqlNode node) {
        super.enterGroupByNode(node);
        if (node instanceof SqlNodeList) {
            SqlNodeList groupByList = (SqlNodeList) node;
            groupByList.forEach(item -> queryAttribute.get(GROUP_BY).add(item.toString()));
        }

    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        getJoinTable(join.getLeft());
        getJoinTable(join.getRight());
    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {
        SqlNodeList orderByList = orderBy.orderList;
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

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        if (select.isDistinct()) {
            select.getSelectList().forEach(node -> addDistinctNames(node));
        }
    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {
        super.enterFunctionNode(function);
        SqlOperator operator = function.getOperator();
        if (AGG_FUNCTION_LIST.contains(operator.getName().toUpperCase())) {
            queryAttribute.get(AGG).add(operator.getName());
        }
    }

    private void getJoinTable(SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier tableNode = (SqlIdentifier) sqlNode;
            queryAttribute.get(JOIN).add(getTableName(tableNode));
        } else if (sqlNode instanceof SqlSelect) {
            getJoinTable(((SqlSelect) sqlNode).getFrom());
        } else if (sqlNode instanceof SqlOrderBy) {
            getJoinTable(((SqlOrderBy) sqlNode).query);
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) sqlNode;
            if (call.getOperator() instanceof SqlAsOperator) {
                SqlNode beforeAs = call.getOperands()[0];
                if (beforeAs instanceof SqlIdentifier) {
                    SqlIdentifier tableNode = (SqlIdentifier) beforeAs;
                    String tableName = getTableName(tableNode);
                    queryAttribute.get(JOIN).add(tableName);
                } else {
                    getJoinTable(beforeAs);
                }
            }
        }
    }

    private String getTableName(SqlIdentifier tableNode) {
        if (tableNode == null) {
            return "";
        }
        if (tableNode.names.size() >= 2) {
            return tableNode.names.get(tableNode.names.size() - 2);
        } else {
            return tableNode.toString();
        }
    }

    private void processNullsFirstOrLast(SqlNode operand) {
        if (operand instanceof SqlBasicCall) {
            SqlNode nullsCallOp0 =
                    ((SqlBasicCall) operand).operands[0];
            if (nullsCallOp0 instanceof SqlIdentifier) {
                processIdentifier((SqlIdentifier) nullsCallOp0);
            }
        }
    }

    private void processDescending(SqlNode operand) {
        if (operand instanceof SqlIdentifier) {
            processIdentifier((SqlIdentifier) operand);
        } else if (operand instanceof SqlBasicCall) {
            SqlBasicCall itemCallOp = (SqlBasicCall) operand;
            SqlKind opKind = itemCallOp.getKind();
            if (opKind == SqlKind.IS_NULL || opKind == SqlKind.IS_NOT_NULL) {
                if (itemCallOp.operands[0] instanceof SqlIdentifier) {
                    processIdentifier((SqlIdentifier) itemCallOp.operands[0]);
                }
            }
        }
    }

    private void processIdentifier(SqlIdentifier orderItem) {
        String columnName = DOT_JOINER.join(orderItem.names);
        queryAttribute.get(ORDER_BY).add(columnName);
    }

    private void addDistinctNames(SqlNode node) {
        queryAttribute.get(DISTINCT).add(node.toString());
    }

    @Override
    protected Object getRetObj() {
        return queryAttribute;
    }
}
