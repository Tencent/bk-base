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

import static com.tencent.bk.base.datalab.bksql.util.SqlNodeUtil.convertToString;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Stack;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

public class QuerySource extends SimpleListenerDeParser {

    public static final String PART00 = "00";
    public static final String PART23 = "23";
    public static final String THEDATE = "thedate";
    public static final String DTPARUNIT = "dt_par_unit";
    public static final String PARTITION_START = "start";
    public static final String PARTITION_END = "end";
    private final Stack<String> tableStack = new Stack<>();
    private final Map<String, Map<String, String>> tablePartitionMap =
            Maps.newHashMap();

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        extractTableName(select.getFrom());
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        extractTableName(join.getLeft());
        extractTableName(join.getRight());
    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall call) {
        super.enterEqualsExpressionNode(call);
        getPartition(call);
    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall call) {
        super.enterGreaterThanExpressionNode(call);
        SqlNode left = call.getOperands()[0];
        SqlNode right = call.getOperands()[1];
        SqlIdentifier column;
        String columnName;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            if (right instanceof SqlIdentifier) {
                return;
            }
            column = (SqlIdentifier) left;
            columnName = column.names.get(column.names.size() - 1);
            value = right;
        } else {
            return;
        }
        boolean isTheDate = THEDATE.equalsIgnoreCase(columnName);
        if (!isTheDate) {
            return;
        }
        String strVal = convertToString(value);
        String gtePartitionVal = strVal + PART00;
        Map<String, String> partitionMap = Maps.newHashMap();
        partitionMap.put(PARTITION_START, gtePartitionVal);
        String lastTable = tableStack.pop();
        tablePartitionMap.put(lastTable, partitionMap);
        tableStack.push(lastTable);
    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall call) {
        super.enterLessThanEqualsExpressionNode(call);
        SqlNode left = call.getOperands()[0];
        SqlNode right = call.getOperands()[1];
        SqlIdentifier column;
        String columnName;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            if (right instanceof SqlIdentifier) {
                return;
            }
            column = (SqlIdentifier) left;
            columnName = column.names.get(column.names.size() - 1);
            value = right;
        } else {
            return;
        }
        boolean isTheDate = THEDATE.equalsIgnoreCase(columnName);
        if (!isTheDate) {
            return;
        }
        String strVal = convertToString(value);
        String ltePartitionVal = strVal + PART23;
        String lastTable = tableStack.pop();
        if (tablePartitionMap.get(lastTable) != null) {
            tablePartitionMap.get(lastTable).put(PARTITION_END, ltePartitionVal);
        }
    }

    /**
     * 解析表达式获取时间分区
     *
     * @param call SqlBasicCall实例
     */
    private void getPartition(SqlBasicCall call) {
        SqlNode left = call.getOperands()[0];
        SqlNode right = call.getOperands()[1];
        SqlIdentifier column;
        String columnName;
        SqlNode value;
        if (left instanceof SqlIdentifier) {
            if (right instanceof SqlIdentifier) {
                return;
            }
            column = (SqlIdentifier) left;
            columnName = column.names.get(column.names.size() - 1);
            value = right;
        } else {
            return;
        }
        boolean isTheDate = THEDATE.equalsIgnoreCase(columnName);
        if (!isTheDate) {
            return;
        }
        String strVal = convertToString(value);
        String ltePartitionVal = strVal + PART23;
        String gtePartitionVal = strVal + PART00;
        Map<String, String> partitionMap = ImmutableMap
                .of(PARTITION_START, gtePartitionVal, PARTITION_END, ltePartitionVal);
        String lastTable = tableStack.pop();
        tablePartitionMap.put(lastTable, partitionMap);
    }

    /**
     * 提取表名
     *
     * @param sqlNode SqlNode实例
     */
    private void extractTableName(SqlNode sqlNode) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case IDENTIFIER:
                String tableName = getTableName((SqlIdentifier) sqlNode);
                tableStack.push(tableName);
                break;
            case SELECT:
                extractTableName(((SqlSelect) sqlNode).getFrom());
                break;
            case ORDER_BY:
                extractTableName(((SqlOrderBy) sqlNode).query);
                break;
            default:
                extractTableNameByBasicCall(sqlNode);

        }
    }

    /**
     * 获取表名字符串
     *
     * @param tableNode SqlIdentifier节点
     * @return 表名字符串
     */
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

    /**
     * 解析SqlBasicCall节点中获取表名
     *
     * @param sqlNode SqlNode实例
     */
    private void extractTableNameByBasicCall(SqlNode sqlNode) {
        if (!(sqlNode instanceof SqlBasicCall)) {
            return;
        }
        SqlBasicCall call = (SqlBasicCall) sqlNode;
        if (call.getOperator() instanceof SqlAsOperator) {
            SqlNode beforeAs = call.getOperands()[0];
            if (beforeAs instanceof SqlIdentifier) {
                SqlIdentifier tableNode = (SqlIdentifier) beforeAs;
                String tableName = getTableName(tableNode);
                tableStack.push(tableName);
            } else {
                extractTableName(beforeAs);
            }
        }
    }

    @Override
    protected Object getRetObj() {
        return tablePartitionMap;
    }
}
