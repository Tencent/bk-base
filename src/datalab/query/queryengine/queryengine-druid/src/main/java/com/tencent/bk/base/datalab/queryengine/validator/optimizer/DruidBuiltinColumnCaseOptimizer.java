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
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.util.ReflectUtil;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DruidBuiltinColumnCaseOptimizer extends ExpressionReplacementOptimizer {

    public static final String ORDER_LIST_FIELD = "orderList";
    public static final String OPERANDS_FIELD = "operands";
    private static final Map<String, String> TIMESTAMP_BUILTIN_COLUMNS = ImmutableMap.of(
            "localtime", "__localtime",
            "dteventtime", "dtEventTime",
            "dteventtimestamp", "dtEventTimeStamp",
            "thedate", "thedate");

    @Override
    protected SqlNode enterSelectColumn(SqlIdentifier column) {
        return addAliasName(column);
    }

    @Override
    protected SqlBasicCall enterAsExpression(SqlBasicCall asExpression) {
        SqlNode leftAs = asExpression.operand(0);
        if (leftAs instanceof SqlIdentifier) {
            convertColumnName((SqlIdentifier) leftAs);
        }
        return asExpression;
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        if (select.getGroup() == null) {
            return;
        }
        SqlNodeList originNodeList = select.getGroup();
        SqlNodeList replacedNodeList = getReplacedNodeList(originNodeList);
        select.setGroupBy(replacedNodeList);
    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {
        super.enterOrderByNode(orderBy);
        SqlNodeList originNodeList = orderBy.orderList;
        SqlNodeList replacedNodeList = getReplacedNodeList(originNodeList);
        ReflectUtil.modifyFinalField(SqlOrderBy.class, ORDER_LIST_FIELD, orderBy, replacedNodeList);
    }

    @Override
    public void enterFunctionNode(SqlBasicCall node) {
        super.enterFunctionNode(node);
        replaceFunctionOperands(node);
    }

    /**
     * 替换函数节点关联的操作数
     *
     * @param node 函数节点
     */
    private void replaceFunctionOperands(SqlBasicCall node) {
        SqlNode[] operands = node.getOperands();
        if (operands == null) {
            return;
        }
        SqlNode[] newOperands = new SqlNode[operands.length];
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof SqlIdentifier) {
                newOperands[i] = convertColumnName((SqlIdentifier) operands[i]);
            } else {
                if ((operands[i] instanceof SqlBasicCall)) {
                    replaceFunctionOperands((SqlBasicCall) operands[i]);
                }
                newOperands[i] = operands[i];
            }
        }
        ReflectUtil.modifyFinalField(SqlBasicCall.class, OPERANDS_FIELD, node, newOperands);
    }

    /**
     * 给 select 字段名加上 as 别名
     *
     * @param column select 字段名
     * @return 加上 as 别名后的表达式
     */
    private SqlBasicCall addAliasName(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        SqlIdentifier alias = new SqlIdentifier(Lists.newArrayList(columnName), SqlParserPos.ZERO);
        SqlNode[] asParams = new SqlNode[2];
        asParams[1] = alias;
        asParams[0] = convertColumnName(column);
        return new SqlBasicCall(SqlStdOperatorTable.AS, asParams, SqlParserPos.ZERO);
    }

    /**
     * 替换蓝鲸基础计算平台内置字段名
     *
     * @param column 原始字段名
     * @return 替换后的字段名
     */
    private SqlIdentifier convertColumnName(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        String trimmed = QuoteUtil.trimQuotes(columnName);
        if (TIMESTAMP_BUILTIN_COLUMNS.containsKey(trimmed.toLowerCase())) {
            String newColumnName = QuoteUtil.replaceValueInsideQuotes(columnName,
                    TIMESTAMP_BUILTIN_COLUMNS.get(trimmed.toLowerCase()));
            column.setNames(Lists.newArrayList(newColumnName),
                    Lists.newArrayList(column.getParserPosition()));
        }
        return column;
    }

    /**
     * 获取转换后的 SqlNodeList 节点
     *
     * @param originNodeList 原始 SqlNodeList 节点
     * @return 转换后的 SqlNodeList 节点
     */
    private SqlNodeList getReplacedNodeList(SqlNodeList originNodeList) {
        SqlNodeList replacedNodeList = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode node : originNodeList) {
            if (node instanceof SqlIdentifier) {
                replacedNodeList.add(convertColumnName((SqlIdentifier) node));
            } else {
                replacedNodeList.add(node);
            }
        }
        return replacedNodeList;
    }
}