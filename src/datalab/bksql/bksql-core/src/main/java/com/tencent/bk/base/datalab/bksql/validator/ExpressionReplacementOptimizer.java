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

package com.tencent.bk.base.datalab.bksql.validator;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ExpressionReplacementOptimizer extends BaseListenerWalker {

    private final Map<SqlNode, SqlNode> replacements = new HashMap<>();

    protected void registerReplacement(SqlNode expr, SqlNode replacement) {
        Preconditions.checkNotNull(expr);
        Preconditions.checkNotNull(replacement);
        if (!replacements.containsKey(expr)) {
            replacements.put(expr, replacement);
        }
    }

    protected SqlNode retrieveReplacement(SqlNode expr) {
        if (expr == null) {
            return null;
        }
        return replacements.getOrDefault(expr, expr);
    }

    protected List<SqlNode> retrieveReplacements(List<SqlNode> exprs) {
        if (exprs == null) {
            return null;
        }
        List<SqlNode> retList = new ArrayList<>(exprs.size());
        for (SqlNode expr : exprs) {
            retList.add(retrieveReplacement(expr));
        }
        return retList;
    }

    @Override
    public void enterBasicCallNode(SqlBasicCall basicCall) {
    }

    @Override
    public void exitBasicCallNode(SqlBasicCall basicCall) {
        replaceCallOperands(basicCall);
    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {
    }

    @Override
    public void exitOrderByNode(SqlOrderBy orderBy) {
        if (orderBy.orderList != null) {
            List<SqlNode> replacedList = Lists.newArrayList();
            SqlNodeList orderList = orderBy.orderList;
            for (SqlNode node : orderList) {
                SqlNode replaceNode = retrieveReplacement(node);
                if (replaceNode instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) replaceNode;
                    if (call.getOperator() instanceof SqlAsOperator) {
                        replacedList.add(call.operand(0));
                    } else {
                        replacedList.add(call);
                    }
                } else {
                    replacedList.add(replaceNode);
                }
            }
            for (int i = 0; i < orderBy.orderList.size(); i++) {
                orderBy.orderList.set(i, replacedList.get(i));
            }
        }

    }

    @Override
    public void enterColumnNode(SqlIdentifier column) {
        registerReplacement(column, enterColumnExpression(column));
    }

    protected SqlNode enterColumnExpression(SqlIdentifier column) {
        return column;
    }

    @Override
    public void exitColumnNode(SqlIdentifier column) {
    }

    @Override
    public void enterAsExpressionNode(SqlBasicCall asExpression) {
        registerReplacement(asExpression, enterAsExpression(asExpression));
    }

    protected SqlBasicCall enterAsExpression(SqlBasicCall asExpression) {
        return asExpression;
    }

    @Override
    public void exitAsExpressionNode(SqlBasicCall asExpression) {
        SqlNode replacedNode = retrieveReplacement(asExpression);
        if (!(replacedNode instanceof SqlBasicCall)) {
            return;
        }
        SqlBasicCall newCall = (SqlBasicCall) retrieveReplacement(asExpression);
        asExpression.setOperand(0, newCall.operand(0));
        asExpression.setOperand(1, newCall.operand(1));
    }

    @Override
    public void enterBetweenNode(SqlBasicCall between) {
        registerReplacement(between, enterBetweenExpression(between));
    }

    protected SqlBasicCall enterBetweenExpression(SqlBasicCall between) {
        return between;
    }

    @Override
    public void exitBetweenNode(SqlBasicCall between) {
        replaceCallOperands(between);
    }

    @Override
    public void enterSelectExpressionNode(SqlBasicCall call) {
    }

    @Override
    public void exitSelectExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        replaceSqlSelect(select);
    }

    @Override
    public void enterBinaryExpressionNode(SqlBasicCall expression) {
    }

    protected SqlBasicCall enterBinaryExpression(SqlBasicCall expression) {
        return expression;
    }

    @Override
    public void exitBinaryExpressionNode(SqlBasicCall expression) {
    }

    @Override
    public void enterPlusExpressionNode(SqlBasicCall plus) {
        registerReplacement(plus, enterPlusExpression(plus));
    }

    protected SqlBasicCall enterPlusExpression(SqlBasicCall plus) {
        return plus;
    }

    @Override
    public void exitPlusExpressionNode(SqlBasicCall plus) {
        replaceCallOperands(plus);
    }

    @Override
    public void enterMinusExpressionNode(SqlBasicCall minus) {
        registerReplacement(minus, enterMinusExpression(minus));
    }

    protected SqlBasicCall enterMinusExpression(SqlBasicCall minus) {
        return minus;
    }

    @Override
    public void exitMinusExpressionNode(SqlBasicCall minus) {
        replaceCallOperands(minus);
    }

    @Override
    public void enterTimesExpressionNode(SqlBasicCall times) {
        registerReplacement(times, enterTimesExpression(times));
    }

    protected SqlBasicCall enterTimesExpression(SqlBasicCall times) {
        return times;
    }

    @Override
    public void exitTimesExpressionNode(SqlBasicCall times) {
        replaceCallOperands(times);
    }

    @Override
    public void enterDivideExpressionNode(SqlBasicCall divide) {
        registerReplacement(divide, enterDivideExpression(divide));
    }

    protected SqlBasicCall enterDivideExpression(SqlBasicCall divide) {
        return divide;
    }

    @Override
    public void exitDivideExpressionNode(SqlBasicCall divide) {
        replaceCallOperands(divide);
    }

    @Override
    public void enterModExpressionNode(SqlBasicCall mod) {
        registerReplacement(mod, enterModExpression(mod));
    }

    protected SqlBasicCall enterModExpression(SqlBasicCall mod) {
        return mod;
    }

    @Override
    public void exitModExpressionNode(SqlBasicCall mod) {
        replaceCallOperands(mod);
    }

    @Override
    public void enterAndExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterAndExpression(call));
    }

    protected SqlBasicCall enterAndExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitAndExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterOrExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterOrExpression(call));
    }

    protected SqlBasicCall enterOrExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitOrExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterNotExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterNotExpression(call));
    }

    protected SqlBasicCall enterNotExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitNotExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterEqualsExpression(call));
    }

    protected SqlBasicCall enterEqualsExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitEqualsExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterNotEqualsExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterNotEqualsExpression(call));
    }

    protected SqlBasicCall enterNotEqualsExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitNotEqualsExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterGreaterThanExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterGreaterThanExpression(call));
    }

    protected SqlBasicCall enterGreaterThanExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitGreaterThanExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall call) {
        registerReplacement(call, enterGreaterThanEqualsExpression(call));
    }

    protected SqlBasicCall enterGreaterThanEqualsExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitGreaterThanEqualsExpNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterLessThanExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterLessThanExpression(call));
    }

    protected SqlBasicCall enterLessThanExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitLessThanExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterLessThanEqualsExpression(call));
    }

    protected SqlBasicCall enterLessThanEqualsExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void exitLessThanEqualsExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterSubSelectNode(SqlSelect call) {
        registerReplacement(call, enterSubSelect(call));
    }

    protected SqlCall enterSubSelect(SqlSelect call) {
        return call;
    }

    @Override
    public void exitSubSelectNode(SqlSelect select) {
        replaceSqlSelect(select);
    }

    @Override
    public void enterOverExpressionNode(SqlBasicCall call) {
        registerReplacement(call, enterOverExpression(call));
    }

    protected SqlNode enterOverExpression(SqlBasicCall call) {
        return call;
    }

    @Override
    public void existOverExpressionNode(SqlBasicCall call) {
        replaceCallOperands(call);
    }

    @Override
    public void enterWithNode(SqlWith call) {
        registerReplacement(call, enterSqlWith(call));
    }

    protected SqlNode enterSqlWith(SqlWith call) {
        return call;
    }

    @Override
    public void exitWithNode(SqlWith call) {
    }

    @Override
    public void enterWithItemNode(SqlWithItem call) {
        registerReplacement(call, enterSqlWithItem(call));
    }

    protected SqlNode enterSqlWithItem(SqlWithItem call) {
        return call;
    }

    @Override
    public void exitWithItemNode(SqlWithItem call) {
    }

    @Override
    public void enterSelectColumnNode(SqlIdentifier column) {
        registerReplacement(column, enterSelectColumn(column));
    }

    protected SqlNode enterSelectColumn(SqlIdentifier column) {
        return column;
    }

    @Override
    public void exitSelectColumnNode(SqlIdentifier column) {
    }

    @Override
    public void enterCreateTableNode(SqlCreateTable createTable) {
        registerReplacement(createTable, enterCreateTable(createTable));
    }

    protected SqlNode enterCreateTable(SqlCreateTable createTable) {
        return createTable;
    }

    @Override
    public void exitCreateTableNode(SqlCreateTable createTable) {
    }

    @Override
    public void enterFunctionNode(SqlBasicCall node) {
        registerReplacement(node, enterFunction(node));
    }

    protected SqlNode enterFunction(SqlBasicCall node) {
        return node;
    }

    @Override
    public void exitFunctionNode(SqlBasicCall node) {
    }

    @Override
    public void enterCaseNode(SqlCase node) {
        registerReplacement(node, enterCase(node));
    }

    protected SqlNode enterCase(SqlCase node) {
        return node;
    }

    @Override
    public void exitCaseNode(SqlCase node) {
        SqlNodeList whenList = node.getWhenOperands();
        if (whenList != null) {
            for (int i = 0, size = whenList.size(); i < size; i++) {
                whenList.set(i, retrieveReplacement(whenList.get(i)));
            }
        }
        SqlNodeList thenList = node.getThenOperands();
        if (thenList != null) {
            for (int i = 0, size = thenList.size(); i < size; i++) {
                thenList.set(i, retrieveReplacement(thenList.get(i)));
            }
        }
        SqlNode elseExpr = node.getElseOperand();
        if (elseExpr != null) {
            node.setOperand(3, retrieveReplacement(elseExpr));
        }
    }

    @Override
    public void enterJoinNode(SqlJoin node) {
    }

    @Override
    public void exitJoinNode(SqlJoin node) {
    }

    protected void replaceSqlSelect(SqlSelect select) {
        if (select.getSelectList() != null) {
            select.setSelectList(
                    new SqlNodeList(retrieveReplacements(select.getSelectList().getList()),
                            SqlParserPos.ZERO));
        }
        if (select.getWhere() != null) {
            if (select.getWhere() instanceof SqlBasicCall) {
                SqlBasicCall where = (SqlBasicCall) select.getWhere();
                for (int i = 0; i < where.operands.length; i++) {
                    where.setOperand(i, retrieveReplacement(where.operands[i]));
                }
            }
            select.setWhere(retrieveReplacement(select.getWhere()));
        }
        if (select.getGroup() != null) {
            List<SqlNode> replacedGroupList = Lists.newArrayList();
            SqlNodeList groupList = select.getGroup();
            for (SqlNode node : groupList) {
                SqlNode replaceNode = retrieveReplacement(node);
                if (replaceNode instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) replaceNode;
                    if (call.getOperator() instanceof SqlAsOperator) {
                        replacedGroupList.add(call.operand(0));
                    } else {
                        replacedGroupList.add(call);
                    }
                } else {
                    replacedGroupList.add(replaceNode);
                }
            }
            select.setGroupBy(new SqlNodeList(replacedGroupList, SqlParserPos.ZERO));
        }
        if (select.getHaving() != null) {
            select.setHaving(retrieveReplacement(select.getHaving()));
        }
    }

    private void replaceCallOperands(SqlBasicCall bin) {
        SqlNode[] operands = bin.getOperands();
        for (int i = 0; i < operands.length; i++) {
            bin.setOperand(i, retrieveReplacement(operands[i]));
        }
    }
}
