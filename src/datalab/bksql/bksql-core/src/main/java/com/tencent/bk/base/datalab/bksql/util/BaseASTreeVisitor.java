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

package com.tencent.bk.base.datalab.bksql.util;

import static org.apache.calcite.sql.JoinConditionType.ON;
import static org.apache.calcite.sql.JoinConditionType.USING;

import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.FromItemVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ItemsListVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.SelectItemVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.SelectVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.StatementVisitor;
import java.util.Optional;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseASTreeVisitor
        extends
        AbstractASTreeVisitor
        implements SqlVisitor<Void>,
        StatementVisitor<Void>,
        SelectVisitor<Void>,
        SelectItemVisitor<Void>,
        ItemsListVisitor<Void>,
        FromItemVisitor<Void>,
        ExpressionVisitor<Void> {

    private static final Logger logger = LoggerFactory.getLogger(BaseASTreeVisitor.class);

    public BaseASTreeVisitor(ListenerBase listener) {
        super(listener);
    }

    @Override
    public Void visitInExpression(SqlBasicCall inExpression) {
        visitor().enterInExpressionNode(inExpression);
        iterate(inExpression.operands);
        visitor().exitInExpressionNode(inExpression);
        return null;
    }

    @Override
    public Void visitNotInExpression(SqlBasicCall notInExpression) {
        visitor().enterNotInExpressionNode(notInExpression);
        iterate(notInExpression.operands);
        visitor().exitNotInExpressionNode(notInExpression);
        return null;
    }

    @Override
    public Void visitLikeExpression(SqlBasicCall likeExpression) {
        visitor().enterLikeExpressionNode(likeExpression);
        iterate(likeExpression.operands);
        visitor().exitLikeExpressionNode(likeExpression);
        return null;
    }

    @Override
    public Void visitBetweenExpression(SqlBasicCall between) {
        visitor().enterBetweenNode(between);
        iterate(between.operands);
        visitor().exitBetweenNode(between);
        return null;
    }

    @Override
    public Void visitPrefixExpression(SqlBasicCall prefixExpression) {
        visitor().enterPrefixExpressionNode(prefixExpression);
        iterate(prefixExpression.operands);
        visitor().exitPrefixExpressionNode(prefixExpression);
        return null;
    }

    @Override
    public Void visitPostfixExpression(SqlBasicCall postfixExpression) {
        visitor().enterPostfixExpressionNode(postfixExpression);
        iterate(postfixExpression.operands);
        visitor().exitPostfixExpressionNode(postfixExpression);
        return null;
    }

    @Override
    public Void visitAsExpression(SqlBasicCall asExpression) {
        visitor().enterAsExpressionNode(asExpression);
        iterate(asExpression.operands);
        visitor().exitAsExpressionNode(asExpression);
        return null;
    }


    @Override
    public Void visitBinaryExpression(SqlBasicCall expression) {
        visitor().enterBinaryExpressionNode(expression);
        switch (expression.getKind()) {
            case PLUS:
                visitPlusExpression(expression);
                break;
            case MINUS:
                visitMinusExpression(expression);
                break;
            case TIMES:
                visitTimesExpression(expression);
                break;
            case DIVIDE:
                visitDivideExpression(expression);
                break;
            case MOD:
                visitModExpression(expression);
                break;
            case AND:
                visitAndExpression(expression);
                break;
            case OR:
                visitOrExpression(expression);
                break;
            case NOT:
                visitNotExpression(expression);
                break;
            case EQUALS:
                visitEqualsExpression(expression);
                break;
            case NOT_EQUALS:
                visitNotEqualsExpression(expression);
                break;
            case GREATER_THAN:
                visitGreaterThanExpression(expression);
                break;
            case GREATER_THAN_OR_EQUAL:
                visitGreaterThanEqualsExpression(expression);
                break;
            case LESS_THAN:
                visitLessThanExpression(expression);
                break;
            case LESS_THAN_OR_EQUAL:
                visitLessThanEqualsExpression(expression);
                break;
            default:
                break;
        }
        visitor().exitBinaryExpressionNode(expression);
        return null;
    }

    @Override
    public Void visitPlusExpression(SqlBasicCall plus) {
        visitor().enterPlusExpressionNode(plus);
        iterate(plus.operands);
        visitor().exitPlusExpressionNode(plus);
        return null;
    }

    @Override
    public Void visitMinusExpression(SqlBasicCall minus) {
        visitor().enterMinusExpressionNode(minus);
        iterate(minus.operands);
        visitor().exitMinusExpressionNode(minus);
        return null;
    }

    @Override
    public Void visitTimesExpression(SqlBasicCall times) {
        visitor().enterTimesExpressionNode(times);
        iterate(times.operands);
        visitor().exitTimesExpressionNode(times);
        return null;
    }

    @Override
    public Void visitDivideExpression(SqlBasicCall divide) {
        visitor().enterDivideExpressionNode(divide);
        iterate(divide.operands);
        visitor().exitDivideExpressionNode(divide);
        return null;
    }

    @Override
    public Void visitModExpression(SqlBasicCall mod) {
        visitor().enterModExpressionNode(mod);
        iterate(mod.operands);
        visitor().exitModExpressionNode(mod);
        return null;
    }

    @Override
    public Void visitAllColumns(SqlIdentifier allColumns) {
        visitor().enterAllColumnsNode(allColumns);
        allColumns.accept(this);
        visitor().exitAllColumnsNode(allColumns);
        return null;
    }

    @Override
    public Void visitAllTableColumns(SqlIdentifier allTableColumns) {
        visitor().enterAllTableColumnsNode(allTableColumns);
        allTableColumns.accept(this);
        visitor().exitAllTableColumnsNode(allTableColumns);
        return null;
    }

    @Override
    public Void visitColumn(SqlIdentifier column) {
        visitor().enterColumnNode(column);
        column.accept(this);
        visitor().exitColumnNode(column);
        return null;
    }

    @Override
    public Void visitSelectExpressionItem(SqlBasicCall call) {
        visitor().enterSelectExpressionNode(call);
        visitBasicCall(call);
        visitor().exitSelectExpressionNode(call);
        return null;
    }

    @Override
    public Void visitSelectNodeList(SqlNodeList nodeList) {
        visitor().enterSelectNodeListNode(nodeList);
        for (SqlNode node : nodeList) {
            if (node instanceof SqlIdentifier) {
                SqlIdentifier column = (SqlIdentifier) node;
                String columnName = column.toString();
                if ("*".equals(columnName)) {
                    visitAllColumns(column);
                } else if (columnName.endsWith(".*")) {
                    visitAllTableColumns(column);
                } else {
                    visitSelectColumn(column);
                }
            } else if (node instanceof SqlBasicCall) {
                SqlBasicCall itemExpression = (SqlBasicCall) node;
                visitSelectExpressionItem(itemExpression);
            }
        }
        visitor().exitSelectNodeListNode(nodeList);
        return null;
    }

    @Override
    public Void visitTable(SqlIdentifier tableName) {
        visitor().enterTableNameNode(tableName);
        tableName.accept(this);
        visitor().existTableNameNode(tableName);
        return null;
    }

    @Override
    public Void visitSubSelect(SqlSelect select) {
        visitor().enterSubSelectNode(select);
        visitSqlSelect(select);
        visitor().exitSubSelectNode(select);
        return null;
    }

    @Override
    public Void visitFunction(SqlBasicCall function) {
        visitor().enterFunctionNode(function);
        iterate(function.operands);
        visitor().exitFunctionNode(function);
        return null;
    }

    @Override
    public Void visitSignedExpression(SqlBasicCall function) {
        return null;
    }

    @Override
    public Void visitPlainSelect(SqlSelect select) {
        visitor().enterPlainSelectNode(select);
        visitSqlSelect(select);
        visitor().exitPlainSelectNode(select);
        return null;
    }

    @Override
    public Void visitSetExpression(SqlBasicCall setOperationList) {
        visitor().enterSetExpressionNode(setOperationList);
        SqlKind opKind = setOperationList.getKind();
        if (opKind == SqlKind.UNION) {
            visitSetUnionExpression(setOperationList);
        } else if (opKind == SqlKind.INTERSECT) {
            visitSetIntersectExpression(setOperationList);
        } else if (opKind == SqlKind.EXCEPT) {
            visitSetMinusExpression(setOperationList);
        }
        visitor().exitSetExpressionNode(setOperationList);
        return null;
    }

    @Override
    public Void visitSetUnionExpression(SqlBasicCall setOperationList) {
        visitor().enterSetUnionExpressionNode(setOperationList);
        setOperationList.getOperandList().forEach(node -> {
            if (node instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) node);
            } else {
                node.accept(this);
            }
        });
        visitor().exitSetUnionExpressionNode(setOperationList);
        return null;
    }

    @Override
    public Void visitSetIntersectExpression(SqlBasicCall setOperationList) {
        visitor().enterSetIntersectExpressionNode(setOperationList);
        for (SqlNode node : setOperationList.getOperandList()) {
            if (node instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) node);
            } else {
                node.accept(this);
            }
        }
        visitor().exitSetIntersectExpressionNode(setOperationList);
        return null;
    }

    @Override
    public Void visitSetMinusExpression(SqlBasicCall setOperationList) {
        visitor().enterSetMinusExpressionNode(setOperationList);
        for (SqlNode node : setOperationList.getOperandList()) {
            if (node instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) node);
            } else {
                node.accept(this);
            }
        }
        visitor().exitSetMinusExpressionNode(setOperationList);
        return null;
    }

    @Override
    public Void visitAndExpression(SqlBasicCall call) {
        visitor().enterAndExpressionNode(call);
        iterate(call.operands);
        visitor().exitAndExpressionNode(call);
        return null;
    }

    @Override
    public Void visitOrExpression(SqlBasicCall call) {
        visitor().enterOrExpressionNode(call);
        iterate(call.operands);
        visitor().exitOrExpressionNode(call);
        return null;
    }

    @Override
    public Void visitNotExpression(SqlBasicCall call) {
        visitor().enterNotExpressionNode(call);
        iterate(call.operands);
        visitor().exitNotExpressionNode(call);
        return null;
    }

    @Override
    public Void visitEqualsExpression(SqlBasicCall call) {
        visitor().enterEqualsExpressionNode(call);
        iterate(call.operands);
        visitor().exitEqualsExpressionNode(call);
        return null;
    }

    @Override
    public Void visitNotEqualsExpression(SqlBasicCall call) {
        visitor().enterNotEqualsExpressionNode(call);
        iterate(call.operands);
        visitor().exitNotEqualsExpressionNode(call);
        return null;
    }

    @Override
    public Void visitGreaterThanExpression(SqlBasicCall call) {
        visitor().enterGreaterThanExpressionNode(call);
        iterate(call.operands);
        visitor().exitGreaterThanExpressionNode(call);
        return null;
    }

    @Override
    public Void visitGreaterThanEqualsExpression(SqlBasicCall call) {
        visitor().enterGreaterThanEqualsExpNode(call);
        iterate(call.operands);
        visitor().exitGreaterThanEqualsExpNode(call);
        return null;
    }

    @Override
    public Void visitLessThanExpression(SqlBasicCall call) {
        visitor().enterLessThanExpressionNode(call);
        iterate(call.operands);
        visitor().exitLessThanExpressionNode(call);
        return null;
    }

    @Override
    public Void visitLessThanEqualsExpression(SqlBasicCall call) {
        visitor().enterLessThanEqualsExpressionNode(call);
        iterate(call.operands);
        visitor().exitLessThanEqualsExpressionNode(call);
        return null;
    }

    @Override
    public Void visitOverExpression(SqlBasicCall call) {
        visitor().enterOverExpressionNode(call);
        iterate(call.operands);
        visitor().existOverExpressionNode(call);
        return null;
    }

    @Override
    public Void visitSelectColumn(SqlIdentifier column) {
        visitor().enterSelectColumnNode(column);
        column.accept(this);
        visitor().exitSelectColumnNode(column);
        return null;
    }

    @Override
    public Void visitKeyWordList(SqlNodeList sqlNodes) {
        return null;
    }

    @Override
    public Void visitCreateTable(SqlCreateTable createTable) {
        visitor().enterCreateTableNode(createTable);
        Optional.ofNullable(createTable.operand(0))
                .ifPresent(node -> visitTable((SqlIdentifier) node));
        Optional.ofNullable(createTable.operand(2)).ifPresent(node -> visitCall((SqlCall) node));
        visitor().exitCreateTableNode(createTable);
        return null;
    }

    @Override
    public Void visitDelete(SqlDelete sqlDelete) {
        visitor().enterDeleteNode(sqlDelete);
        SqlNode target = sqlDelete.getTargetTable();
        SqlNode condition = sqlDelete.getCondition();
        target.accept(this);
        condition.accept(this);
        visitor().exitDeleteNode(sqlDelete);
        return null;
    }

    @Override
    public Void visitUpdate(SqlUpdate sqlUpdate) {
        visitor().enterUpdateNode(sqlUpdate);
        visitor().exitUpdateNode(sqlUpdate);
        return null;
    }

    @Override
    public Void visitInsert(SqlInsert sqlInsert) {
        visitor().enterInsertNode(sqlInsert);
        visitor().exitInsertNode(sqlInsert);
        return null;
    }

    @Override
    public Void visitShow(SqlShow sqlShow) {
        visitor().enterShowNode(sqlShow);
        visitor().exitShowNode(sqlShow);
        return null;
    }

    @Override
    public Void visitShowSql(SqlShowSql sqlShowSql) {
        visitor().enterShowSqlNode(sqlShowSql);
        visitor().exitShowSqlNode(sqlShowSql);
        return null;
    }

    @Override
    public Void visitGroupBy(SqlNode node) {
        visitor().enterGroupByNode(node);
        if (node instanceof SqlNodeList) {
            visit((SqlNodeList) node);
        }
        visitor().exitGroupByNode(node);
        return null;
    }

    @Override
    public Void visitDropModels(SqlDropModels sqlDropModels) {
        visitor().enterDropModelsNode(sqlDropModels);
        visitor().existDropModelsNode(sqlDropModels);
        return null;
    }

    @Override
    public Void visitExplain(SqlExplain node) {
        visitor().enterExplainNode(node);
        Optional.ofNullable(node.getExplicandum()).ifPresent(dum -> visitCall((SqlCall) dum));
        visitor().exitExplainNode(node);
        return null;
    }

    @Override
    public Void visit(SqlBasicCall call) {
        visitor().enterBasicCallNode(call);
        visitBasicCall(call);
        visitor().exitBasicCallNode(call);
        return null;
    }

    @Override
    public Void visit(SqlWith with) {
        visitor().enterWithNode(with);
        SqlNodeList withItemList = with.withList;
        SqlNode query = with.body;
        if (withItemList != null && withItemList.size() > 0) {
            for (SqlNode node : withItemList) {
                if (node instanceof SqlWithItem) {
                    visit((SqlWithItem) node);
                }
            }
        }

        if (query != null) {
            if (query instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) query);
            } else if (query instanceof SqlOrderBy) {
                visit((SqlOrderBy) query);
            }
        }
        visitor().exitWithNode(with);
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        visitor().enterCallNode(call);
        SqlKind kind = call.getKind();
        switch (kind) {
            case WITH:
            case ORDER_BY:
            case UNION:
            case INTERSECT:
            case EXCEPT:
            case SELECT:
            case CASE:
            case JOIN:
                visitQuery(call);
                break;
            default:
                visitCall(call);
                break;
        }
        visitor().exitCallNode(call);
        return null;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        visitor().enterNodeListNode(nodeList);
        for (SqlNode node : nodeList) {
            if (node instanceof SqlIdentifier) {
                SqlIdentifier column = (SqlIdentifier) node;
                String columnName = column.toString();
                if ("*".equals(columnName)) {
                    visitAllColumns(column);
                } else if (columnName.endsWith(".*")) {
                    visitAllTableColumns(column);
                } else {
                    visitColumn(column);
                }
            } else if (node instanceof SqlBasicCall) {
                visit((SqlBasicCall) node);
            }
        }
        visitor().exitNodeListNode(nodeList);
        return null;
    }

    @Override
    public Void visit(SqlSelect select) {
        visitor().enterSelectNode(select);
        visitSqlSelect(select);
        visitor().exitSelectNode(select);
        return null;
    }

    @Override
    public Void visit(SqlIdentifier id) {
        visitor().enterIdentifierNode(id);
        visitor().exitIdentifierNode(id);
        return null;
    }

    @Override
    public Void visit(SqlJoin join) {
        visitor().enterJoinNode(join);
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        if (left != null) {
            if (left instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) left);
            } else if (left instanceof SqlBasicCall) {
                visit((SqlBasicCall) left);
            } else {
                left.accept(this);
            }
        }
        if (right != null) {
            if (right instanceof SqlSelect) {
                visitPlainSelect((SqlSelect) right);
            } else if (right instanceof SqlBasicCall) {
                visit((SqlBasicCall) right);
            } else {
                right.accept(this);
            }
        }
        JoinConditionType conditionType = join.getConditionType();
        SqlNode condition = join.getCondition();
        if (conditionType == ON) {
            if (condition instanceof SqlBasicCall) {
                visit((SqlBasicCall) condition);
            } else {
                condition.accept(this);
            }
        } else if (conditionType == USING) {
            if (condition instanceof SqlNodeList) {
                visit((SqlNodeList) condition);
            } else {
                condition.accept(this);
            }
        }
        visitor().exitJoinNode(join);
        return null;
    }

    @Override
    public Void visit(SqlOrderBy orderBySelect) {
        visitor().enterOrderByNode(orderBySelect);
        SqlNode query = orderBySelect.query;
        if (query instanceof SqlSelect) {
            visitPlainSelect(((SqlSelect) query));
        } else if (query instanceof SqlOrderBy) {
            visit((SqlOrderBy) query);
        } else if (query instanceof SqlWith) {
            visit((SqlWith) query);
        } else if (query instanceof SqlBasicCall) {
            visit((SqlBasicCall) query);
        } else {
            query.accept(this);
        }
        SqlNodeList orderList = orderBySelect.orderList;
        orderList.accept(this);
        visitor().exitOrderByNode(orderBySelect);
        return null;
    }

    @Override
    public Void visit(SqlWithItem withItem) {
        visitor().enterWithItemNode(withItem);
        SqlNodeList columnList = withItem.columnList;
        SqlIdentifier alias = withItem.name;
        SqlNode query = withItem.query;

        if (alias != null) {
            alias.accept(this);
        }

        if (columnList != null && columnList.size() > 0) {
            for (SqlNode column : columnList) {
                if (column instanceof SqlIdentifier) {
                    column.accept(this);
                }
            }
        }

        if (query instanceof SqlCall) {
            SqlCall call = (SqlCall) query;
            visitCall(call);
        }
        visitor().exitWithItemNode(withItem);
        return null;
    }

    @Override
    public Void visit(SqlCase sqlCase) {
        visitor().enterCaseNode(sqlCase);
        Optional.ofNullable(sqlCase.getWhenOperands())
                .ifPresent(list -> list.forEach(node -> node.accept(this)));
        Optional.ofNullable(sqlCase.getThenOperands())
                .ifPresent(list -> list.forEach(node -> node.accept(this)));
        Optional.ofNullable(sqlCase.getElseOperand())
                .ifPresent(node -> node.accept(this));
        visitor().exitCaseNode(sqlCase);
        return null;
    }

    private void visitQuery(SqlCall call) {
        visitor().enterQueryNode(call);
        if (call instanceof SqlBasicCall) {
            visit((SqlBasicCall) call);
        } else if (call instanceof SqlCase) {
            visit((SqlCase) call);
        } else if (call instanceof SqlJoin) {
            visit((SqlJoin) call);
        } else if (call instanceof SqlSelect) {
            visitPlainSelect((SqlSelect) call);
        } else if (call instanceof SqlOrderBy) {
            visit((SqlOrderBy) call);
        } else if (call instanceof SqlWith) {
            visit((SqlWith) call);
        }
        visitor().exitQueryNode(call);
    }

    /**
     * 处理SqlBasicCall的公共方法
     *
     * @param call SqlBasicCall实例
     */
    private void visitBasicCall(SqlBasicCall call) {
        SqlKind kind = call.getKind();
        switch (kind) {
            case IN:
                visitInExpression(call);
                break;
            case NOT_IN:
                visitNotInExpression(call);
                break;
            case LIKE:
                visitLikeExpression(call);
                break;
            case AS:
                visitAsExpression(call);
                break;
            case BETWEEN:
                visitBetweenExpression(call);
                break;
            case OVER:
                visitOverExpression(call);
                break;
            case UNION:
            case INTERSECT:
            case EXCEPT:
                visitSetExpression(call);
                break;
            default:
                SqlOperator operator = call.getOperator();
                if (operator instanceof SqlPrefixOperator) {
                    visitPrefixExpression(call);
                } else if (operator instanceof SqlPostfixOperator) {
                    visitPostfixExpression(call);
                } else if (operator instanceof SqlBinaryOperator) {
                    visitBinaryExpression(call);
                } else if (operator instanceof SqlFunction) {
                    visitFunction(call);
                }
        }
    }

    private void visitCall(SqlCall call) {
        SqlKind kind = call.getKind();
        Object result = null;
        switch (kind) {
            case CASE:
                result = visit((SqlCase) call);
                break;
            case JOIN:
                result = visit((SqlJoin) call);
                break;
            case SELECT:
                result = visitPlainSelect((SqlSelect) call);
                break;
            case ORDER_BY:
                result = visit((SqlOrderBy) call);
                break;
            case WITH:
                result = visit((SqlWith) call);
                break;
            case CREATE_TABLE:
                result = visitCreateTable((SqlCreateTable) call);
                break;
            case DELETE:
                result = visitDelete((SqlDelete) call);
                break;
            case UPDATE:
                result = visitUpdate((SqlUpdate) call);
                break;
            case INSERT:
                result = visitInsert((SqlInsert) call);
                break;
            case EXPLAIN:
                result = visitExplain((SqlExplain) call);
                break;
            case WINDOW:
                result = visit((SqlWindow) call);
                break;
            case SHOW:
                result = visitShow((SqlShow) call);
                break;
            case SHOW_SQL:
                result = visitShowSql((SqlShowSql) call);
                break;
            case DROP_TABLE:
                result = visitDropModels((SqlDropModels) call);
                break;
            default:
                if (call instanceof SqlBasicCall) {
                    visit((SqlBasicCall) call);
                }
        }
        logger.debug("visit result:{}", result);
    }

    private void iterate(SqlNode[] operands) {
        if (operands != null) {
            for (SqlNode node : operands) {
                if (node instanceof SqlSelect) {
                    visitPlainSelect((SqlSelect) node);
                } else if (node instanceof SqlOrderBy) {
                    visit((SqlOrderBy) node);
                } else if (node instanceof SqlBasicCall) {
                    visit((SqlBasicCall) node);
                } else if (node instanceof SqlIdentifier) {
                    SqlIdentifier column = (SqlIdentifier) node;
                    String columnName = column.toString();
                    if ("*".equals(columnName)) {
                        visitAllColumns(column);
                    } else if (columnName.endsWith(".*")) {
                        visitAllTableColumns(column);
                    } else {
                        visitColumn(column);
                    }
                } else {
                    node.accept(this);
                }
            }
        }
    }

    /**
     * 处理SqlSelect节点的公共方法
     *
     * @param select SqlSelect实例
     */
    private void visitSqlSelect(SqlSelect select) {
        Optional.ofNullable(select.getSelectList()).ifPresent(this::visitSelectNodeList
        );
        Optional.ofNullable(select.getFrom()).ifPresent(node -> {
            SqlKind kind = node.getKind();
            switch (kind) {
                case IDENTIFIER:
                    visitTable((SqlIdentifier) node);
                    break;
                case SELECT:
                    visitSubSelect((SqlSelect) node);
                    break;
                case ORDER_BY:
                    visit((SqlOrderBy) node);
                    break;
                case JOIN:
                    visit((SqlJoin) node);
                    break;
                default:
                    if (node instanceof SqlCall) {
                        visitCall((SqlCall) node);
                    }
            }
        });
        Optional.ofNullable(select.getWhere()).ifPresent(node -> {
            if (node instanceof SqlBasicCall) {
                visit((SqlBasicCall) node);
            } else if (node instanceof SqlLiteral) {
                visit((SqlLiteral) node);
            } else {
                select.getWhere()
                        .accept(this);
            }
        });
        Optional.ofNullable(select.getGroup())
                .ifPresent(this::visitGroupBy
                );
        Optional.ofNullable(select.getHaving())
                .ifPresent(node -> node.accept(this));
    }
}
