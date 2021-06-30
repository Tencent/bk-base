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

package com.tencent.bk.base.datalab.bksql.util.calcite.deparser;

import static com.tencent.bk.base.datalab.bksql.enums.SpecialConvertFuncEnum.OTHER;

import com.google.common.base.Enums;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.enums.SpecialConvertFuncEnum;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitorAdapter;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.FromItemVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.SelectItemVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.SelectVisitor;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.StatementVisitor;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.MySqlIndexHint;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlAlterTableAddColumn;
import org.apache.calcite.sql.SqlAlterTableDropColumn;
import org.apache.calcite.sql.SqlAlterTableName;
import org.apache.calcite.sql.SqlAlterTableReNameColumn;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLateralOperator;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlTrainModel;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlItemOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlRollupOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;

public class SelectDeParser implements SelectVisitor<Void>, SelectItemVisitor<Void>,
        FromItemVisitor<Void> {

    private static final String WHITESPACE = "' '";
    private StringBuilder buffer = new StringBuilder();
    private ExpressionVisitor expressionVisitor = new ExpressionVisitorAdapter();
    private StatementVisitor statementVisitor;

    public SelectDeParser() {
    }

    public SelectDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
    }

    public StatementVisitor getStatementVisitor() {
        return statementVisitor;
    }

    public void setStatementVisitor(StatementVisitor statementVisitor) {
        this.statementVisitor = statementVisitor;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = expressionVisitor;
    }

    @Override
    public Void visitTable(SqlIdentifier tableName) {
        return null;
    }

    @Override
    public Void visitSubSelect(SqlSelect subSelect) {
        return null;
    }

    @Override
    public Void visitJoin(SqlJoin join) {
        return null;
    }

    @Override
    public Void visitTableFunction(SqlBasicCall tableFunction) {
        return null;
    }

    @Override
    public Void visitLateralSubSelect(SqlBasicCall tableFunction) {
        return null;
    }

    @Override
    public Void visitAllColumns(SqlIdentifier allColumns) {
        return null;
    }

    @Override
    public Void visitAllTableColumns(SqlIdentifier allTableColumns) {
        return null;
    }

    @Override
    public Void visitColumn(SqlIdentifier column) {
        return null;
    }

    @Override
    public Void visitSelectExpressionItem(SqlBasicCall selectExpressionItem) {
        return null;
    }

    @Override
    public Void visitPlainSelect(SqlSelect plainSelect) {
        return null;
    }

    @Override
    public Void visitOrderBySelect(SqlOrderBy orderBySelect) {
        return null;
    }

    @Override
    public Void visitWithItem(SqlWithItem withItem) {
        return null;
    }

    @Override
    public Void visitSetOperationList(SqlBasicCall setOpList) {
        return null;
    }

    @Override
    public Void visitSetExpression(SqlBasicCall setOperationList) {
        return null;
    }

    @Override
    public Void visitSetUnionExpression(SqlBasicCall setOperationList) {
        return null;
    }

    @Override
    public Void visitSetIntersectExpression(SqlBasicCall setOperationList) {
        return null;
    }

    @Override
    public Void visitSetMinusExpression(SqlBasicCall setOperationList) {
        return null;
    }

    @Override
    public Void visitSelectNodeList(SqlNodeList sqlNodes) {
        Preconditions.checkArgument(sqlNodes != null && sqlNodes.size() > 0);
        for (Iterator<SqlNode> iter = sqlNodes.iterator(); iter.hasNext(); ) {
            SqlNode selectItem = iter.next();
            boolean addParenthesis = false;
            if (selectItem instanceof SqlSelect) {
                addParenthesis = true;
            }
            if (addParenthesis) {
                buffer.append("(");
            }
            selectItem.accept(this);
            if (addParenthesis) {
                buffer.append(")");
            }
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
        return null;
    }

    @Override
    public Void visitSelectColumn(SqlIdentifier column) {
        return null;
    }

    @Override
    public Void visitKeyWordList(SqlNodeList keyWordList) {
        for (Iterator<SqlNode> iter = keyWordList.iterator(); iter.hasNext(); ) {
            SqlNode keyWord = iter.next();
            if (keyWord instanceof SqlLiteral) {
                keyWord.accept(this);
                buffer.append(" ");
            }
        }
        return null;
    }

    @Override
    public Void visitCreateTable(SqlCreateTable createTable) {
        SqlNode tableName = createTable.operand(0);
        SqlNode query = createTable.operand(2);
        boolean isCreateTableAs = tableName instanceof SqlIdentifier
                && query instanceof SqlCall;
        if (isCreateTableAs) {
            buffer.append("CREATE TABLE ");
            visit((SqlIdentifier) tableName);
            buffer.append(" AS ");
            visit((SqlCall) query);
        }
        return null;
    }

    @Override
    public Void visitCreateTableFromModel(SqlCreateTableFromModel createTableFromModel) {
        buffer.append("CREATE TABLE ");
        SqlNode tableName = createTableFromModel.operand(0);
        visit((SqlIdentifier) tableName);
        buffer.append("AS RUN ");
        SqlNode query = createTableFromModel.operand(2);
        visit((SqlCall) query);
        return null;
    }

    @Override
    public Void visitTrainModel(SqlTrainModel trainModel) {
        buffer.append("TRAIN MODEL ");
        SqlNode modelName = trainModel.operand(0);
        visit((SqlIdentifier)modelName);
        buffer.append("OPTIONS(");
        SqlNode optionList = trainModel.operand(1);
        visit((SqlNodeList) optionList);
        buffer.append(") FROM ");
        SqlNode query = trainModel.operand(2);
        visit((SqlSelect)query);
        return null;
    }

    @Override
    public Void visitDelete(SqlDelete sqlDelete) {
        buffer.append("DELETE FROM ");
        visit((SqlIdentifier) sqlDelete.getTargetTable());
        buffer.append(" WHERE ");
        visit((SqlCall) sqlDelete.getCondition());
        return null;
    }

    @Override
    public Void visitUpdate(SqlUpdate sqlUpdate) {
        final SqlNode targetTable = sqlUpdate.getTargetTable();
        final SqlNode condition = sqlUpdate.getCondition();
        final SqlNodeList updateColumnList = sqlUpdate.getTargetColumnList();
        final SqlNodeList updateExpList = sqlUpdate.getSourceExpressionList();
        buffer.append("UPDATE ");
        visit((SqlIdentifier) targetTable);
        buffer.append(" SET ");
        boolean first = true;
        for (int i = 0; i < updateColumnList.size(); i++) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            updateColumnList.get(i).accept(this);
            buffer.append(" = ");
            updateExpList.get(i).accept(this);
        }
        buffer.append(" WHERE ");
        visit((SqlCall) condition);
        return null;
    }

    @Override
    public Void visitExplain(SqlExplain sqlExplain) {
        buffer.append("EXPLAIN");
        SqlExplainFormat format = sqlExplain.getFormat();
        switch (format) {
            case JSON:
                buffer.append(" (FORMAT JSON, ");
                break;
            case XML:
                buffer.append(" (FORMAT XML, ");
                break;
            default:
                buffer.append(" (FORMAT TEXT, ");
        }
        SqlExplainType type = sqlExplain.getType();
        switch (type) {
            case LOGICAL:
                buffer.append("TYPE LOGICAL) ");
                break;
            case VALIDATE:
                buffer.append("TYPE VALIDATE) ");
                break;
            case IO:
                buffer.append("TYPE IO) ");
                break;
            default:
                buffer.append("TYPE DISTRIBUTED) ");
        }
        SqlNode explicanDum = sqlExplain.getExplicandum();
        visit((SqlCall) explicanDum);
        return null;
    }

    @Override
    public Void visitDropTable(SqlDropTable sqlDropTable) {
        return null;
    }

    @Override
    public Void visitInsert(SqlInsert sqlInsert) {
        if (sqlInsert.isFromModel()) {
            return null;
        }
        buffer.append("INSERT ");
        if (sqlInsert.isOverwrite()) {
            buffer.append("OVERWRITE ");
        } else {
            buffer.append("INTO ");
        }
        visit((SqlIdentifier) sqlInsert.getTargetTable());
        if (sqlInsert.getTargetColumnList() != null) {
            buffer.append("(");
            visit(sqlInsert.getTargetColumnList());
            buffer.append(")");
        }
        buffer.append(" ");
        visit((SqlCall) sqlInsert.getSource());
        return null;
    }

    @Override
    public Void visitDropModels(SqlDropModels sqlDropModels) {
        buffer.append("DROP ");
        if (sqlDropModels.isDropModels()) {
            buffer.append("MODEL ");
        } else {
            buffer.append("TABLE ");
        }
        if (sqlDropModels.isIfExists()) {
            buffer.append("IF EXISTS ");
        }
        visit(sqlDropModels.getName());
        return null;
    }

    @Override
    public Void visitAlter(SqlAlter call) {
        if (call instanceof SqlAlterTableName) {
            visitAlterTableName((SqlAlterTableName) call);
        } else if (call instanceof SqlAlterTableReNameColumn) {
            visitAlterTableReNameColumn((SqlAlterTableReNameColumn) call);
        } else if (call instanceof SqlAlterTableDropColumn) {
            visitAlterTableDropColumn((SqlAlterTableDropColumn) call);
        } else if (call instanceof SqlAlterTableAddColumn) {
            visitAlterTableAddColumn((SqlAlterTableAddColumn) call);
        } else {
            throw new RuntimeException(
                    String.format("Not support SqlOperator:%s", call.getOperator()
                            .getName()));
        }
        return null;
    }

    @Override
    public Void visitAlterTableName(SqlAlterTableName sqlAlterTableName) {
        buffer.append("ALTER TABLE ");
        visit(sqlAlterTableName.getFromTableName());
        buffer.append(" RENAME TO ");
        visit(sqlAlterTableName.getToTableName());
        return null;
    }

    @Override
    public Void visitAlterTableDropColumn(SqlAlterTableDropColumn sqlAlterTableDropColumn) {
        buffer.append("ALTER TABLE ");
        visit(sqlAlterTableDropColumn.getName());
        SqlNodeList columnList = sqlAlterTableDropColumn.getColumnList();
        final boolean[] isFirst = {true};
        columnList.forEach(column -> {
            if (isFirst[0]) {
                buffer.append(" DROP COLUMN ");
                visit((SqlIdentifier) column);
                isFirst[0] = false;
            } else {
                buffer.append(", DROP COLUMN ");
                visit((SqlIdentifier) column);
            }
        });
        return null;
    }

    @Override
    public Void visitAlterTableReNameColumn(SqlAlterTableReNameColumn sqlAlterTableReNameColumn) {
        buffer.append("ALTER TABLE ");
        visit(sqlAlterTableReNameColumn.getName());
        buffer.append(" RENAME COLUMN ");
        visit(sqlAlterTableReNameColumn.getFromColumnName());
        buffer.append(" TO ");
        visit(sqlAlterTableReNameColumn.getToColumnName());
        return null;
    }

    @Override
    public Void visitAlterTableAddColumn(SqlAlterTableAddColumn sqlAlterTableAddColumn) {
        buffer.append("ALTER TABLE ");
        visit(sqlAlterTableAddColumn.getName());
        boolean isFirst = true;
        for (SqlNode c : sqlAlterTableAddColumn.getColumnList()) {
            SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c;
            if (isFirst) {
                buffer.append(" ADD COLUMN ");
                visitColumnDeclaration(columnDeclaration);
                isFirst = false;
            } else {
                buffer.append(", ADD COLUMN ");
                visitColumnDeclaration(columnDeclaration);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlWindow sqlWindow) {
        SqlNodeList partList = sqlWindow.getPartitionList();
        boolean hasPartList = partList != null && partList.size() > 0;
        if (hasPartList) {
            buffer.append("PARTITION BY ");
            visitSelectNodeList(partList);
        }
        SqlNodeList orderList = sqlWindow.getOrderList();
        if (orderList != null && orderList.size() > 0) {
            if (hasPartList) {
                buffer.append(" ");
            }
            buffer.append("ORDER BY ");
            visitSelectNodeList(orderList);
        }
        SqlNode lowerBound = sqlWindow.getLowerBound();
        SqlNode upperBound = sqlWindow.getUpperBound();
        deparseSqlWindowBound(sqlWindow, lowerBound, upperBound);
        return null;
    }

    @Override
    public Void visit(SqlSelect select) {
        buffer.append("SELECT ");
        if (select.isKeywordPresent(SqlSelectKeyword.DISTINCT)) {
            buffer.append("DISTINCT ");
        }
        Optional.ofNullable(select.getSelectList())
                .ifPresent(node -> node.accept(this));
        Optional.ofNullable(select.getFrom()).ifPresent(node -> {
            buffer.append(" FROM ");
            SqlNode fromNode = node;
            SqlKind kind = fromNode.getKind();
            switch (kind) {
                case SELECT:
                    buffer.append("(");
                    visit((SqlSelect) fromNode);
                    buffer.append(")");
                    break;
                case IDENTIFIER:
                    visit((SqlIdentifier) fromNode);
                    break;
                case JOIN:
                    visit((SqlJoin) fromNode);
                    break;
                case ORDER_BY:
                    buffer.append("(");
                    visit((SqlOrderBy) fromNode);
                    buffer.append(")");
                    break;
                default:
                    if (fromNode instanceof SqlBasicCall) {
                        visit((SqlBasicCall) fromNode);
                    } else {
                        fromNode.accept(this);
                    }
            }
        });
        Optional.ofNullable(select.getMySqlIndexHint()).ifPresent(node -> {
            buffer.append(" ");
            node.accept(this);
        });
        Optional.ofNullable(select.getWhere()).ifPresent(node -> {
            buffer.append(" WHERE ");
            node.accept(this);
        });
        Optional.ofNullable(select.getGroup()).ifPresent(node -> {
            buffer.append(" GROUP BY ");
            node.accept(this);
        });
        Optional.ofNullable(select.getHaving()).ifPresent(node -> {
            buffer.append(" HAVING ");
            node.accept(this);
        });
        if (ObjectUtils.anyNotNull(select.getOffset(), select.getFetch())) {
            this.buffer.append(" LIMIT ");
            Optional.ofNullable(select.getOffset()).ifPresent(
                    node -> buffer.append((((SqlNumericLiteral) node).getValue() + ", ")));
            Optional.ofNullable(select.getFetch()).ifPresent(
                    node -> buffer.append((((SqlNumericLiteral) node).getValue())));
        }
        return null;
    }

    @Override
    public Void visit(SqlOrderBy orderBySelect) {
        if (orderBySelect.query != null) {
            orderBySelect.query.accept(this);
        }
        if (orderBySelect.orderList != null && orderBySelect.orderList.size() > 0) {
            buffer.append(" ORDER BY ");
            orderBySelect.orderList.accept(this);
        }
        if (ObjectUtils.anyNotNull(orderBySelect.offset, orderBySelect.fetch)) {
            buffer.append(" LIMIT ");
            if (orderBySelect.offset != null) {
                if (orderBySelect.offset instanceof SqlNumericLiteral) {
                    buffer.append(((SqlNumericLiteral) orderBySelect.offset).getValue() + ", ");
                }
            }
            if (orderBySelect.fetch instanceof SqlNumericLiteral) {
                buffer.append(((SqlNumericLiteral) orderBySelect.fetch).getValue());
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlWithItem withItem) {
        SqlNodeList columnList = withItem.columnList;
        SqlIdentifier alias = withItem.name;
        SqlNode query = withItem.query;
        if (alias != null) {
            buffer.append(" ");
            alias.accept(this);
        }
        if (columnList != null && columnList.size() > 0) {
            buffer.append(" (");
            boolean isHead = true;
            for (SqlNode column : columnList) {
                if (column instanceof SqlIdentifier) {
                    if (!isHead) {
                        buffer.append(", ");
                    } else {
                        isHead = false;
                    }
                    column.accept(this);
                }
            }
            buffer.append(")");
        }
        if (query instanceof SqlCall) {
            buffer.append(" AS (");
            SqlCall call = (SqlCall) query;
            call.accept(this);
            buffer.append(")");
        }
        return null;
    }

    @Override
    public Void visit(SqlWith with) {
        SqlNodeList withItemList = with.withList;
        SqlNode query = with.body;
        buffer.append("WITH");
        if (withItemList != null && withItemList.size() > 0) {
            boolean isHead = true;
            for (SqlNode node : withItemList) {
                if (node instanceof SqlWithItem) {
                    if (!isHead) {
                        buffer.append(", ");
                    } else {
                        isHead = false;
                    }
                    visit((SqlWithItem) node);
                }
            }
        }
        if (query != null) {
            buffer.append(" ");
            query.accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlSetOperator setOperator) {
        return null;
    }

    @Override
    public Void visit(SqlJoin join) {
        JoinType joinType = join.getJoinType();
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        JoinConditionType conditionType = join.getConditionType();
        SqlNode condition = join.getCondition();
        if (joinType == JoinType.COMMA) {
            left.accept(this);
            buffer.append(", ");
            right.accept(this);
        }
        if (joinType == JoinType.CROSS) {
            left.accept(this);
            buffer.append(" CROSS JOIN ");
            right.accept(this);
        } else if (joinType == JoinType.INNER) {
            left.accept(this);
            buffer.append(" INNER JOIN ");
            right.accept(this);
        } else if (joinType == JoinType.FULL) {
            left.accept(this);
            buffer.append(" FULL OUTER JOIN ");
            right.accept(this);
        } else if (joinType == JoinType.LEFT) {
            left.accept(this);
            if (join.isNatural()) {
                buffer.append(" NATURAL");
            }
            buffer.append(" LEFT OUTER JOIN ");
            right.accept(this);
        } else if (joinType == JoinType.RIGHT) {
            left.accept(this);
            if (join.isNatural()) {
                buffer.append(" NATURAL");
            }
            buffer.append(" RIGHT OUTER JOIN ");
            right.accept(this);
        }

        visitJoinCondition(conditionType, condition);

        return null;
    }

    @Override
    public Void visit(SqlLateralOperator lateralOperator) {
        return null;
    }

    @Override
    public Void visit(SqlLiteral literal) {
        literal.accept(expressionVisitor);
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            visit((SqlBasicCall) call);
            return null;
        }
        switch (call.getKind()) {
            case CASE:
                call.accept(expressionVisitor);
                break;
            case JOIN:
                visit((SqlJoin) call);
                break;
            case SELECT:
                visit((SqlSelect) call);
                break;
            case ORDER_BY:
                visit((SqlOrderBy) call);
                break;
            case WINDOW:
                visit((SqlWindow) call);
                break;
            case WITH:
                visit((SqlWith) call);
                break;
            case DELETE:
                visitDelete((SqlDelete) call);
                break;
            case UPDATE:
                visitUpdate((SqlUpdate) call);
                break;
            case CREATE_TABLE:
                if (call instanceof SqlCreateTableFromModel) {
                    visitCreateTableFromModel((SqlCreateTableFromModel)call);
                } else {
                    visitCreateTable((SqlCreateTable) call);
                }
                break;
            case ALTER_TABLE:
                visitAlter((SqlAlter) call);
                break;
            case DROP_TABLE:
                visitDropModels((SqlDropModels) call);
                break;
            case EXPLAIN:
                visitExplain((SqlExplain) call);
                break;
            case INSERT:
                visitInsert((SqlInsert) call);
                break;
            case USE_INDEX:
                visit((MySqlIndexHint) call);
                break;
            case TRAIN:
                visitTrainModel((SqlTrainModel)call);
                break;
            default:
                throw new RuntimeException(
                        String.format("Not support SqlOperator:%s", call.getOperator()
                                .getName()));
        }
        return null;
    }

    @Override
    public Void visit(SqlNodeList nodeList) {
        for (Iterator<SqlNode> iter = nodeList.iterator(); iter.hasNext(); ) {
            SqlNode selectItem = iter.next();
            boolean addParenthesis = false;
            if (selectItem instanceof SqlSelect) {
                addParenthesis = true;
            }
            if (addParenthesis) {
                buffer.append("(");
            }
            selectItem.accept(this);
            if (addParenthesis) {
                buffer.append(")");
            }
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlIdentifier identifier) {
        buffer.append(identifier.toString());
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
        SqlTypeNameSpec typeNameSpec = type.getTypeNameSpec();
        deparseSqlTypeName(typeNameSpec);
        return null;
    }

    @Override
    public Void visit(SqlDynamicParam param) {
        return null;
    }

    @Override
    public Void visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }

    @Override
    public Void visit(SqlBasicCall call) {
        SqlOperator operator = call.getOperator();
        if (operator != null) {
            if (operator instanceof SqlInOperator) {
                visitIn(call, operator);
            } else if (operator instanceof SqlLikeOperator) {
                visitLike(call, (SqlLikeOperator) operator);
            } else if (operator instanceof SqlAsOperator) {
                visitAs(call);
            } else if (operator instanceof SqlFunction) {
                visitFunction(call, operator);
            } else if (operator instanceof SqlBetweenOperator) {
                visitBetweenAnd(call, (SqlBetweenOperator) operator);
            } else if (operator instanceof SqlPrefixOperator) {
                visitPrefixOp(call, operator);
            } else if (operator instanceof SqlPostfixOperator) {
                visitPostfixOp(call, operator);
            } else if (operator instanceof SqlOverOperator) {
                visitOver(call, operator);
            } else if (operator instanceof SqlBinaryOperator) {
                visitBinary(call, operator);
            } else if (operator instanceof SqlRollupOperator) {
                visitRollup(call, operator);
            } else if (operator instanceof SqlRowOperator) {
                visitRow(call, operator);
            } else if (operator instanceof SqlItemOperator) {
                visitItem(call, operator);
            } else if (operator instanceof SqlArrayValueConstructor) {
                visitArrayOrMapValue(call, operator);
            } else if (operator instanceof SqlMapValueConstructor) {
                visitArrayOrMapValue(call, operator);
            }
        }
        return null;
    }

    @Override
    public Void visit(MySqlIndexHint mySqlIndexHint) {
        visit(mySqlIndexHint.getAction());
        buffer.append(" ");
        visit(mySqlIndexHint.getIndexQualifier());
        buffer.append(" (");
        visit(mySqlIndexHint.getIndexList());
        buffer.append(")");
        return null;
    }

    /**
     * 连接条件解析
     *
     * @param conditionType 连接类型
     * @param condition 连接条件
     */
    protected void visitJoinCondition(JoinConditionType conditionType, SqlNode condition) {
        if (conditionType == JoinConditionType.ON) {
            buffer.append(" ON ");
        } else if (conditionType == JoinConditionType.USING) {
            buffer.append(" USING ");
        }
        if (condition != null) {
            buffer.append("(");
            condition.accept(this);
            buffer.append(")");
        }
    }

    protected void deparseSqlTypeName(SqlTypeNameSpec typeNameSpec) {
        if (typeNameSpec instanceof SqlBasicTypeNameSpec) {
            SqlBasicTypeNameSpec basicTypeNameSpec = (SqlBasicTypeNameSpec) typeNameSpec;
            int precision = basicTypeNameSpec.getPrecision();
            int scale = basicTypeNameSpec.getScale();
            buffer.append(basicTypeNameSpec.getTypeName());
            if (precision >= 0) {
                buffer.append("(").append(precision);
                if (scale >= 0) {
                    buffer.append(", ").append(scale);
                }
                buffer.append(")");
            }
        } else if (typeNameSpec instanceof SqlCollectionTypeNameSpec) {
            SqlCollectionTypeNameSpec collectionTypeNameSpec =
                    (SqlCollectionTypeNameSpec) typeNameSpec;
            SqlIdentifier collectionType = collectionTypeNameSpec.getTypeName();
            SqlTypeNameSpec elementType = collectionTypeNameSpec.getElementTypeName();
            collectionType.accept(this);
            buffer.append("(");
            deparseSqlTypeName(elementType);
            buffer.append(")");
        } else {
            buffer.append(typeNameSpec.getTypeName());
        }
    }

    /**
     * deparse 函数里面的参数
     *
     * @param funcName 函数名
     * @param operands 函数操作数
     * @param quantifier 函数量词
     */
    protected void visitFunctionOperands(String funcName, List<SqlNode> operands,
            SqlLiteral quantifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(funcName));
        buffer.append(funcName);
        buffer.append("(");
        if (quantifier != null) {
            buffer.append(quantifier.getValue())
                    .append(" ");
        }

        SpecialConvertFuncEnum funcEnum = Enums
                .getIfPresent(SpecialConvertFuncEnum.class,
                        funcName.toUpperCase(Locale.ENGLISH))
                .or(OTHER);
        switch (funcEnum) {
            case CONVERT:
                deparseConvertFunction(operands);
                break;
            case CAST:
            case TRY_CAST:
                deparseCastFunction(operands);
                break;
            case TRIM:
                deparseTrimFunction(operands);
                break;
            default:
                deparseOtherFunction(operands);
        }
        buffer.append(")");
    }

    protected void deparseTrimFunction(List<SqlNode> operands) {
        Preconditions.checkArgument(operands != null);
        String flag = ((SqlLiteral) operands.get(0)).getValue().toString();
        String trimStr = ((SqlLiteral) operands.get(1)).getValue().toString();
        SqlNode column = operands.get(2);
        if (WHITESPACE.equals(trimStr)) {
            if (Flag.BOTH.name().equals(flag)) {
                column.accept(this);
            }
        } else {
            buffer.append(String.format("%s %s FROM ", flag.toUpperCase(), trimStr));
            column.accept(this);
        }
    }

    protected void deparseOtherFunction(List<SqlNode> operands) {
        Preconditions.checkArgument(operands != null);
        boolean first = true;
        for (Iterator<SqlNode> iter = operands.iterator(); iter.hasNext(); ) {
            SqlNode item = iter.next();
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            item.accept(this);
        }
    }

    protected void deparseCastFunction(List<SqlNode> operands) {
        Preconditions.checkArgument(operands != null && operands.size() == 2);
        SqlNode firstOp = operands.get(0);
        SqlNode secondOp = operands.get(1);
        firstOp.accept(this);
        buffer.append(" AS ");
        secondOp.accept(this);
    }

    protected void deparseConvertFunction(List<SqlNode> operands) {
        Preconditions.checkArgument(operands != null && operands.size() == 2);
        SqlNode firstOp = operands.get(0);
        SqlNode secondOp = operands.get(1);
        if (secondOp instanceof SqlIdentifier) {
            firstOp.accept(this);
            buffer.append(" USING ");
            secondOp.accept(this);
        } else if (secondOp instanceof SqlDataTypeSpec) {
            firstOp.accept(this);
            buffer.append(", ");
            secondOp.accept(this);
        } else {
            throw new RuntimeException(
                    String.format("ConvertFunction's second operator is illegal:%s",
                            secondOp.toString()));
        }
    }

    protected void deparseCallOperands(SqlNode[] operands, String delimiter) {
        boolean first = true;
        for (SqlNode operand : operands) {
            if (first) {
                first = false;
            } else {
                buffer.append(String.format("%s ", delimiter));
            }
            operand.accept(this);
        }
    }

    protected void visitBinary(SqlBasicCall call, SqlOperator operator) {
        SqlNode left = call.operands[0];
        SqlNode right = call.operands[1];
        final boolean leftParenth = shouldAddParentheses(left);
        final boolean rightParenth = shouldAddParentheses(right);
        if (leftParenth) {
            buffer.append("(");
        }
        left.accept(this);
        if (leftParenth) {
            buffer.append(")");
        }
        buffer.append(" " + operator.getName() + " ");
        if (rightParenth) {
            buffer.append("(");
        }
        right.accept(this);
        if (rightParenth) {
            buffer.append(")");
        }
    }

    protected void visitRow(SqlBasicCall call, SqlOperator operator) {
        buffer.append("(");
        deparseCallOperands(call.operands, ",");
        buffer.append(")");
    }

    protected void visitRollup(SqlBasicCall call, SqlOperator operator) {
        SqlKind kind = operator.getKind();
        switch (kind) {
            case CUBE:
                buffer.append("CUBE(");
                break;
            case ROLLUP:
                buffer.append("ROLLUP(");
                break;
            case GROUPING_SETS:
                buffer.append("GROUPING SETS(");
                break;
            default:
                throw new RuntimeException(
                        String.format("not support roll up operator:%s", kind.lowerName));
        }
        deparseCallOperands(call.operands, ",");
        buffer.append(")");
    }

    protected void visitOver(SqlBasicCall call, SqlOperator operator) {
        call.getOperands()[0].accept(this);
        buffer.append(" OVER(");
        call.getOperands()[1].accept(this);
        buffer.append(")");
    }

    protected void visitPostfixOp(SqlBasicCall call, SqlOperator operator) {
        call.getOperands()[0].accept(this);
        buffer.append(" " + operator.getName());
    }

    protected void visitPrefixOp(SqlBasicCall call, SqlOperator operator) {
        if (operator.getKind() == SqlKind.EXPLICIT_TABLE) {
            buffer.append("(");
            buffer.append(operator.toString() + " ");
            call.getOperands()[0].accept(this);
            buffer.append(")");
        } else {
            buffer.append(operator.getName() + " ");
            buffer.append("(");
            boolean addParentheses = call.getOperands()[0] instanceof SqlSelect;
            if (addParentheses) {
                buffer.append("(");
            }
            call.getOperands()[0].accept(this);
            if (addParentheses) {
                buffer.append(")");
            }
            buffer.append(")");
        }
    }

    protected void visitBetweenAnd(SqlBasicCall call, SqlBetweenOperator operator) {
        call.getOperands()[0].accept(this);
        if (operator.isNegated()) {
            buffer.append(" NOT");
        }
        buffer.append(" BETWEEN ");
        call.getOperands()[1].accept(this);
        buffer.append(" AND ");
        call.getOperands()[2].accept(this);
    }

    protected void visitFunction(SqlBasicCall call, SqlOperator operator) {
        String funcName = operator.getName();
        if (call.getOperands() == null || call.operandCount() == 0) {
            buffer.append(funcName);
            buffer.append("()");
        } else {
            visitFunctionOperands(funcName, call.getOperandList(),
                    call.getFunctionQuantifier());
        }
    }

    protected void visitAs(SqlBasicCall call) {
        boolean addParentheses = true;
        SqlNode beforeAs = call.getOperands()[0];
        if (beforeAs instanceof SqlCall) {
            SqlCall sqlCall = ((SqlCall) beforeAs);
            if (sqlCall.getOperator() instanceof SqlFunction) {
                addParentheses = false;
            }
            if (sqlCall.getOperator() instanceof SqlCaseOperator) {
                addParentheses = false;
            }
        }
        if (beforeAs instanceof SqlIdentifier || beforeAs instanceof SqlLiteral) {
            addParentheses = false;
        }
        if (addParentheses) {
            buffer.append("(");
        }
        beforeAs.accept(this);
        if (addParentheses) {
            buffer.append(")");
        }
        buffer.append(" AS ");
        call.getOperands()[1].accept(this);
    }

    protected void visitLike(SqlBasicCall call, SqlLikeOperator operator) {
        call.getOperands()[0].accept(this);
        SqlLikeOperator likeOperator = operator;
        buffer.append(String.format(" %s ", likeOperator.getName()));
        call.getOperands()[1].accept(this);
    }

    protected void visitIn(SqlBasicCall call, SqlOperator operator) {
        call.getOperands()[0].accept(this);
        buffer.append(String.format(" %s ", operator.getName()));
        buffer.append("(");
        call.getOperands()[1].accept(this);
        buffer.append(")");
    }

    protected void visitItem(SqlBasicCall call, SqlOperator operator) {
        call.getOperands()[0].accept(this);
        buffer.append("[");
        call.getOperands()[1].accept(this);
        buffer.append("]");
    }

    protected void visitArrayOrMapValue(SqlBasicCall call, SqlOperator operator) {
        buffer.append(String.format("%s[", operator.getName()));
        deparseCallOperands(call.operands, ",");
        buffer.append("]");
    }

    protected void visitColumnDeclaration(SqlColumnDeclaration columnDeclaration) {
        visit(columnDeclaration.getName());
        buffer.append(" ");
        visit(columnDeclaration.getDataType());
        ColumnStrategy columnStrategy = columnDeclaration.getStrategy();
        switch (columnStrategy) {
            case NULLABLE:
                break;
            case NOT_NULLABLE:
                buffer.append(" NOT NULL");
                break;
            default:
                buffer.append(columnStrategy);
        }
    }

    protected boolean shouldAddParentheses(SqlNode sqlNode) {
        boolean checkResult = false;
        if (sqlNode instanceof SqlCall) {
            checkResult = true;
        }
        return checkResult;
    }

    /**
     * 解析窗口函数 上下边界
     *
     * @param sqlWindow SqlWindow实例
     * @param lowerBound 下边界
     * @param upperBound 上边界
     */
    protected void deparseSqlWindowBound(SqlWindow sqlWindow, SqlNode lowerBound,
            SqlNode upperBound) {
        if (lowerBound != null || upperBound != null) {
            if (sqlWindow.isRows()) {
                buffer.append(" ROWS ");
            } else {
                buffer.append(" RANGE ");
            }
        }
        if (lowerBound != null && upperBound != null) {
            buffer.append("BETWEEN ");
            lowerBound.accept(this);
            buffer.append(" AND ");
            upperBound.accept(this);
        } else if (lowerBound != null) {
            lowerBound.accept(this);
        }
    }
}
