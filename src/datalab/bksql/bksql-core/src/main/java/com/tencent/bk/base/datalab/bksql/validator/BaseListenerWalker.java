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

import com.tencent.bk.base.datalab.bksql.util.BaseASTreeVisitor;
import com.tencent.bk.base.datalab.bksql.util.ListenerBase;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDescribeSchema;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlCheckConstraint;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateForeignSchema;
import org.apache.calcite.sql.ddl.SqlCreateFunction;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropFunction;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropType;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;

public class BaseListenerWalker extends AbstractASTreeWalker implements ListenerBase {

    private final SqlVisitor visitor;

    public BaseListenerWalker() {
        visitor = new BaseASTreeVisitor(this);
    }

    @Override
    public final void walk(SqlNode statement) {
        statement.accept(visitor());
    }

    protected final SqlVisitor visitor() {
        return visitor;
    }

    @Override
    public void enterDateLiteralNode(SqlDateLiteral node) {

    }

    @Override
    public void exitDateLiteralNode(SqlDateLiteral node) {

    }

    @Override
    public void enterTimeLiteralNode(SqlTimeLiteral node) {

    }

    @Override
    public void exitTimeLiteralNode(SqlTimeLiteral node) {

    }

    @Override
    public void enterTimestampLiteralNode(SqlTimestampLiteral node) {

    }

    @Override
    public void exitTimestampLiteralNode(SqlTimestampLiteral node) {

    }

    @Override
    public void enterBinaryStringLiteralNode(SqlBinaryStringLiteral node) {

    }

    @Override
    public void exitBinaryStringLiteralNode(SqlBinaryStringLiteral node) {

    }

    @Override
    public void enterCharStringLiteralNode(SqlCharStringLiteral node) {

    }

    @Override
    public void exitCharStringLiteralNode(SqlCharStringLiteral node) {

    }

    @Override
    public void enterIntervalLiteralNode(SqlIntervalLiteral node) {

    }

    @Override
    public void exitIntervalLiteralNode(SqlIntervalLiteral node) {

    }

    @Override
    public void enterIntervalQualifierNode(SqlIntervalQualifier node) {

    }

    @Override
    public void exitIntervalQualifierNode(SqlIntervalQualifier node) {

    }

    @Override
    public void enterNumericLiteralNode(SqlNumericLiteral node) {

    }

    @Override
    public void exitNumericLiteralNode(SqlNumericLiteral node) {

    }

    @Override
    public void enterIdentifierNode(SqlIdentifier node) {

    }

    @Override
    public void exitIdentifierNode(SqlIdentifier node) {

    }

    @Override
    public void enterSetOptionNode(SqlSetOption node) {

    }

    @Override
    public void exitSetOptionNode(SqlSetOption node) {

    }

    @Override
    public void enterCallNode(SqlCall node) {

    }

    @Override
    public void exitCallNode(SqlCall node) {

    }

    @Override
    public void enterBasicCallNode(SqlBasicCall node) {

    }

    @Override
    public void exitBasicCallNode(SqlBasicCall node) {

    }

    @Override
    public void enterDataTypeSpecNode(SqlDataTypeSpec node) {

    }

    @Override
    public void exitDataTypeSpecNode(SqlDataTypeSpec node) {

    }

    @Override
    public void enterDeleteNode(SqlDelete node) {

    }

    @Override
    public void exitDeleteNode(SqlDelete node) {

    }

    @Override
    public void enterDescribeSchemaNode(SqlDescribeSchema node) {

    }

    @Override
    public void exitDescribeSchemaNode(SqlDescribeSchema node) {

    }

    @Override
    public void enterDescribeTableNode(SqlDescribeTable node) {

    }

    @Override
    public void exitDescribeTableNode(SqlDescribeTable node) {

    }

    @Override
    public void enterDropSchemaNode(SqlDropSchema node) {

    }

    @Override
    public void exitDropSchemaNode(SqlDropSchema node) {

    }

    @Override
    public void enterDropFunctionNode(SqlDropFunction node) {

    }

    @Override
    public void exitDropFunctionNode(SqlDropFunction node) {

    }

    @Override
    public void enterDropMaterializedViewNode(SqlDropMaterializedView node) {

    }

    @Override
    public void exitDropMaterializedViewNode(SqlDropMaterializedView node) {

    }

    @Override
    public void enterDropTableNode(SqlDropTable node) {

    }

    @Override
    public void exitDropTableNode(SqlDropTable node) {

    }

    @Override
    public void enterDropTypeNode(SqlDropType node) {

    }

    @Override
    public void exitDropTypeNode(SqlDropType node) {

    }

    @Override
    public void enterDropViewNode(SqlDropView node) {

    }

    @Override
    public void exitDropViewNode(SqlDropView node) {

    }

    @Override
    public void enterDynamicParamNode(SqlDynamicParam node) {

    }

    @Override
    public void exitDynamicParamNode(SqlDynamicParam node) {

    }

    @Override
    public void enterExplainNode(SqlExplain node) {

    }

    @Override
    public void exitExplainNode(SqlExplain node) {

    }

    @Override
    public void enterInsertNode(SqlInsert node) {

    }

    @Override
    public void exitInsertNode(SqlInsert node) {

    }

    @Override
    public void enterJoinNode(SqlJoin node) {

    }

    @Override
    public void exitJoinNode(SqlJoin node) {

    }

    @Override
    public void enterMatchRecognizeNode(SqlMatchRecognize node) {

    }

    @Override
    public void exitMatchRecognizeNode(SqlMatchRecognize node) {

    }

    @Override
    public void enterMergeNode(SqlMerge node) {

    }

    @Override
    public void exitMergeNode(SqlMerge node) {

    }

    @Override
    public void enterNodeListNode(SqlNodeList node) {

    }

    @Override
    public void exitNodeListNode(SqlNodeList node) {

    }

    @Override
    public void enterOrderByNode(SqlOrderBy node) {

    }

    @Override
    public void exitOrderByNode(SqlOrderBy node) {

    }

    @Override
    public void enterSelectNode(SqlSelect node) {

    }

    @Override
    public void exitSelectNode(SqlSelect node) {

    }

    @Override
    public void enterSnapshotNode(SqlSnapshot node) {

    }

    @Override
    public void exitSnapshotNode(SqlSnapshot node) {

    }

    @Override
    public void enterUpdateNode(SqlUpdate node) {

    }

    @Override
    public void exitUpdateNode(SqlUpdate node) {

    }

    @Override
    public void enterCaseNode(SqlCase node) {

    }

    @Override
    public void exitCaseNode(SqlCase node) {

    }

    @Override
    public void enterCreateViewNode(SqlCreateView node) {

    }

    @Override
    public void exitCreateViewNode(SqlCreateView node) {

    }

    @Override
    public void enterCreateTypeNode(SqlCreateType node) {

    }

    @Override
    public void exitCreateTypeNode(SqlCreateType node) {

    }

    @Override
    public void enterCreateSchemaNode(SqlCreateSchema node) {

    }

    @Override
    public void exitCreateSchemaNode(SqlCreateSchema node) {

    }

    @Override
    public void enterCreateMaterializedViewNode(SqlCreateMaterializedView node) {

    }

    @Override
    public void exitCreateMaterializedViewNode(SqlCreateMaterializedView node) {

    }

    @Override
    public void enterCreateFunctionNode(SqlCreateFunction node) {

    }

    @Override
    public void exitCreateFunctionNode(SqlCreateFunction node) {

    }

    @Override
    public void enterCreateForeignSchemaNode(SqlCreateForeignSchema node) {

    }

    @Override
    public void exitCreateForeignSchemaNode(SqlCreateForeignSchema node) {

    }

    @Override
    public void enterColumnDeclarationNode(SqlColumnDeclaration node) {

    }

    @Override
    public void exitColumnDeclarationNode(SqlColumnDeclaration node) {

    }

    @Override
    public void enterCheckConstraintNode(SqlCheckConstraint node) {

    }

    @Override
    public void exitCheckConstraint(SqlCheckConstraint node) {

    }

    @Override
    public void enterAttributeDefinitionNode(SqlAttributeDefinition node) {

    }

    @Override
    public void exitAttributeDefinitionNode(SqlAttributeDefinition node) {

    }

    @Override
    public void enterTableNameNode(SqlIdentifier node) {

    }

    @Override
    public void existTableNameNode(SqlIdentifier node) {

    }

    @Override
    public void enterColumnNode(SqlIdentifier node) {

    }

    @Override
    public void exitColumnNode(SqlIdentifier node) {

    }

    @Override
    public void enterAllColumnsNode(SqlIdentifier node) {

    }

    @Override
    public void exitAllColumnsNode(SqlIdentifier node) {

    }

    @Override
    public void enterAllTableColumnsNode(SqlIdentifier node) {

    }

    @Override
    public void exitAllTableColumnsNode(SqlIdentifier node) {

    }

    @Override
    public void enterInExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitInExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterNotInExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitNotInExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterLikeExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitLikeExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterAsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitAsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterBetweenNode(SqlBasicCall node) {

    }

    @Override
    public void exitBetweenNode(SqlBasicCall node) {

    }

    @Override
    public void enterPrefixExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitPrefixExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterPostfixExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitPostfixExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterFunctionNode(SqlBasicCall node) {

    }

    @Override
    public void exitFunctionNode(SqlBasicCall node) {

    }

    @Override
    public void enterSelectExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitSelectExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterPlainSelectNode(SqlSelect node) {

    }

    @Override
    public void exitPlainSelectNode(SqlSelect node) {

    }

    @Override
    public void enterSetExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitSetExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterSetUnionExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitSetUnionExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterSetIntersectExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitSetIntersectExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterSetMinusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitSetMinusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterQueryNode(SqlCall node) {

    }

    @Override
    public void exitQueryNode(SqlCall node) {

    }

    @Override
    public void enterBinaryExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitBinaryExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterPlusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitPlusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterMinusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitMinusExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterTimesExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitTimesExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterDivideExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitDivideExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterModExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitModExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterAndExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitAndExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterOrExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitOrExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterNotExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitNotExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterNotEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitNotEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterGreaterThanExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitGreaterThanExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall node) {

    }

    @Override
    public void exitGreaterThanEqualsExpNode(SqlBasicCall node) {

    }

    @Override
    public void enterLessThanExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitLessThanExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void exitLessThanEqualsExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterSubSelectNode(SqlSelect node) {

    }

    @Override
    public void exitSubSelectNode(SqlSelect node) {

    }

    @Override
    public void enterOverExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void existOverExpressionNode(SqlBasicCall node) {

    }

    @Override
    public void enterWithNode(SqlWith node) {

    }

    @Override
    public void exitWithNode(SqlWith node) {

    }

    @Override
    public void enterWithItemNode(SqlWithItem node) {

    }

    @Override
    public void exitWithItemNode(SqlWithItem node) {

    }

    @Override
    public void enterSelectNodeListNode(SqlNodeList node) {

    }

    @Override
    public void exitSelectNodeListNode(SqlNodeList node) {

    }

    @Override
    public void enterSelectColumnNode(SqlIdentifier node) {

    }

    @Override
    public void exitSelectColumnNode(SqlIdentifier node) {

    }

    @Override
    public void enterCreateTableNode(SqlCreateTable node) {

    }

    @Override
    public void exitCreateTableNode(SqlCreateTable node) {

    }

    @Override
    public void enterShowNode(SqlShow node) {

    }

    @Override
    public void exitShowNode(SqlShow node) {

    }

    @Override
    public void enterShowSqlNode(SqlShowSql node) {

    }

    @Override
    public void exitShowSqlNode(SqlShowSql node) {

    }

    @Override
    public void enterGroupByNode(SqlNode node) {

    }

    @Override
    public void exitGroupByNode(SqlNode node) {

    }

    @Override
    public void enterDropModelsNode(SqlDropModels node) {

    }

    @Override
    public void existDropModelsNode(SqlDropModels node) {

    }
}
