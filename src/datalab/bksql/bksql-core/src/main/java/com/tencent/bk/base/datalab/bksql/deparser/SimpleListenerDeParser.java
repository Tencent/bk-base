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

public class SimpleListenerDeParser extends AbstractListenerDeParser {

    @Override
    protected Object getRetObj() {
        return null;
    }

    @Override
    public void enterDateLiteralNode(SqlDateLiteral dateValue) {

    }

    @Override
    public void exitDateLiteralNode(SqlDateLiteral dateValue) {

    }

    @Override
    public void enterTimeLiteralNode(SqlTimeLiteral timeValue) {

    }

    @Override
    public void exitTimeLiteralNode(SqlTimeLiteral dateValue) {

    }

    @Override
    public void enterTimestampLiteralNode(SqlTimestampLiteral timeStampValue) {

    }

    @Override
    public void exitTimestampLiteralNode(SqlTimestampLiteral timestampValue) {

    }

    @Override
    public void enterBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {

    }

    @Override
    public void exitBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {

    }

    @Override
    public void enterCharStringLiteralNode(SqlCharStringLiteral stringValue) {

    }

    @Override
    public void exitCharStringLiteralNode(SqlCharStringLiteral stringValue) {

    }

    @Override
    public void enterIntervalLiteralNode(SqlIntervalLiteral intervalValue) {

    }

    @Override
    public void exitIntervalLiteralNode(SqlIntervalLiteral intervalValue) {

    }

    @Override
    public void enterIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {

    }

    @Override
    public void exitIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {

    }

    @Override
    public void enterNumericLiteralNode(SqlNumericLiteral numericValue) {

    }

    @Override
    public void exitNumericLiteralNode(SqlNumericLiteral numericValue) {

    }

    @Override
    public void enterIdentifierNode(SqlIdentifier identifier) {

    }

    @Override
    public void exitIdentifierNode(SqlIdentifier identifier) {

    }

    @Override
    public void enterSetOptionNode(SqlSetOption setOption) {

    }

    @Override
    public void exitSetOptionNode(SqlSetOption setOption) {

    }

    @Override
    public void enterCallNode(SqlCall call) {

    }

    @Override
    public void exitCallNode(SqlCall call) {

    }

    @Override
    public void enterBasicCallNode(SqlBasicCall basicCall) {

    }

    @Override
    public void exitBasicCallNode(SqlBasicCall basicCall) {

    }

    @Override
    public void enterDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {

    }

    @Override
    public void exitDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {

    }

    @Override
    public void enterDeleteNode(SqlDelete delete) {

    }

    @Override
    public void exitDeleteNode(SqlDelete delete) {

    }

    @Override
    public void enterDescribeSchemaNode(SqlDescribeSchema describeSchema) {

    }

    @Override
    public void exitDescribeSchemaNode(SqlDescribeSchema describeSchema) {

    }

    @Override
    public void enterDescribeTableNode(SqlDescribeTable describeTable) {

    }

    @Override
    public void exitDescribeTableNode(SqlDescribeTable describeTable) {

    }

    @Override
    public void enterDropSchemaNode(SqlDropSchema dropSchema) {

    }

    @Override
    public void exitDropSchemaNode(SqlDropSchema dropSchema) {

    }

    @Override
    public void enterDropFunctionNode(SqlDropFunction dropFunction) {

    }

    @Override
    public void exitDropFunctionNode(SqlDropFunction dropFunction) {

    }

    @Override
    public void enterDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {

    }

    @Override
    public void exitDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {

    }

    @Override
    public void enterDropTableNode(SqlDropTable dropTable) {

    }

    @Override
    public void exitDropTableNode(SqlDropTable dropTable) {

    }

    @Override
    public void enterDropTypeNode(SqlDropType dropType) {

    }

    @Override
    public void exitDropTypeNode(SqlDropType dropType) {

    }

    @Override
    public void enterDropViewNode(SqlDropView dropView) {

    }

    @Override
    public void exitDropViewNode(SqlDropView dropView) {

    }

    @Override
    public void enterDynamicParamNode(SqlDynamicParam dynamicParam) {

    }

    @Override
    public void exitDynamicParamNode(SqlDynamicParam dynamicParam) {

    }

    @Override
    public void enterExplainNode(SqlExplain explain) {

    }

    @Override
    public void exitExplainNode(SqlExplain explain) {

    }

    @Override
    public void enterInsertNode(SqlInsert insert) {

    }

    @Override
    public void exitInsertNode(SqlInsert insert) {

    }

    @Override
    public void enterJoinNode(SqlJoin join) {

    }

    @Override
    public void exitJoinNode(SqlJoin join) {

    }

    @Override
    public void enterMatchRecognizeNode(SqlMatchRecognize matchRecognize) {

    }

    @Override
    public void exitMatchRecognizeNode(SqlMatchRecognize matchRecognize) {

    }

    @Override
    public void enterMergeNode(SqlMerge merge) {

    }

    @Override
    public void exitMergeNode(SqlMerge merge) {

    }

    @Override
    public void enterNodeListNode(SqlNodeList sqlNodes) {

    }

    @Override
    public void exitNodeListNode(SqlNodeList sqlNodes) {

    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {

    }

    @Override
    public void exitOrderByNode(SqlOrderBy orderBy) {

    }

    @Override
    public void enterSelectNode(SqlSelect select) {

    }

    @Override
    public void exitSelectNode(SqlSelect select) {

    }

    @Override
    public void enterSnapshotNode(SqlSnapshot snapshot) {

    }

    @Override
    public void exitSnapshotNode(SqlSnapshot snapshot) {

    }

    @Override
    public void enterUpdateNode(SqlUpdate update) {

    }

    @Override
    public void exitUpdateNode(SqlUpdate update) {

    }

    @Override
    public void enterCaseNode(SqlCase sqlCase) {

    }

    @Override
    public void exitCaseNode(SqlCase sqlCase) {

    }

    @Override
    public void enterCreateViewNode(SqlCreateView createView) {

    }

    @Override
    public void exitCreateViewNode(SqlCreateView createView) {

    }

    @Override
    public void enterCreateTypeNode(SqlCreateType createType) {

    }

    @Override
    public void exitCreateTypeNode(SqlCreateType createType) {

    }

    @Override
    public void enterCreateSchemaNode(SqlCreateSchema createSchema) {

    }

    @Override
    public void exitCreateSchemaNode(SqlCreateSchema createSchema) {

    }

    @Override
    public void enterCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {

    }

    @Override
    public void exitCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {

    }

    @Override
    public void enterCreateFunctionNode(SqlCreateFunction createFunction) {

    }

    @Override
    public void exitCreateFunctionNode(SqlCreateFunction createFunction) {

    }

    @Override
    public void enterCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {

    }

    @Override
    public void exitCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {

    }

    @Override
    public void enterColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {

    }

    @Override
    public void exitColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {

    }

    @Override
    public void enterCheckConstraintNode(SqlCheckConstraint checkConstraint) {

    }

    @Override
    public void exitCheckConstraint(SqlCheckConstraint checkConstraint) {

    }

    @Override
    public void enterAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {

    }

    @Override
    public void exitAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {

    }

    @Override
    public void enterTableNameNode(SqlIdentifier tableName) {

    }

    @Override
    public void existTableNameNode(SqlIdentifier tableName) {

    }

    @Override
    public void enterColumnNode(SqlIdentifier column) {

    }

    @Override
    public void exitColumnNode(SqlIdentifier column) {

    }

    @Override
    public void enterAllColumnsNode(SqlIdentifier allColumns) {

    }

    @Override
    public void exitAllColumnsNode(SqlIdentifier allColumns) {

    }

    @Override
    public void enterAllTableColumnsNode(SqlIdentifier allTableColumns) {

    }

    @Override
    public void exitAllTableColumnsNode(SqlIdentifier allTableColumns) {

    }

    @Override
    public void enterInExpressionNode(SqlBasicCall inExpression) {

    }


    @Override
    public void exitInExpressionNode(SqlBasicCall inExpression) {

    }

    @Override
    public void enterNotInExpressionNode(SqlBasicCall inExpression) {

    }


    @Override
    public void exitNotInExpressionNode(SqlBasicCall inExpression) {

    }

    @Override
    public void enterLikeExpressionNode(SqlBasicCall likeExpression) {

    }

    @Override
    public void exitLikeExpressionNode(SqlBasicCall likeExpression) {

    }

    @Override
    public void enterAsExpressionNode(SqlBasicCall asExpression) {

    }

    @Override
    public void exitAsExpressionNode(SqlBasicCall asExpression) {

    }

    @Override
    public void enterBetweenNode(SqlBasicCall between) {

    }

    @Override
    public void exitBetweenNode(SqlBasicCall between) {

    }

    @Override
    public void enterPrefixExpressionNode(SqlBasicCall prefixExpression) {

    }

    @Override
    public void exitPrefixExpressionNode(SqlBasicCall prefixExpression) {

    }

    @Override
    public void enterPostfixExpressionNode(SqlBasicCall postfixExpression) {

    }

    @Override
    public void exitPostfixExpressionNode(SqlBasicCall postfixExpression) {

    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {

    }

    @Override
    public void exitFunctionNode(SqlBasicCall function) {

    }

    @Override
    public void enterSelectExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitSelectExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {

    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {

    }

    @Override
    public void enterSetExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void exitSetExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void enterSetUnionExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void exitSetUnionExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void enterSetIntersectExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void exitSetIntersectExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void enterSetMinusExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void exitSetMinusExpressionNode(SqlBasicCall setOperationList) {

    }

    @Override
    public void enterQueryNode(SqlCall call) {

    }

    @Override
    public void exitQueryNode(SqlCall call) {

    }

    @Override
    public void enterBinaryExpressionNode(SqlBasicCall expression) {

    }

    @Override
    public void exitBinaryExpressionNode(SqlBasicCall expression) {

    }

    @Override
    public void enterPlusExpressionNode(SqlBasicCall plus) {

    }

    @Override
    public void exitPlusExpressionNode(SqlBasicCall plus) {

    }

    @Override
    public void enterMinusExpressionNode(SqlBasicCall minus) {

    }

    @Override
    public void exitMinusExpressionNode(SqlBasicCall minus) {

    }

    @Override
    public void enterTimesExpressionNode(SqlBasicCall times) {

    }

    @Override
    public void exitTimesExpressionNode(SqlBasicCall times) {

    }

    @Override
    public void enterDivideExpressionNode(SqlBasicCall divide) {

    }

    @Override
    public void exitDivideExpressionNode(SqlBasicCall divide) {

    }

    @Override
    public void enterModExpressionNode(SqlBasicCall mod) {

    }

    @Override
    public void exitModExpressionNode(SqlBasicCall mod) {

    }

    @Override
    public void enterAndExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitAndExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterOrExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitOrExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterNotExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitNotExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterNotEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitNotEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterGreaterThanExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitGreaterThanExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall call) {

    }

    @Override
    public void exitGreaterThanEqualsExpNode(SqlBasicCall call) {

    }

    @Override
    public void enterLessThanExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitLessThanExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void exitLessThanEqualsExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterSubSelectNode(SqlSelect subSelect) {

    }

    @Override
    public void exitSubSelectNode(SqlSelect subSelect) {

    }

    @Override
    public void enterOverExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void existOverExpressionNode(SqlBasicCall call) {

    }

    @Override
    public void enterWithNode(SqlWith with) {

    }

    @Override
    public void exitWithNode(SqlWith with) {

    }

    @Override
    public void enterWithItemNode(SqlWithItem withItem) {

    }

    @Override
    public void exitWithItemNode(SqlWithItem withItem) {

    }

    @Override
    public void enterSelectNodeListNode(SqlNodeList nodeList) {

    }

    @Override
    public void exitSelectNodeListNode(SqlNodeList nodeList) {

    }

    @Override
    public void enterSelectColumnNode(SqlIdentifier column) {

    }

    @Override
    public void exitSelectColumnNode(SqlIdentifier column) {

    }

    @Override
    public void enterCreateTableNode(SqlCreateTable createTable) {

    }

    @Override
    public void exitCreateTableNode(SqlCreateTable createTable) {

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
