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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.util.ListenerBase;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDescribeSchema;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
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

public class ParallelGroup extends BaseListenerWalker {

    private final List<ListenerBase> listenerBases = new ArrayList<>();

    public ParallelGroup(@JsonProperty("groups") List<ASTreeWalker> groups) {
        for (ASTreeWalker group : groups) {
            try {
                // strictly require for a listener compatible implementation
                ListenerBase listenerBase = (ListenerBase) group;
                listenerBases.add(listenerBase);
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to create node listener", e);
            }
        }
    }

    @Override
    public void enterDateLiteralNode(SqlDateLiteral dateValue) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDateLiteralNode(dateValue));
    }

    @Override
    public void exitDateLiteralNode(SqlDateLiteral dateValue) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDateLiteralNode(dateValue));
    }

    @Override
    public void enterTimeLiteralNode(SqlTimeLiteral timeValue) {
        listenerBases.forEach(listenerBase -> listenerBase.enterTimeLiteralNode(timeValue));
    }

    @Override
    public void exitTimeLiteralNode(SqlTimeLiteral dateValue) {
        listenerBases.forEach(listenerBase -> listenerBase.exitTimeLiteralNode(dateValue));
    }

    @Override
    public void enterTimestampLiteralNode(SqlTimestampLiteral timeStampValue) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterTimestampLiteralNode(timeStampValue));
    }

    @Override
    public void exitTimestampLiteralNode(SqlTimestampLiteral timestampValue) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitTimestampLiteralNode(timestampValue));
    }

    @Override
    public void enterBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterBinaryStringLiteralNode(binaryStringValue));
    }

    @Override
    public void exitBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {
        listenerBases.forEach(
                listenerBase -> listenerBase.exitBinaryStringLiteralNode(binaryStringValue));
    }

    @Override
    public void enterCharStringLiteralNode(SqlCharStringLiteral stringValue) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCharStringLiteralNode(stringValue));
    }

    @Override
    public void exitCharStringLiteralNode(SqlCharStringLiteral stringValue) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCharStringLiteralNode(stringValue));
    }

    @Override
    public void enterIntervalLiteralNode(SqlIntervalLiteral intervalValue) {
        listenerBases.forEach(listenerBase -> listenerBase.enterIntervalLiteralNode(intervalValue));
    }

    @Override
    public void exitIntervalLiteralNode(SqlIntervalLiteral intervalValue) {
        listenerBases.forEach(listenerBase -> listenerBase.exitIntervalLiteralNode(intervalValue));
    }

    @Override
    public void enterIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterIntervalQualifierNode(intervalQualifier));
    }

    @Override
    public void exitIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitIntervalQualifierNode(intervalQualifier));
    }

    @Override
    public void enterNumericLiteralNode(SqlNumericLiteral numericValue) {
        listenerBases.forEach(listenerBase -> listenerBase.enterNumericLiteralNode(numericValue));
    }

    @Override
    public void exitNumericLiteralNode(SqlNumericLiteral numericValue) {
        listenerBases.forEach(listenerBase -> listenerBase.exitNumericLiteralNode(numericValue));
    }

    @Override
    public void enterIdentifierNode(SqlIdentifier identifier) {
        listenerBases.forEach(listenerBase -> listenerBase.enterIdentifierNode(identifier));
    }

    @Override
    public void exitIdentifierNode(SqlIdentifier identifier) {
        listenerBases.forEach(listenerBase -> listenerBase.exitIdentifierNode(identifier));
    }

    @Override
    public void enterSetOptionNode(SqlSetOption setOption) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSetOptionNode(setOption));
    }

    @Override
    public void exitSetOptionNode(SqlSetOption setOption) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSetOptionNode(setOption));
    }

    @Override
    public void enterCallNode(SqlCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCallNode(call));
    }

    @Override
    public void exitCallNode(SqlCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCallNode(call));
    }

    @Override
    public void enterBasicCallNode(SqlBasicCall basicCall) {
        listenerBases.forEach(listenerBase -> listenerBase.enterBasicCallNode(basicCall));
    }

    @Override
    public void exitBasicCallNode(SqlBasicCall basicCall) {
        listenerBases.forEach(listenerBase -> listenerBase.exitBasicCallNode(basicCall));
    }

    @Override
    public void enterDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDataTypeSpecNode(dataTypeSpec));
    }

    @Override
    public void exitDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDataTypeSpecNode(dataTypeSpec));
    }

    @Override
    public void enterDeleteNode(SqlDelete delete) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDeleteNode(delete));
    }

    @Override
    public void exitDeleteNode(SqlDelete delete) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDeleteNode(delete));
    }

    @Override
    public void enterDescribeSchemaNode(SqlDescribeSchema describeSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDescribeSchemaNode(describeSchema));
    }

    @Override
    public void exitDescribeSchemaNode(SqlDescribeSchema describeSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDescribeSchemaNode(describeSchema));
    }

    @Override
    public void enterDescribeTableNode(SqlDescribeTable describeTable) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDescribeTableNode(describeTable));
    }

    @Override
    public void exitDescribeTableNode(SqlDescribeTable describeTable) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDescribeTableNode(describeTable));
    }

    @Override
    public void enterDropSchemaNode(SqlDropSchema dropSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDropSchemaNode(dropSchema));
    }

    @Override
    public void exitDropSchemaNode(SqlDropSchema dropSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDropSchemaNode(dropSchema));
    }

    @Override
    public void enterDropFunctionNode(SqlDropFunction dropFunction) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDropFunctionNode(dropFunction));
    }

    @Override
    public void exitDropFunctionNode(SqlDropFunction dropFunction) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDropFunctionNode(dropFunction));
    }

    @Override
    public void enterDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterDropMaterializedViewNode(dropMaterializedView));
    }

    @Override
    public void exitDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {
        listenerBases.forEach(
                listenerBase -> listenerBase.exitDropMaterializedViewNode(dropMaterializedView));
    }

    @Override
    public void enterDropTableNode(SqlDropTable dropTable) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDropTableNode(dropTable));
    }

    @Override
    public void exitDropTableNode(SqlDropTable dropTable) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDropTableNode(dropTable));
    }

    @Override
    public void enterDropTypeNode(SqlDropType dropType) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDropTypeNode(dropType));
    }

    @Override
    public void exitDropTypeNode(SqlDropType dropType) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDropTypeNode(dropType));
    }

    @Override
    public void enterDropViewNode(SqlDropView dropView) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDropViewNode(dropView));
    }

    @Override
    public void exitDropViewNode(SqlDropView dropView) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDropViewNode(dropView));
    }

    @Override
    public void enterDynamicParamNode(SqlDynamicParam dynamicParam) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDynamicParamNode(dynamicParam));
    }

    @Override
    public void exitDynamicParamNode(SqlDynamicParam dynamicParam) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDynamicParamNode(dynamicParam));
    }

    @Override
    public void enterExplainNode(SqlExplain explain) {
        listenerBases.forEach(listenerBase -> listenerBase.enterExplainNode(explain));
    }

    @Override
    public void exitExplainNode(SqlExplain explain) {
        listenerBases.forEach(listenerBase -> listenerBase.exitExplainNode(explain));
    }

    @Override
    public void enterInsertNode(SqlInsert insert) {
        listenerBases.forEach(listenerBase -> listenerBase.enterInsertNode(insert));
    }

    @Override
    public void exitInsertNode(SqlInsert insert) {
        listenerBases.forEach(listenerBase -> listenerBase.exitInsertNode(insert));
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        listenerBases.forEach(listenerBase -> listenerBase.enterJoinNode(join));
    }

    @Override
    public void exitJoinNode(SqlJoin join) {
        listenerBases.forEach(listenerBase -> listenerBase.exitJoinNode(join));
    }

    @Override
    public void enterMatchRecognizeNode(SqlMatchRecognize matchRecognize) {
        listenerBases.forEach(listenerBase -> listenerBase.enterMatchRecognizeNode(matchRecognize));
    }

    @Override
    public void exitMatchRecognizeNode(SqlMatchRecognize matchRecognize) {
        listenerBases.forEach(listenerBase -> listenerBase.exitMatchRecognizeNode(matchRecognize));
    }

    @Override
    public void enterMergeNode(SqlMerge merge) {
        listenerBases.forEach(listenerBase -> listenerBase.enterMergeNode(merge));
    }

    @Override
    public void exitMergeNode(SqlMerge merge) {
        listenerBases.forEach(listenerBase -> listenerBase.exitMergeNode(merge));
    }

    @Override
    public void enterNodeListNode(SqlNodeList sqlNodes) {
        listenerBases.forEach(listenerBase -> listenerBase.enterNodeListNode(sqlNodes));
    }

    @Override
    public void exitNodeListNode(SqlNodeList sqlNodes) {
        listenerBases.forEach(listenerBase -> listenerBase.exitNodeListNode(sqlNodes));
    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {
        listenerBases.forEach(listenerBase -> listenerBase.enterOrderByNode(orderBy));
    }

    @Override
    public void exitOrderByNode(SqlOrderBy orderBy) {
        listenerBases.forEach(listenerBase -> listenerBase.exitOrderByNode(orderBy));
    }

    @Override
    public void enterSelectNode(SqlSelect select) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSelectNode(select));
    }

    @Override
    public void exitSelectNode(SqlSelect select) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSelectNode(select));
    }

    @Override
    public void enterSnapshotNode(SqlSnapshot snapshot) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSnapshotNode(snapshot));
    }

    @Override
    public void exitSnapshotNode(SqlSnapshot snapshot) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSnapshotNode(snapshot));
    }

    @Override
    public void enterUpdateNode(SqlUpdate update) {
        listenerBases.forEach(listenerBase -> listenerBase.enterUpdateNode(update));
    }

    @Override
    public void exitUpdateNode(SqlUpdate update) {
        listenerBases.forEach(listenerBase -> listenerBase.exitUpdateNode(update));
    }

    @Override
    public void enterCaseNode(SqlCase sqlCase) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCaseNode(sqlCase));
    }

    @Override
    public void exitCaseNode(SqlCase sqlCase) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCaseNode(sqlCase));
    }

    @Override
    public void enterCreateViewNode(SqlCreateView createView) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCreateViewNode(createView));
    }

    @Override
    public void exitCreateViewNode(SqlCreateView createView) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCreateViewNode(createView));
    }

    @Override
    public void enterCreateTypeNode(SqlCreateType createType) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCreateTypeNode(createType));
    }

    @Override
    public void exitCreateTypeNode(SqlCreateType createType) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCreateTypeNode(createType));
    }

    @Override
    public void enterCreateSchemaNode(SqlCreateSchema createSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCreateSchemaNode(createSchema));
    }

    @Override
    public void exitCreateSchemaNode(SqlCreateSchema createSchema) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCreateSchemaNode(createSchema));
    }

    @Override
    public void enterCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {
        listenerBases.forEach(listenerBase -> listenerBase
                .enterCreateMaterializedViewNode(createMaterializedView));
    }

    @Override
    public void exitCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {
        listenerBases.forEach(listenerBase -> listenerBase
                .exitCreateMaterializedViewNode(createMaterializedView));
    }

    @Override
    public void enterCreateFunctionNode(SqlCreateFunction createFunction) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCreateFunctionNode(createFunction));
    }

    @Override
    public void exitCreateFunctionNode(SqlCreateFunction createFunction) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCreateFunctionNode(createFunction));
    }

    @Override
    public void enterCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterCreateForeignSchemaNode(createForeignSchema));
    }

    @Override
    public void exitCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {
        listenerBases.forEach(
                listenerBase -> listenerBase.exitCreateForeignSchemaNode(createForeignSchema));
    }

    @Override
    public void enterColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterColumnDeclarationNode(columnDeclaration));
    }

    @Override
    public void exitColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitColumnDeclarationNode(columnDeclaration));
    }

    @Override
    public void enterCheckConstraintNode(SqlCheckConstraint checkConstraint) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterCheckConstraintNode(checkConstraint));
    }

    @Override
    public void exitCheckConstraint(SqlCheckConstraint checkConstraint) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCheckConstraint(checkConstraint));
    }

    @Override
    public void enterAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterAttributeDefinitionNode(attributeDefinition));
    }

    @Override
    public void exitAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {
        listenerBases.forEach(
                listenerBase -> listenerBase.exitAttributeDefinitionNode(attributeDefinition));
    }

    @Override
    public void enterTableNameNode(SqlIdentifier tableName) {
        listenerBases.forEach(listenerBase -> listenerBase.enterTableNameNode(tableName));
    }

    @Override
    public void existTableNameNode(SqlIdentifier tableName) {
        listenerBases.forEach(listenerBase -> listenerBase.existTableNameNode(tableName));
    }

    @Override
    public void enterColumnNode(SqlIdentifier column) {
        listenerBases.forEach(listenerBase -> listenerBase.enterColumnNode(column));

    }

    @Override
    public void exitColumnNode(SqlIdentifier column) {
        listenerBases.forEach(listenerBase -> listenerBase.exitColumnNode(column));
    }

    @Override
    public void enterAllColumnsNode(SqlIdentifier allColumns) {
        listenerBases.forEach(listenerBase -> listenerBase.enterAllColumnsNode(allColumns));
    }

    @Override
    public void exitAllColumnsNode(SqlIdentifier allColumns) {
        listenerBases.forEach(listenerBase -> listenerBase.exitAllColumnsNode(allColumns));
    }

    @Override
    public void enterAllTableColumnsNode(SqlIdentifier allTableColumns) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterAllTableColumnsNode(allTableColumns));
    }

    @Override
    public void exitAllTableColumnsNode(SqlIdentifier allTableColumns) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitAllTableColumnsNode(allTableColumns));
    }

    @Override
    public void enterInExpressionNode(SqlBasicCall inExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.enterInExpressionNode(inExpression));
    }

    @Override
    public void enterLikeExpressionNode(SqlBasicCall likeExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.enterLikeExpressionNode(likeExpression));
    }

    @Override
    public void exitLikeExpressionNode(SqlBasicCall likeExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.exitLikeExpressionNode(likeExpression));
    }

    @Override
    public void enterAsExpressionNode(SqlBasicCall asExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.enterAsExpressionNode(asExpression));
    }

    @Override
    public void exitAsExpressionNode(SqlBasicCall asExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.exitAsExpressionNode(asExpression));
    }

    @Override
    public void enterBetweenNode(SqlBasicCall between) {
        listenerBases.forEach(listenerBase -> listenerBase.enterBetweenNode(between));
    }

    @Override
    public void exitBetweenNode(SqlBasicCall between) {
        listenerBases.forEach(listenerBase -> listenerBase.exitBetweenNode(between));
    }

    @Override
    public void enterPrefixExpressionNode(SqlBasicCall prefixExpression) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterPrefixExpressionNode(prefixExpression));

    }

    @Override
    public void exitPrefixExpressionNode(SqlBasicCall prefixExpression) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitPrefixExpressionNode(prefixExpression));
    }

    @Override
    public void enterPostfixExpressionNode(SqlBasicCall postfixExpression) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterPostfixExpressionNode(postfixExpression));
    }

    @Override
    public void exitPostfixExpressionNode(SqlBasicCall postfixExpression) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitPostfixExpressionNode(postfixExpression));

    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {
        listenerBases.forEach(listenerBase -> listenerBase.enterFunctionNode(function));
    }

    @Override
    public void exitFunctionNode(SqlBasicCall function) {
        listenerBases.forEach(listenerBase -> listenerBase.exitFunctionNode(function));
    }

    @Override
    public void enterSelectExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSelectExpressionNode(call));
    }

    @Override
    public void exitSelectExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSelectExpressionNode(call));
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        listenerBases.forEach(listenerBase -> listenerBase.enterPlainSelectNode(select));
    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        listenerBases.forEach(listenerBase -> listenerBase.exitPlainSelectNode(select));
    }

    @Override
    public void enterSetExpressionNode(SqlBasicCall setOperationList) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterSetExpressionNode(setOperationList));
    }

    @Override
    public void exitSetExpressionNode(SqlBasicCall setOperationList) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSetExpressionNode(setOperationList));
    }

    @Override
    public void exitInExpressionNode(SqlBasicCall inExpression) {
        listenerBases.forEach(listenerBase -> listenerBase.exitInExpressionNode(inExpression));
    }

    @Override
    public void enterSetUnionExpressionNode(SqlBasicCall setOperationList) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterSetUnionExpressionNode(setOperationList));
    }

    @Override
    public void exitSetUnionExpressionNode(SqlBasicCall setOperationList) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitSetUnionExpressionNode(setOperationList));
    }

    @Override
    public void enterSetIntersectExpressionNode(SqlBasicCall setOperationList) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterSetIntersectExpressionNode(setOperationList));
    }

    @Override
    public void exitSetIntersectExpressionNode(SqlBasicCall setOperationList) {
        listenerBases.forEach(
                listenerBase -> listenerBase.exitSetIntersectExpressionNode(setOperationList));
    }

    @Override
    public void enterSetMinusExpressionNode(SqlBasicCall setOperationList) {
        listenerBases.forEach(
                listenerBase -> listenerBase.enterSetMinusExpressionNode(setOperationList));
    }

    @Override
    public void exitSetMinusExpressionNode(SqlBasicCall setOperationList) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitSetMinusExpressionNode(setOperationList));
    }

    @Override
    public void enterQueryNode(SqlCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterQueryNode(call));
    }

    @Override
    public void exitQueryNode(SqlCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitQueryNode(call));
    }

    @Override
    public void enterBinaryExpressionNode(SqlBasicCall expression) {
        listenerBases.forEach(listenerBase -> listenerBase.enterBinaryExpressionNode(expression));
    }

    @Override
    public void exitBinaryExpressionNode(SqlBasicCall expression) {
        listenerBases.forEach(listenerBase -> listenerBase.exitBinaryExpressionNode(expression));

    }

    @Override
    public void enterPlusExpressionNode(SqlBasicCall plus) {
        listenerBases.forEach(listenerBase -> listenerBase.enterPlusExpressionNode(plus));
    }

    @Override
    public void exitPlusExpressionNode(SqlBasicCall plus) {
        listenerBases.forEach(listenerBase -> listenerBase.exitPlusExpressionNode(plus));
    }

    @Override
    public void enterMinusExpressionNode(SqlBasicCall minus) {
        listenerBases.forEach(listenerBase -> listenerBase.enterMinusExpressionNode(minus));
    }

    @Override
    public void exitMinusExpressionNode(SqlBasicCall minus) {
        listenerBases.forEach(listenerBase -> listenerBase.exitMinusExpressionNode(minus));
    }

    @Override
    public void enterTimesExpressionNode(SqlBasicCall times) {
        listenerBases.forEach(listenerBase -> listenerBase.enterTimesExpressionNode(times));
    }

    @Override
    public void exitTimesExpressionNode(SqlBasicCall times) {
        listenerBases.forEach(listenerBase -> listenerBase.exitTimesExpressionNode(times));
    }

    @Override
    public void enterDivideExpressionNode(SqlBasicCall divide) {
        listenerBases.forEach(listenerBase -> listenerBase.enterDivideExpressionNode(divide));
    }

    @Override
    public void exitDivideExpressionNode(SqlBasicCall divide) {
        listenerBases.forEach(listenerBase -> listenerBase.exitDivideExpressionNode(divide));
    }

    @Override
    public void enterModExpressionNode(SqlBasicCall mod) {
        listenerBases.forEach(listenerBase -> listenerBase.enterModExpressionNode(mod));
    }

    @Override
    public void exitModExpressionNode(SqlBasicCall mod) {
        listenerBases.forEach(listenerBase -> listenerBase.exitModExpressionNode(mod));
    }

    @Override
    public void enterAndExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterAndExpressionNode(call));
    }

    @Override
    public void exitAndExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitAndExpressionNode(call));
    }

    @Override
    public void enterOrExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterOrExpressionNode(call));
    }

    @Override
    public void exitOrExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitOrExpressionNode(call));
    }

    @Override
    public void enterNotExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterNotExpressionNode(call));
    }

    @Override
    public void exitNotExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitNotExpressionNode(call));
    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterEqualsExpressionNode(call));
    }

    @Override
    public void exitEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitEqualsExpressionNode(call));
    }

    @Override
    public void enterNotEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterNotEqualsExpressionNode(call));
    }

    @Override
    public void exitNotEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitNotEqualsExpressionNode(call));
    }

    @Override
    public void enterGreaterThanExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterGreaterThanExpressionNode(call));
    }

    @Override
    public void exitGreaterThanExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitGreaterThanExpressionNode(call));
    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall call) {
        listenerBases
                .forEach(listenerBase -> listenerBase.enterGreaterThanEqualsExpNode(call));
    }

    @Override
    public void exitGreaterThanEqualsExpNode(SqlBasicCall call) {
        listenerBases
                .forEach(listenerBase -> listenerBase.exitGreaterThanEqualsExpNode(call));
    }

    @Override
    public void enterLessThanExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterLessThanExpressionNode(call));
    }

    @Override
    public void exitLessThanExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitLessThanExpressionNode(call));
    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterLessThanEqualsExpressionNode(call));
    }

    @Override
    public void exitLessThanEqualsExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitLessThanEqualsExpressionNode(call));
    }

    @Override
    public void enterSubSelectNode(SqlSelect call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSubSelectNode(call));
    }

    @Override
    public void exitSubSelectNode(SqlSelect call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSubSelectNode(call));
    }

    @Override
    public void enterOverExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterOverExpressionNode(call));
    }

    @Override
    public void existOverExpressionNode(SqlBasicCall call) {
        listenerBases.forEach(listenerBase -> listenerBase.existOverExpressionNode(call));
    }

    @Override
    public void enterWithNode(SqlWith call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterWithNode(call));
    }

    @Override
    public void exitWithNode(SqlWith call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitWithNode(call));
    }

    @Override
    public void enterWithItemNode(SqlWithItem call) {
        listenerBases.forEach(listenerBase -> listenerBase.enterWithItemNode(call));
    }

    @Override
    public void exitWithItemNode(SqlWithItem call) {
        listenerBases.forEach(listenerBase -> listenerBase.exitWithItemNode(call));
    }

    @Override
    public void enterSelectNodeListNode(SqlNodeList nodeList) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSelectNodeListNode(nodeList));
    }

    @Override
    public void exitSelectNodeListNode(SqlNodeList nodeList) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSelectNodeListNode(nodeList));
    }

    @Override
    public void enterSelectColumnNode(SqlIdentifier column) {
        listenerBases.forEach(listenerBase -> listenerBase.enterSelectColumnNode(column));
    }

    @Override
    public void exitSelectColumnNode(SqlIdentifier column) {
        listenerBases.forEach(listenerBase -> listenerBase.exitSelectColumnNode(column));
    }

    @Override
    public void enterCreateTableNode(org.apache.calcite.sql.SqlCreateTable createTable) {
        listenerBases.forEach(listenerBase -> listenerBase.enterCreateTableNode(createTable));
    }

    @Override
    public void exitCreateTableNode(org.apache.calcite.sql.SqlCreateTable createTable) {
        listenerBases.forEach(listenerBase -> listenerBase.exitCreateTableNode(createTable));
    }
}