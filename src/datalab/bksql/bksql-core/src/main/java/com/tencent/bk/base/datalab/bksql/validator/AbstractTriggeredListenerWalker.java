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

import java.util.Set;
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

public abstract class AbstractTriggeredListenerWalker extends BaseListenerWalker {

    private Set<Class<?>> defaultNodeTypes;

    protected AbstractTriggeredListenerWalker(Set<Class<?>> defaultNodeTypes) {
        this.defaultNodeTypes = defaultNodeTypes;
    }

    protected abstract void enterObject0(Object node);

    protected abstract void exitObject0(Object node);

    private void enterObject(Object node) {
        if (defaultNodeTypes.contains(node.getClass())) {
            enterObject0(node);
        }
    }

    private void exitObject(Object node) {
        if (defaultNodeTypes.contains(node.getClass())) {
            exitObject0(node);
        }

    }

    @Override
    public void enterDateLiteralNode(SqlDateLiteral dateValue) {
        enterObject(dateValue);
    }

    @Override
    public void exitDateLiteralNode(SqlDateLiteral dateValue) {
        exitObject(dateValue);
    }

    @Override
    public void enterTimeLiteralNode(SqlTimeLiteral timeValue) {
        enterObject(timeValue);
    }

    @Override
    public void exitTimeLiteralNode(SqlTimeLiteral timeValue) {
        exitObject(timeValue);
    }

    @Override
    public void enterTimestampLiteralNode(SqlTimestampLiteral timeStampValue) {
        enterObject(timeStampValue);
    }

    @Override
    public void exitTimestampLiteralNode(SqlTimestampLiteral timestampValue) {
        exitObject(timestampValue);
    }

    @Override
    public void enterBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {
        enterObject(binaryStringValue);
    }

    @Override
    public void exitBinaryStringLiteralNode(SqlBinaryStringLiteral binaryStringValue) {
        exitObject(binaryStringValue);
    }

    @Override
    public void enterCharStringLiteralNode(SqlCharStringLiteral stringValue) {
        enterObject(stringValue);
    }

    @Override
    public void exitCharStringLiteralNode(SqlCharStringLiteral stringValue) {
        exitObject(stringValue);
    }

    @Override
    public void enterIntervalLiteralNode(SqlIntervalLiteral intervalValue) {
        enterObject(intervalValue);
    }

    @Override
    public void exitIntervalLiteralNode(SqlIntervalLiteral intervalValue) {
        exitObject(intervalValue);
    }

    @Override
    public void enterIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {
        enterObject(intervalQualifier);
    }

    @Override
    public void exitIntervalQualifierNode(SqlIntervalQualifier intervalQualifier) {
        exitObject(intervalQualifier);
    }

    @Override
    public void enterNumericLiteralNode(SqlNumericLiteral numericValue) {
        enterObject(numericValue);
    }

    @Override
    public void exitNumericLiteralNode(SqlNumericLiteral numericValue) {
        exitObject(numericValue);
    }

    @Override
    public void enterIdentifierNode(SqlIdentifier identifier) {
        enterObject(identifier);
    }

    @Override
    public void exitIdentifierNode(SqlIdentifier identifier) {
        exitObject(identifier);
    }

    @Override
    public void enterSetOptionNode(SqlSetOption setOption) {
        enterObject(setOption);
    }

    @Override
    public void exitSetOptionNode(SqlSetOption setOption) {
        exitObject(setOption);
    }

    @Override
    public void enterCallNode(SqlCall call) {
        enterObject(call);
    }

    @Override
    public void exitCallNode(SqlCall call) {
        exitObject(call);
    }

    @Override
    public void enterBasicCallNode(SqlBasicCall basicCall) {
        enterObject(basicCall);
    }

    @Override
    public void exitBasicCallNode(SqlBasicCall basicCall) {
        exitObject(basicCall);
    }

    @Override
    public void enterDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {
        enterObject(dataTypeSpec);
    }

    @Override
    public void exitDataTypeSpecNode(SqlDataTypeSpec dataTypeSpec) {
        exitObject(dataTypeSpec);
    }

    @Override
    public void enterDeleteNode(SqlDelete delete) {
        enterObject(delete);
    }

    @Override
    public void exitDeleteNode(SqlDelete delete) {
        exitObject(delete);
    }

    @Override
    public void enterDescribeSchemaNode(SqlDescribeSchema describeSchema) {
        enterObject(describeSchema);
    }

    @Override
    public void exitDescribeSchemaNode(SqlDescribeSchema describeSchema) {
        exitObject(describeSchema);
    }

    @Override
    public void enterDescribeTableNode(SqlDescribeTable describeTable) {
        enterObject(describeTable);
    }

    @Override
    public void exitDescribeTableNode(SqlDescribeTable describeTable) {
        exitObject(describeTable);
    }

    @Override
    public void enterDropSchemaNode(SqlDropSchema dropSchema) {
        enterObject(dropSchema);
    }

    @Override
    public void exitDropSchemaNode(SqlDropSchema dropSchema) {
        exitObject(dropSchema);
    }

    @Override
    public void enterDropFunctionNode(SqlDropFunction dropFunction) {
        enterObject(dropFunction);
    }

    @Override
    public void exitDropFunctionNode(SqlDropFunction dropFunction) {
        exitObject(dropFunction);
    }

    @Override
    public void enterDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {
        enterObject(dropMaterializedView);
    }

    @Override
    public void exitDropMaterializedViewNode(SqlDropMaterializedView dropMaterializedView) {
        exitObject(dropMaterializedView);
    }

    @Override
    public void enterDropTableNode(SqlDropTable dropTable) {
        enterObject(dropTable);
    }

    @Override
    public void exitDropTableNode(SqlDropTable dropTable) {
        exitObject(dropTable);
    }

    @Override
    public void enterDropTypeNode(SqlDropType dropType) {
        enterObject(dropType);
    }

    @Override
    public void exitDropTypeNode(SqlDropType dropType) {
        exitObject(dropType);
    }

    @Override
    public void enterDropViewNode(SqlDropView dropView) {
        enterObject(dropView);
    }

    @Override
    public void exitDropViewNode(SqlDropView dropView) {
        exitObject(dropView);
    }

    @Override
    public void enterDynamicParamNode(SqlDynamicParam dynamicParam) {
        enterObject(dynamicParam);
    }

    @Override
    public void exitDynamicParamNode(SqlDynamicParam dynamicParam) {
        exitObject(dynamicParam);
    }

    @Override
    public void enterExplainNode(SqlExplain explain) {
        enterObject(explain);
    }

    @Override
    public void exitExplainNode(SqlExplain explain) {
        exitObject(explain);
    }

    @Override
    public void enterInsertNode(SqlInsert insert) {
        enterObject(insert);
    }

    @Override
    public void exitInsertNode(SqlInsert insert) {
        enterObject(insert);
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        enterObject(join);
    }

    @Override
    public void exitJoinNode(SqlJoin join) {
        exitObject(join);
    }

    @Override
    public void enterMatchRecognizeNode(SqlMatchRecognize matchRecognize) {
        enterObject(matchRecognize);
    }

    @Override
    public void exitMatchRecognizeNode(SqlMatchRecognize matchRecognize) {
        exitObject(matchRecognize);
    }

    @Override
    public void enterMergeNode(SqlMerge merge) {
        enterObject(merge);
    }

    @Override
    public void exitMergeNode(SqlMerge merge) {
        enterObject(merge);
    }

    @Override
    public void enterNodeListNode(SqlNodeList sqlNodes) {
        enterObject(sqlNodes);
    }

    @Override
    public void exitNodeListNode(SqlNodeList sqlNodes) {
        exitObject(sqlNodes);
    }

    @Override
    public void enterOrderByNode(SqlOrderBy orderBy) {
        enterObject(orderBy);
    }

    @Override
    public void exitOrderByNode(SqlOrderBy orderBy) {
        exitObject(orderBy);
    }

    @Override
    public void enterSelectNode(SqlSelect select) {
        enterObject(select);
    }

    @Override
    public void exitSelectNode(SqlSelect select) {
        exitObject(select);
    }

    @Override
    public void enterSnapshotNode(SqlSnapshot snapshot) {
        enterObject(snapshot);
    }

    @Override
    public void exitSnapshotNode(SqlSnapshot snapshot) {
        exitObject(snapshot);
    }

    @Override
    public void enterUpdateNode(SqlUpdate update) {
        enterObject(update);
    }

    @Override
    public void exitUpdateNode(SqlUpdate update) {
        exitObject(update);
    }

    @Override
    public void enterCaseNode(SqlCase sqlCase) {
        enterObject(sqlCase);
    }

    @Override
    public void exitCaseNode(SqlCase sqlCase) {
        exitObject(sqlCase);
    }

    @Override
    public void enterCreateViewNode(SqlCreateView createView) {
        enterObject(createView);
    }

    @Override
    public void exitCreateViewNode(SqlCreateView createView) {
        exitObject(createView);
    }

    @Override
    public void enterCreateTypeNode(SqlCreateType createType) {
        enterObject(createType);
    }

    @Override
    public void exitCreateTypeNode(SqlCreateType createType) {
        exitObject(createType);
    }

    @Override
    public void enterCreateSchemaNode(SqlCreateSchema createSchema) {
        enterObject(createSchema);
    }

    @Override
    public void exitCreateSchemaNode(SqlCreateSchema createSchema) {
        exitObject(createSchema);
    }

    @Override
    public void enterCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {
        enterObject(createMaterializedView);
    }

    @Override
    public void exitCreateMaterializedViewNode(SqlCreateMaterializedView createMaterializedView) {
        exitObject(createMaterializedView);
    }

    @Override
    public void enterAllColumnsNode(SqlIdentifier allColumns) {
        enterObject(allColumns);
    }

    @Override
    public void exitAllColumnsNode(SqlIdentifier allColumns) {
        exitObject(allColumns);
    }

    @Override
    public void enterAllTableColumnsNode(SqlIdentifier allTableColumns) {
        enterObject(allTableColumns);
    }

    @Override
    public void exitAllTableColumnsNode(SqlIdentifier allTableColumns) {
        exitObject(allTableColumns);
    }

    @Override
    public void enterInExpressionNode(SqlBasicCall inExpression) {
        enterObject(inExpression);
    }

    @Override
    public void exitInExpressionNode(SqlBasicCall inExpression) {
        exitObject(inExpression);
    }

    @Override
    public void enterLikeExpressionNode(SqlBasicCall likeExpression) {
        enterObject(likeExpression);
    }

    @Override
    public void exitLikeExpressionNode(SqlBasicCall likeExpression) {
        exitObject(likeExpression);
    }

    @Override
    public void enterAsExpressionNode(SqlBasicCall asExpression) {
        enterObject(asExpression);
    }

    @Override
    public void exitAsExpressionNode(SqlBasicCall asExpression) {
        exitObject(asExpression);
    }

    @Override
    public void enterBetweenNode(SqlBasicCall between) {
        enterObject(between);
    }

    @Override
    public void exitBetweenNode(SqlBasicCall between) {
        exitObject(between);
    }

    @Override
    public void enterPrefixExpressionNode(SqlBasicCall prefixExpression) {
        enterObject(prefixExpression);
    }

    @Override
    public void exitPrefixExpressionNode(SqlBasicCall prefixExpression) {
        exitObject(prefixExpression);
    }

    @Override
    public void enterPostfixExpressionNode(SqlBasicCall postfixExpression) {
        enterObject(postfixExpression);
    }

    @Override
    public void exitPostfixExpressionNode(SqlBasicCall postfixExpression) {
        exitObject(postfixExpression);
    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {
        enterObject(function);
    }

    @Override
    public void exitFunctionNode(SqlBasicCall function) {
        exitObject(function);
    }

    @Override
    public void enterSelectExpressionNode(SqlBasicCall call) {
        enterObject(call);
    }

    @Override
    public void exitSelectExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterSetExpressionNode(SqlBasicCall setOperationList) {
        enterObject(setOperationList);
    }

    @Override
    public void exitSetExpressionNode(SqlBasicCall setOperationList) {
        exitObject(setOperationList);
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        enterObject(select);
    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        exitObject(select);
    }


    @Override
    public void enterCreateFunctionNode(SqlCreateFunction createFunction) {
        enterObject(createFunction);
    }

    @Override
    public void exitCreateFunctionNode(SqlCreateFunction createFunction) {
        exitObject(createFunction);
    }

    @Override
    public void enterCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {
        enterObject(createForeignSchema);
    }

    @Override
    public void exitCreateForeignSchemaNode(SqlCreateForeignSchema createForeignSchema) {
        exitObject(createForeignSchema);
    }

    @Override
    public void enterColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {
        enterObject(columnDeclaration);
    }

    @Override
    public void exitColumnDeclarationNode(SqlColumnDeclaration columnDeclaration) {
        exitObject(columnDeclaration);
    }

    @Override
    public void enterCheckConstraintNode(SqlCheckConstraint checkConstraint) {
        enterObject(checkConstraint);
    }

    @Override
    public void exitCheckConstraint(SqlCheckConstraint checkConstraint) {
        exitObject(checkConstraint);
    }

    @Override
    public void enterAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {
        enterObject(attributeDefinition);
    }

    @Override
    public void exitAttributeDefinitionNode(SqlAttributeDefinition attributeDefinition) {
        exitObject(attributeDefinition);
    }

    @Override
    public void enterTableNameNode(SqlIdentifier tableName) {
        enterObject(tableName);
    }

    @Override
    public void existTableNameNode(SqlIdentifier tableName) {
        exitObject(tableName);
    }

    @Override
    public void enterColumnNode(SqlIdentifier column) {
        enterObject(column);
    }

    @Override
    public void exitColumnNode(SqlIdentifier column) {
        exitObject(column);
    }

    @Override
    public void enterSetUnionExpressionNode(SqlBasicCall setOperationList) {
        enterObject(setOperationList);
    }

    @Override
    public void exitSetUnionExpressionNode(SqlBasicCall setOperationList) {
        exitObject(setOperationList);
    }

    @Override
    public void enterSetIntersectExpressionNode(SqlBasicCall setOperationList) {
        enterObject(setOperationList);
    }

    @Override
    public void exitSetIntersectExpressionNode(SqlBasicCall setOperationList) {
        exitObject(setOperationList);
    }

    @Override
    public void enterSetMinusExpressionNode(SqlBasicCall setOperationList) {
        enterObject(setOperationList);
    }

    @Override
    public void exitSetMinusExpressionNode(SqlBasicCall setOperationList) {
        exitObject(setOperationList);
    }

    @Override
    public void enterQueryNode(SqlCall call) {
        enterObject(call);
    }

    @Override
    public void exitQueryNode(SqlCall call) {
        exitObject(call);
    }

    @Override
    public void enterBinaryExpressionNode(SqlBasicCall expression) {
        enterObject(expression);
    }

    @Override
    public void exitBinaryExpressionNode(SqlBasicCall expression) {
        exitObject(expression);
    }

    @Override
    public void enterPlusExpressionNode(SqlBasicCall plus) {
        enterObject(plus);
    }

    @Override
    public void exitPlusExpressionNode(SqlBasicCall plus) {
        exitObject(plus);
    }

    @Override
    public void enterMinusExpressionNode(SqlBasicCall minus) {
        enterObject(minus);
    }

    @Override
    public void exitMinusExpressionNode(SqlBasicCall minus) {
        exitObject(minus);
    }

    @Override
    public void enterTimesExpressionNode(SqlBasicCall times) {
        enterObject(times);
    }

    @Override
    public void exitTimesExpressionNode(SqlBasicCall times) {
        exitObject(times);
    }

    @Override
    public void enterDivideExpressionNode(SqlBasicCall divide) {
        enterObject(divide);
    }

    @Override
    public void exitDivideExpressionNode(SqlBasicCall divide) {
        exitObject(divide);
    }

    @Override
    public void enterModExpressionNode(SqlBasicCall mod) {
        enterObject(mod);
    }

    @Override
    public void exitModExpressionNode(SqlBasicCall mod) {
        exitObject(mod);
    }

    @Override
    public void enterAndExpressionNode(SqlBasicCall call) {
        enterObject(call);
    }

    @Override
    public void exitAndExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterOrExpressionNode(SqlBasicCall call) {
        enterObject(call);
    }

    @Override
    public void exitOrExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterNotExpressionNode(SqlBasicCall call) {
        enterObject(call);
    }

    @Override
    public void exitNotExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterNotEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitNotEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterGreaterThanExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitGreaterThanExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterGreaterThanEqualsExpNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitGreaterThanEqualsExpNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterLessThanExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitLessThanExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterLessThanEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void exitLessThanEqualsExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterSubSelectNode(SqlSelect call) {
        enterObject(call);
    }

    @Override
    public void exitSubSelectNode(SqlSelect call) {
        exitObject(call);
    }

    @Override
    public void enterOverExpressionNode(SqlBasicCall call) {
        enterObject(call);
    }

    @Override
    public void existOverExpressionNode(SqlBasicCall call) {
        exitObject(call);
    }

    @Override
    public void enterWithNode(SqlWith call) {
        enterObject(call);
    }

    @Override
    public void exitWithNode(SqlWith call) {
        exitObject(call);
    }

    @Override
    public void enterWithItemNode(SqlWithItem call) {
        enterObject(call);
    }

    @Override
    public void exitWithItemNode(SqlWithItem call) {
        exitObject(call);
    }

    @Override
    public void enterSelectNodeListNode(SqlNodeList node) {
        enterObject(node);
    }

    @Override
    public void exitSelectNodeListNode(SqlNodeList node) {
        exitObject(node);
    }

    @Override
    public void enterSelectColumnNode(SqlIdentifier node) {
        enterObject(node);
    }

    @Override
    public void exitSelectColumnNode(SqlIdentifier node) {
        exitObject(node);
    }

    @Override
    public void enterCreateTableNode(org.apache.calcite.sql.SqlCreateTable createTable) {
        enterObject(createTable);
    }

    @Override
    public void exitCreateTableNode(org.apache.calcite.sql.SqlCreateTable createTable) {
        exitObject(createTable);
    }
}
