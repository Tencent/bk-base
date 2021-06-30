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

public interface ListenerBase {

    /**
     * 进入SqlDateLiteral节点之后的方法调用
     *
     * @param node SqlDateLiteral节点
     */
    void enterDateLiteralNode(SqlDateLiteral node);

    /**
     * 退出 SqlDateLiteral 节点之前的方法调用
     *
     * @param node SqlDateLiteral 节点
     */
    void exitDateLiteralNode(SqlDateLiteral node);

    /**
     * 进入 SqlTimeLiteral 节点之后的方法调用
     *
     * @param node SqlTimeLiteral 节点
     */
    void enterTimeLiteralNode(SqlTimeLiteral node);

    /**
     * 退出 SqlTimeLiteral 节点之前的方法调用
     *
     * @param node SqlTimeLiteral 节点
     */
    void exitTimeLiteralNode(SqlTimeLiteral node);

    /**
     * 进入 SqlTimestampLiteral 节点之后的方法调用
     *
     * @param node SqlTimestampLiteral 节点
     */
    void enterTimestampLiteralNode(SqlTimestampLiteral node);

    /**
     * 退出 SqlTimestampLiteral 节点之前的方法调用
     *
     * @param node SqlTimestampLiteral 节点
     */
    void exitTimestampLiteralNode(SqlTimestampLiteral node);

    /**
     * 进入 SqlBinaryStringLiteral 节点之后的方法调用
     *
     * @param node SqlBinaryStringLiteral 节点
     */
    void enterBinaryStringLiteralNode(SqlBinaryStringLiteral node);

    /**
     * 退出 SqlBinaryStringLiteral 节点之前的方法调用
     *
     * @param node SqlBinaryStringLiteral
     */
    void exitBinaryStringLiteralNode(SqlBinaryStringLiteral node);

    /**
     * 进入 SqlCharStringLiteral 节点之后的方法调用
     *
     * @param node SqlCharStringLiteral 节点
     */
    void enterCharStringLiteralNode(SqlCharStringLiteral node);

    /**
     * 退出 SqlCharStringLiteral 节点之前的方法调用
     *
     * @param node SqlCharStringLiteral 节点
     */
    void exitCharStringLiteralNode(SqlCharStringLiteral node);

    /**
     * 进入 SqlIntervalLiteral 节点之后的方法调用
     *
     * @param node SqlIntervalLiteral 节点
     */
    void enterIntervalLiteralNode(SqlIntervalLiteral node);

    /**
     * 退出 SqlIntervalLiteral 节点之前的方法调用
     *
     * @param node SqlIntervalLiteral 节点
     */
    void exitIntervalLiteralNode(SqlIntervalLiteral node);

    /**
     * 进入 SqlIntervalQualifier 节点之后的方法调用
     *
     * @param node SqlIntervalQualifier 节点
     */
    void enterIntervalQualifierNode(SqlIntervalQualifier node);

    /**
     * 退出 SqlIntervalQualifier 节点之前的方法调用
     *
     * @param node SqlIntervalQualifier 节点
     */
    void exitIntervalQualifierNode(SqlIntervalQualifier node);

    /**
     * 进入 SqlNumericLiteral 节点之后的方法调用
     *
     * @param node SqlNumericLiteral 节点
     */
    void enterNumericLiteralNode(SqlNumericLiteral node);

    /**
     * 退出 SqlNumericLiteral 节点之前的方法调用
     *
     * @param node SqlNumericLiteral 节点
     */
    void exitNumericLiteralNode(SqlNumericLiteral node);

    /**
     * 进入 SqlIdentifier 节点之后的方法调用
     *
     * @param node SqlIdentifier 节点
     */
    void enterIdentifierNode(SqlIdentifier node);

    /**
     * 退出 SqlIdentifier 节点之前的方法调用
     *
     * @param node SqlIdentifier 节点
     */
    void exitIdentifierNode(SqlIdentifier node);

    /**
     * 进入 SqlSetOption 节点之后的方法调用
     *
     * @param node SqlSetOption 节点
     */
    void enterSetOptionNode(SqlSetOption node);

    /**
     * 退出 SqlSetOption 节点之前的方法调用
     *
     * @param node SqlSetOption 节点
     */
    void exitSetOptionNode(SqlSetOption node);

    /**
     * 进入 SqlCall 节点之后的方法调用
     *
     * @param node SqlCall 节点
     */
    void enterCallNode(SqlCall node);

    /**
     * 退出 SqlCall 节点之前的方法调用
     *
     * @param node SqlCall 节点
     */
    void exitCallNode(SqlCall node);

    /**
     * 进入 SqlBasicCall 节点之后的方法调用
     *
     * @param node SqlBasicCall 节点
     */
    void enterBasicCallNode(SqlBasicCall node);

    /**
     * 退出 SqlBasicCall 节点之前的方法调用
     *
     * @param node SqlBasicCall 节点
     */
    void exitBasicCallNode(SqlBasicCall node);

    /**
     * 进入 SqlDataTypeSpec 节点之后的方法调用
     *
     * @param node SqlDataTypeSpec 节点
     */
    void enterDataTypeSpecNode(SqlDataTypeSpec node);

    /**
     * 退出 SqlDateLiteral 节点之前的方法调用
     *
     * @param node SqlDateLiteral 节点
     */
    void exitDataTypeSpecNode(SqlDataTypeSpec node);

    /**
     * 进入 SqlDelete 节点之后的方法调用
     *
     * @param node SqlDelete 节点
     */
    void enterDeleteNode(SqlDelete node);

    /**
     * 退出 SqlDelete 节点之前的方法调用
     *
     * @param node SqlDelete 节点
     */
    void exitDeleteNode(SqlDelete node);

    /**
     * 进入 SqlDescribeSchema 节点之后的方法调用
     *
     * @param node SqlDescribeSchema 节点
     */
    void enterDescribeSchemaNode(SqlDescribeSchema node);

    /**
     * 退出 SqlDescribeSchema 节点之前的方法调用
     *
     * @param node SqlDescribeSchema 节点
     */
    void exitDescribeSchemaNode(SqlDescribeSchema node);

    /**
     * 进入 SqlDescribeTable 节点之前的方法调用
     *
     * @param node SqlDescribeTable 节点
     */
    void enterDescribeTableNode(SqlDescribeTable node);

    /**
     * 退出 SqlDescribeTable 节点之前的方法调用
     *
     * @param node SqlDescribeTable 节点
     */
    void exitDescribeTableNode(SqlDescribeTable node);

    /**
     * 进入 SqlDropSchema 节点之后的方法调用
     *
     * @param node SqlDropSchema 节点
     */
    void enterDropSchemaNode(SqlDropSchema node);

    /**
     * 退出 SqlDropSchema 节点之前的方法调用
     *
     * @param node SqlDropSchema 节点
     */
    void exitDropSchemaNode(SqlDropSchema node);

    /**
     * 进入 SqlDropFunction 节点之后的方法调用
     *
     * @param node SqlDropFunction 节点
     */
    void enterDropFunctionNode(SqlDropFunction node);

    /**
     * 退出 SqlDropFunction 节点之前的方法调用
     *
     * @param node SqlDropFunction 节点
     */
    void exitDropFunctionNode(SqlDropFunction node);

    /**
     * 进入 SqlDropMaterializedView 节点之后的方法调用
     *
     * @param node SqlDropMaterializedView 节点
     */
    void enterDropMaterializedViewNode(SqlDropMaterializedView node);

    /**
     * 退出 SqlDropMaterializedView 节点之前的方法调用
     *
     * @param node SqlDropMaterializedView 节点
     */
    void exitDropMaterializedViewNode(SqlDropMaterializedView node);

    /**
     * 进入 SqlDropTable 节点之后的方法调用
     *
     * @param node SqlDropTable 节点
     */
    void enterDropTableNode(SqlDropTable node);

    /**
     * 退出 SqlDropTable 节点之前的方法调用
     *
     * @param node SqlDropTable 节点
     */
    void exitDropTableNode(SqlDropTable node);

    /**
     * 进入 SqlDropType 节点之后的方法调用
     *
     * @param node SqlDropType 节点
     */
    void enterDropTypeNode(SqlDropType node);

    /**
     * 退出 SqlDropType 节点之前的方法调用
     *
     * @param node SqlDropType 节点
     */
    void exitDropTypeNode(SqlDropType node);

    /**
     * 进入 SqlDropView 节点之后的方法调用
     *
     * @param node SqlDropView 节点
     */
    void enterDropViewNode(SqlDropView node);

    /**
     * 退出 SqlDropView 节点之前的方法调用
     *
     * @param node SqlDropView 节点
     */
    void exitDropViewNode(SqlDropView node);

    /**
     * 进入 SqlDynamicParam 节点之后的方法调用
     *
     * @param node SqlDynamicParam 节点
     */
    void enterDynamicParamNode(SqlDynamicParam node);

    /**
     * 退出 SqlDynamicParam 节点之前的方法调用
     *
     * @param node SqlDynamicParam 节点
     */
    void exitDynamicParamNode(SqlDynamicParam node);

    /**
     * 进入 SqlExplain 节点之后的方法调用
     *
     * @param node SqlExplain 节点
     */
    void enterExplainNode(SqlExplain node);

    /**
     * 退出 SqlExplain 节点之前的方法调用
     *
     * @param node SqlExplain 节点
     */
    void exitExplainNode(SqlExplain node);

    /**
     * 进入 SqlInsert 节点之后的方法调用
     *
     * @param node SqlInsert 节点
     */
    void enterInsertNode(SqlInsert node);

    /**
     * 退出 SqlInsert 节点之前的方法调用
     *
     * @param node SqlInsert 节点
     */
    void exitInsertNode(SqlInsert node);

    /**
     * 进入 SqlJoin 节点之后的方法调用
     *
     * @param node SqlJoin 节点
     */
    void enterJoinNode(SqlJoin node);

    /**
     * 退出 SqlJoin 节点之前的方法调用
     *
     * @param node SqlJoin 节点
     */
    void exitJoinNode(SqlJoin node);

    /**
     * 进入 SqlMatchRecognize 节点之后的方法调用
     *
     * @param node SqlMatchRecognize 节点
     */
    void enterMatchRecognizeNode(SqlMatchRecognize node);

    /**
     * 退出 SqlMatchRecognize 节点之前的方法调用
     *
     * @param node SqlMatchRecognize 节点
     */
    void exitMatchRecognizeNode(SqlMatchRecognize node);

    /**
     * 进入 SqlMerge 节点之后的方法调用
     *
     * @param node SqlMerge 节点
     */
    void enterMergeNode(SqlMerge node);

    /**
     * 退出 SqlMerge 节点之前的方法调用
     *
     * @param node SqlMerge 节点
     */
    void exitMergeNode(SqlMerge node);

    /**
     * 进入 SqlNodeList 节点之后的方法调用
     *
     * @param node SqlNodeList 节点
     */
    void enterNodeListNode(SqlNodeList node);

    /**
     * 退出 SqlNodeList 节点之前的方法调用
     *
     * @param node SqlNodeList 节点
     */
    void exitNodeListNode(SqlNodeList node);

    /**
     * 进入 SqlOrderBy 节点之后的方法调用
     *
     * @param node SqlOrderBy 节点
     */
    void enterOrderByNode(SqlOrderBy node);

    /**
     * 退出 SqlOrderBy 节点之前的方法调用
     *
     * @param node SqlOrderBy 节点
     */
    void exitOrderByNode(SqlOrderBy node);

    /**
     * 进入 SqlSelect 节点之后的方法调用
     *
     * @param node SqlSelect 节点
     */
    void enterSelectNode(SqlSelect node);

    /**
     * 退出 SqlSelect 节点之前的方法调用
     *
     * @param node SqlSelect 节点
     */
    void exitSelectNode(SqlSelect node);

    /**
     * 进入 SqlSnapshot 节点之后的方法调用
     *
     * @param node SqlSnapshot 节点
     */
    void enterSnapshotNode(SqlSnapshot node);

    /**
     * 退出 SqlSnapshot 节点之前的方法调用
     *
     * @param node SqlSnapshot 节点
     */
    void exitSnapshotNode(SqlSnapshot node);

    /**
     * 进入 SqlUpdate 节点之后的方法调用
     *
     * @param node SqlUpdate 节点
     */
    void enterUpdateNode(SqlUpdate node);

    /**
     * 退出 SqlUpdate 节点之前的方法调用
     *
     * @param node SqlUpdate 节点
     */
    void exitUpdateNode(SqlUpdate node);

    /**
     * 进入 SqlCase 节点之后的方法调用
     *
     * @param node SqlCase 节点
     */
    void enterCaseNode(SqlCase node);

    /**
     * 退出 SqlCase 节点之前的方法调用
     *
     * @param node SqlCase 节点
     */
    void exitCaseNode(SqlCase node);

    /**
     * 进入 SqlCreateView 节点之后的方法调用
     *
     * @param node SqlCreateView 节点
     */
    void enterCreateViewNode(SqlCreateView node);

    /**
     * 退出 SqlCreateView 节点之前的方法调用
     *
     * @param node SqlCreateView 节点
     */
    void exitCreateViewNode(SqlCreateView node);

    /**
     * 进入 SqlCreateType 节点之后的方法调用
     *
     * @param node SqlCreateType 节点
     */
    void enterCreateTypeNode(SqlCreateType node);

    /**
     * 退出 SqlCreateType 节点之前的方法调用
     *
     * @param node SqlCreateType 节点
     */
    void exitCreateTypeNode(SqlCreateType node);

    /**
     * 进入 SqlCreateSchema 节点之后的方法调用
     *
     * @param node SqlCreateSchema 节点
     */
    void enterCreateSchemaNode(SqlCreateSchema node);

    /**
     * 退出 SqlCreateSchema 节点之前的方法调用
     *
     * @param node SqlCreateSchema 节点
     */
    void exitCreateSchemaNode(SqlCreateSchema node);

    /**
     * 进入 SqlCreateMaterializedView 节点之后的方法调用
     *
     * @param node SqlCreateMaterializedView 节点
     */
    void enterCreateMaterializedViewNode(SqlCreateMaterializedView node);

    /**
     * 退出 SqlCreateMaterializedView 节点之前的方法调用
     *
     * @param node SqlCreateMaterializedView 节点
     */
    void exitCreateMaterializedViewNode(SqlCreateMaterializedView node);

    /**
     * 进入 SqlCreateFunction 节点之后的方法调用
     *
     * @param node SqlCreateFunction 节点
     */
    void enterCreateFunctionNode(SqlCreateFunction node);

    /**
     * 退出 SqlCreateFunction 节点之前的方法调用
     *
     * @param node SqlCreateFunction 节点
     */
    void exitCreateFunctionNode(SqlCreateFunction node);

    /**
     * 进入 SqlCreateForeignSchema 节点之后的方法调用
     *
     * @param node SqlCreateForeignSchema 节点
     */
    void enterCreateForeignSchemaNode(SqlCreateForeignSchema node);

    /**
     * 退出 SqlCreateForeignSchema 节点之前的方法调用
     *
     * @param node SqlCreateForeignSchema 节点
     */
    void exitCreateForeignSchemaNode(SqlCreateForeignSchema node);

    /**
     * 进入 SqlColumnDeclaration 节点之后的方法调用
     *
     * @param node SqlColumnDeclaration 节点
     */
    void enterColumnDeclarationNode(SqlColumnDeclaration node);

    /**
     * 退出 SqlColumnDeclaration 节点之前的方法调用
     *
     * @param node SqlColumnDeclaration 节点
     */
    void exitColumnDeclarationNode(SqlColumnDeclaration node);

    /**
     * 进入 SqlCheckConstraint 节点之后的方法调用
     *
     * @param node SqlCheckConstraint 节点
     */
    void enterCheckConstraintNode(SqlCheckConstraint node);

    /**
     * 退出 SqlCheckConstraint 节点之前的方法调用
     *
     * @param node SqlCheckConstraint 节点
     */
    void exitCheckConstraint(SqlCheckConstraint node);

    /**
     * 进入 SqlAttributeDefinition 节点之后的方法调用
     *
     * @param node SqlAttributeDefinition 节点
     */
    void enterAttributeDefinitionNode(SqlAttributeDefinition node);

    /**
     * 退出 SqlAttributeDefinition 节点之前的方法调用
     *
     * @param node SqlAttributeDefinition 节点
     */
    void exitAttributeDefinitionNode(SqlAttributeDefinition node);

    /**
     * 进入 Table 节点之后的方法调用
     *
     * @param node Table节点
     */
    void enterTableNameNode(SqlIdentifier node);

    /**
     * 退出 Table 节点之前的方法调用
     *
     * @param node Table 节点
     */
    void existTableNameNode(SqlIdentifier node);

    /**
     * 进入 Column 节点之后的方法调用
     *
     * @param node Column 节点
     */
    void enterColumnNode(SqlIdentifier node);

    /**
     * 退出 Column 节点之前的方法调用
     *
     * @param node Column 节点
     */
    void exitColumnNode(SqlIdentifier node);

    /**
     * 进入 AllColumns(*) 之后的方法调用
     *
     * @param node AllColumns 节点
     */
    void enterAllColumnsNode(SqlIdentifier node);

    /**
     * 退出 AllColumns(*) 节点之前的方法调用
     *
     * @param node AllColumns 节点
     */
    void exitAllColumnsNode(SqlIdentifier node);

    /**
     * 进入 AllTableColumns(table.*) 节点之前的方法调用
     *
     * @param node AllTableColumns 节点
     */
    void enterAllTableColumnsNode(SqlIdentifier node);

    /**
     * 退出 AllTableColumns(table.*) 节点之前的方法调用
     *
     * @param node AllTableColumns 节点
     */
    void exitAllTableColumnsNode(SqlIdentifier node);

    /**
     * 进入In表达式节点之前的方法调用
     *
     * @param node In表达式节点
     */
    void enterInExpressionNode(SqlBasicCall node);

    /**
     * 退出In表达式节点之后的方法调用
     *
     * @param node In表达式节点
     */
    void exitInExpressionNode(SqlBasicCall node);

    /**
     * 进入Not In表达式节点之前的方法调用
     *
     * @param node In表达式节点
     */
    void enterNotInExpressionNode(SqlBasicCall node);

    /**
     * 退出Not In表达式节点之后的方法调用
     *
     * @param node In表达式节点
     */
    void exitNotInExpressionNode(SqlBasicCall node);

    /**
     * 进入 like表达式 节点之前的方法调用
     *
     * @param node like表达式 节点
     */
    void enterLikeExpressionNode(SqlBasicCall node);

    /**
     * 退出 like表达式 节点之前的方法调用
     *
     * @param node like表达式 节点
     */
    void exitLikeExpressionNode(SqlBasicCall node);

    /**
     * 进入 SqlBasicCall 节点之后的方法调用
     *
     * @param node SqlBasicCall 节点
     */
    void enterAsExpressionNode(SqlBasicCall node);

    /**
     * 退出 SqlBasicCall 节点之前的方法调用
     *
     * @param node SqlBasicCall 节点
     */
    void exitAsExpressionNode(SqlBasicCall node);

    /**
     * 进入 Between And 节点之后的方法调用
     *
     * @param node Between And 节点
     */
    void enterBetweenNode(SqlBasicCall node);

    /**
     * 退出 Between And 节点之前的方法调用
     *
     * @param node Between And 节点
     */
    void exitBetweenNode(SqlBasicCall node);

    /**
     * 进入 前缀表达式 节点之后的方法调用
     *
     * @param node 前缀表达式 节点
     */
    void enterPrefixExpressionNode(SqlBasicCall node);

    /**
     * 退出 前缀表达式 节点之前的方法调用
     *
     * @param node 前缀表达式 节点
     */
    void exitPrefixExpressionNode(SqlBasicCall node);

    /**
     * 进入 后缀表达式 节点之后的方法调用
     *
     * @param node 后缀表达式 节点
     */
    void enterPostfixExpressionNode(SqlBasicCall node);

    /**
     * 退出 后缀表达式 节点之前的方法调用
     *
     * @param node 后缀表达式 节点
     */
    void exitPostfixExpressionNode(SqlBasicCall node);

    /**
     * 进入 函数 节点之后的方法调用
     *
     * @param node 函数节点
     */
    void enterFunctionNode(SqlBasicCall node);

    /**
     * 退出 函数 节点之前的方法调用
     *
     * @param node 函数节点
     */
    void exitFunctionNode(SqlBasicCall node);

    /**
     * 进入 Select表达式 节点之后的方法调用
     *
     * @param node Select表达式 节点
     */
    void enterSelectExpressionNode(SqlBasicCall node);

    /**
     * 退出 Select表达式 节点之前的方法调用
     *
     * @param node Select表达式 节点
     */
    void exitSelectExpressionNode(SqlBasicCall node);

    /**
     * 进入 PlainSelect 节点之后的方法调用
     *
     * @param node PlainSelect 节点
     */
    void enterPlainSelectNode(SqlSelect node);

    /**
     * 退出 PlainSelect 节点之前的方法调用
     *
     * @param node PlainSelect 节点
     */
    void exitPlainSelectNode(SqlSelect node);

    /**
     * 进入 集合操作表达式 节点之后的方法调用
     *
     * @param node 集合操作表达式 节点
     */
    void enterSetExpressionNode(SqlBasicCall node);

    /**
     * 退出 集合操作表达式 节点之前的方法调用
     *
     * @param node 集合操作表达式 节点
     */
    void exitSetExpressionNode(SqlBasicCall node);

    /**
     * 进入 并集操作表达式 节点之后的方法调用
     *
     * @param node 并集操作表达式 节点
     */
    void enterSetUnionExpressionNode(SqlBasicCall node);

    /**
     * 退出 并集操作表达式 节点之前的方法调用
     *
     * @param node 并集操作表达式 节点
     */
    void exitSetUnionExpressionNode(SqlBasicCall node);

    /**
     * 进入 交集操作表达式 节点之前后的方法调用
     *
     * @param node 交集操作表达式 节点
     */
    void enterSetIntersectExpressionNode(SqlBasicCall node);

    /**
     * 退出 交集操作表达式 节点之前的方法调用
     *
     * @param node 交集操作表达式 节点
     */
    void exitSetIntersectExpressionNode(SqlBasicCall node);

    /**
     * 进入 差集操作表达式 节点之后的方法调用
     *
     * @param node 差集操作表达式 节点
     */
    void enterSetMinusExpressionNode(SqlBasicCall node);

    /**
     * 进入 差集操作表达式 节点之前的方法调用
     *
     * @param node 差集操作表达式 节点
     */
    void exitSetMinusExpressionNode(SqlBasicCall node);

    /**
     * 进入 顶层查询 节点之后的方法调用
     *
     * @param node 顶层查询 节点
     */
    void enterQueryNode(SqlCall node);

    /**
     * 退出 顶层查询 节点之前的方法调用
     *
     * @param node 顶层查询 节点
     */
    void exitQueryNode(SqlCall node);

    /**
     * 进入 二元表达式 节点之后的方法调用
     *
     * @param node 二元表达式 节点
     */
    void enterBinaryExpressionNode(SqlBasicCall node);

    /**
     * 退出 二元表达式 节点之前的方法调用
     *
     * @param node 二元表达式 节点
     */
    void exitBinaryExpressionNode(SqlBasicCall node);

    /**
     * 进入 加法表达式 节点之后的方法调用
     *
     * @param node 加法表达式 节点
     */
    void enterPlusExpressionNode(SqlBasicCall node);

    /**
     * 退出 加法表达式  节点之前的方法调用
     *
     * @param node 加法表达式 节点
     */
    void exitPlusExpressionNode(SqlBasicCall node);

    /**
     * 进入 减法表达式 节点之后的方法调用
     *
     * @param node 减法表达式 节点
     */
    void enterMinusExpressionNode(SqlBasicCall node);

    /**
     * 退出 减法表达式 节点之前的方法调用
     *
     * @param node 减法表达式 节点
     */
    void exitMinusExpressionNode(SqlBasicCall node);

    /**
     * 进入 乘法表达式 节点之前的方法调用
     *
     * @param node 乘法表达式 节点
     */
    void enterTimesExpressionNode(SqlBasicCall node);

    /**
     * 退出 乘法表达式 节点之前的方法调用
     *
     * @param node 乘法表达式 节点
     */
    void exitTimesExpressionNode(SqlBasicCall node);

    /**
     * 进入 除法表达式 节点之后的方法调用
     *
     * @param node 除法表达式 节点
     */
    void enterDivideExpressionNode(SqlBasicCall node);

    /**
     * 退出 除法表达式 节点之前的方法调用
     *
     * @param node 除法表达式 节点
     */
    void exitDivideExpressionNode(SqlBasicCall node);

    /**
     * 进入 取模表达式 节点之后的方法调用
     *
     * @param node 取模表达式 节点
     */
    void enterModExpressionNode(SqlBasicCall node);

    /**
     * 退出 取模表达式 节点之前的方法调用
     *
     * @param node 取模表达式 节点
     */
    void exitModExpressionNode(SqlBasicCall node);

    /**
     * 进入 逻辑与表达式 节点之后的方法调用
     *
     * @param node 逻辑与表达式 节点
     */
    void enterAndExpressionNode(SqlBasicCall node);

    /**
     * 退出 逻辑与表达式 节点之前的方法调用
     *
     * @param node 逻辑与表达式 节点
     */
    void exitAndExpressionNode(SqlBasicCall node);

    /**
     * 退出 逻辑或表达式 节点之前的方法调用
     *
     * @param node 逻辑或表达式 节点
     */
    void enterOrExpressionNode(SqlBasicCall node);

    /**
     * 退出 逻辑或表达式 节点之前的方法调用
     *
     * @param node 逻辑或表达式 节点
     */
    void exitOrExpressionNode(SqlBasicCall node);

    /**
     * 进入 逻辑非表达式 节点之后的方法调用
     *
     * @param node 逻辑非表达式 节点
     */
    void enterNotExpressionNode(SqlBasicCall node);

    /**
     * 退出 逻辑非表达式 节点之前的方法调用
     *
     * @param node 逻辑非表达式 节点
     */
    void exitNotExpressionNode(SqlBasicCall node);

    /**
     * 进入 等于(=)表达式 节点之后的方法调用
     *
     * @param node 等于(=)表达式 节点
     */
    void enterEqualsExpressionNode(SqlBasicCall node);

    /**
     * 退出 等于(=)表达式 节点之前的方法调用
     *
     * @param node 等于(=)表达式 节点
     */
    void exitEqualsExpressionNode(SqlBasicCall node);

    /**
     * 进入 不等于(!=)表达式 节点之后的方法调用
     *
     * @param node 不等于(!=)表达式 节点
     */
    void enterNotEqualsExpressionNode(SqlBasicCall node);

    /**
     * 退出 不等于(!=)表达式 节点之前的方法调用
     *
     * @param node 不等于(!=)表达式 节点
     */
    void exitNotEqualsExpressionNode(SqlBasicCall node);

    /**
     * 进入 大于(>)表达式 节点之后的方法调用
     *
     * @param node 大于(>)表达式 节点
     */
    void enterGreaterThanExpressionNode(SqlBasicCall node);

    /**
     * 退出 大于(>)表达式 节点之前的方法调用
     *
     * @param node 大于(>)表达式 节点
     */
    void exitGreaterThanExpressionNode(SqlBasicCall node);

    /**
     * 进入 大于等于(>=)表达式 节点之后的方法调用
     *
     * @param node 大于等于(>=)表达式 节点
     */
    void enterGreaterThanEqualsExpNode(SqlBasicCall node);

    /**
     * 退出 大于等于(>=)表达式 节点之前的方法调用
     *
     * @param node 大于等于(>=)表达式 节点
     */
    void exitGreaterThanEqualsExpNode(SqlBasicCall node);

    /**
     * 进入 小于(<)表达式 节点之后的方法调用
     *
     * @param node 小于(<)表达式 节点
     */
    void enterLessThanExpressionNode(SqlBasicCall node);

    /**
     * 退出 小于(<)表达式 节点之前的方法调用
     *
     * @param node 小于(<)表达式 节点
     */
    void exitLessThanExpressionNode(SqlBasicCall node);

    /**
     * 进入 小于等于(<=)表达式 节点之后的方法调用
     *
     * @param node 小于等于(<=)表达式 节点
     */
    void enterLessThanEqualsExpressionNode(SqlBasicCall node);

    /**
     * 退出 小于等于(<=) 节点之前的方法调用
     *
     * @param node 小于等于(<=)表达式 节点
     */
    void exitLessThanEqualsExpressionNode(SqlBasicCall node);

    /**
     * 进入 SubQuery(子查询) 节点之后的方法调用
     *
     * @param node SubQuery 节点
     */
    void enterSubSelectNode(SqlSelect node);

    /**
     * 退出 SubQuery(子查询) 节点之前的方法调用
     *
     * @param node SubQuery 节点
     */
    void exitSubSelectNode(SqlSelect node);

    /**
     * 进入 OverExpression(窗口表达式) 节点之后的方法调用
     *
     * @param node OverExpression 节点
     */
    void enterOverExpressionNode(SqlBasicCall node);

    /**
     * 退出 OverExpression(窗口表达式) 节点之前的方法调用
     *
     * @param node OverExpression 节点
     */
    void existOverExpressionNode(SqlBasicCall node);

    /**
     * 进入 SqlWith 节点之后的方法调用
     *
     * @param node SqlWith 节点
     */
    void enterWithNode(SqlWith node);

    /**
     * 退出 SqlWith 节点之前的方法调用
     *
     * @param node SqlWith 节点
     */
    void exitWithNode(SqlWith node);

    /**
     * 进入 SqlWithItem 节点之后的方法调用
     *
     * @param node SqlWithItem 节点
     */
    void enterWithItemNode(SqlWithItem node);

    /**
     * 退出 SqlWithItem 节点之前的方法调用
     *
     * @param node SqlWithItem 节点
     */
    void exitWithItemNode(SqlWithItem node);

    /**
     * 进入 SelectNodeList 节点之后的方法调用
     *
     * @param node SelectNodeList 节点
     */
    void enterSelectNodeListNode(SqlNodeList node);

    /**
     * 退出 SelectNodeList 节点之前的方法调用
     *
     * @param node SelectNodeList 节点
     */
    void exitSelectNodeListNode(SqlNodeList node);

    /**
     * 进入 SelectColumn 节点之后的方法调用
     *
     * @param node SelectColumn 节点
     */
    void enterSelectColumnNode(SqlIdentifier node);

    /**
     * 退出 SelectColumn 节点之前的方法调用
     *
     * @param node SelectColumn 节点
     */
    void exitSelectColumnNode(SqlIdentifier node);

    /**
     * 进入 SqlCreateTable 节点之后的方法调用
     *
     * @param node SqlCreateTable 节点
     */
    void enterCreateTableNode(SqlCreateTable node);

    /**
     * 退出 SqlCreateTable 节点之前的方法调用
     *
     * @param node SqlCreateTable 节点
     */
    void exitCreateTableNode(SqlCreateTable node);

    /**
     * 进入 SqlShow 节点之后的方法调用
     *
     * @param node SqlShow 节点
     */
    void enterShowNode(SqlShow node);

    /**
     * 退出 SqlShow 节点之前的方法调用
     *
     * @param node SqlShow 节点
     */
    void exitShowNode(SqlShow node);

    /**
     * 进入 SqlShowSql 节点之后的方法调用
     *
     * @param node SqlShowSql 节点
     */
    void enterShowSqlNode(SqlShowSql node);

    /**
     * 退出 SqlShowSql 节点之前的方法调用
     *
     * @param node SqlShowSql 节点
     */
    void exitShowSqlNode(SqlShowSql node);

    /**
     * 进入 GroupBy 节点之后的方法调用
     *
     * @param node GroupBy 节点
     */
    void enterGroupByNode(SqlNode node);

    /**
     * 退出 GroupBy 节点之前的方法调用
     *
     * @param node GroupBy 节点
     */
    void exitGroupByNode(SqlNode node);

    /**
     * 进入 SqlDropModels 节点之前的方法调用
     *
     * @param node SqlDropModels 节点
     */
    void enterDropModelsNode(SqlDropModels node);

    /**
     * 退出 SqlDropModels 节点之前的方法调用
     *
     * @param node SqlDropModels 节点
     */
    void existDropModelsNode(SqlDropModels node);
}
