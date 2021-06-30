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

package com.tencent.bk.base.datalab.bksql.util.calcite.visitor;

import org.apache.calcite.sql.MySqlIndexHint;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlAlterTableAddColumn;
import org.apache.calcite.sql.SqlAlterTableDropColumn;
import org.apache.calcite.sql.SqlAlterTableName;
import org.apache.calcite.sql.SqlAlterTableReNameColumn;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlTrainModel;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.ddl.SqlDropTable;

public interface SelectVisitor<R> extends DefaultSqlVisitor<R> {

    default R visit(SqlSelect plainSelect) {
        return null;
    }

    default R visit(SqlOrderBy orderBySelect) {
        return null;
    }

    default R visit(SqlWith sqlWith) {
        return null;
    }

    default R visit(SqlWithItem withItem) {
        return null;
    }

    default R visit(SqlSetOperator setOperator) {
        return null;
    }

    default R visit(MySqlIndexHint mySqlIndexHint) {
        return null;
    }

    default R visitPlainSelect(SqlSelect plainSelect) {
        return null;
    }

    default R visitOrderBySelect(SqlOrderBy orderBySelect) {
        return null;
    }

    default R visitWithItem(SqlWithItem withItem) {
        return null;
    }

    default R visitSetOperationList(SqlBasicCall setOpList) {
        return null;
    }

    default R visitSetExpression(SqlBasicCall setOperationList) {
        return null;
    }

    default R visitSetUnionExpression(SqlBasicCall setOperationList) {
        return null;
    }

    default R visitSetIntersectExpression(SqlBasicCall setOperationList) {
        return null;
    }

    default R visitSetMinusExpression(SqlBasicCall setOperationList) {
        return null;
    }

    default R visitSelectNodeList(SqlNodeList sqlNodes) {
        return null;
    }

    default R visitKeyWordList(SqlNodeList sqlNodes) {
        return null;
    }

    default R visitCreateTable(SqlCreateTable createTable) {
        return null;
    }

    default R visitDelete(SqlDelete sqlDelete) {
        return null;
    }

    default R visitUpdate(SqlUpdate sqlUpdate) {
        return null;
    }

    default R visitShow(SqlShow sqlShow) {
        return null;
    }

    default R visitShowSql(SqlShowSql sqlShowSql) {
        return null;
    }

    default R visitExplain(SqlExplain sqlExplain) {
        return null;
    }

    default R visitInsert(SqlInsert sqlInsert) {
        return null;
    }

    default R visitDropTable(SqlDropTable sqlDropTable) {
        return null;
    }

    default R visitDropModels(SqlDropModels sqlDropModels) {
        return null;
    }

    default R visitAlter(SqlAlter sqlAlter) {
        return null;
    }

    default R visitAlterTableName(SqlAlterTableName sqlAlterTableName) {
        return null;
    }

    default R visitAlterTableDropColumn(SqlAlterTableDropColumn sqlAlterTableDropColumn) {
        return null;
    }

    default R visitAlterTableReNameColumn(SqlAlterTableReNameColumn sqlAlterTableReNameColumn) {
        return null;
    }

    default R visitAlterTableAddColumn(SqlAlterTableAddColumn sqlAlterTableAddColumn) {
        return null;
    }

    default R visitGroupBy(SqlNode node) {
        return null;
    }

    default R visitCreateTableFromModel(SqlCreateTableFromModel createTableFromModel) {
        return null;
    }

    default R visitTrainModel(SqlTrainModel trainModel) {
        return null;
    }
}
