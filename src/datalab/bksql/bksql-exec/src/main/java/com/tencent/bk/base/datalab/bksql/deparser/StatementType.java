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

import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlUpdate;

public class StatementType extends SimpleListenerDeParser {

    private static final String DML_SELECT = "dml_select";
    private static final String DDL_CTAS = "ddl_ctas";
    private static final String DDL_CREATE = "ddl_create";
    private static final String DDL_DROP = "ddl_drop";
    private static final String DML_UPDATE = "dml_update";
    private static final String DML_INSERT_INTO = "dml_insert_into";
    private static final String DML_INSERT_OVERWRITE = "dml_insert_overwrite";
    private static final String DML_DELETE = "dml_delete";
    private static final String DML_EXPLAIN = "dml_explain";
    private static final String SHOW_TABLES = "show_tables";
    private static final String SHOW_MODELS = "show_models";
    private static final String SHOW_SQL = "show_sql";
    private String statementType = DML_SELECT;

    @Override
    public void enterCreateTableNode(SqlCreateTable createTable) {
        super.enterCreateTableNode(createTable);
        SqlNode query = createTable.operand(2);
        //这里判断是否是简单的建表语句，而不是CTAS
        if (query == null) {
            statementType = DDL_CREATE;
        } else {
            statementType = DDL_CTAS;
        }
    }

    @Override
    public void enterDropModelsNode(SqlDropModels node) {
        super.enterDropModelsNode(node);
        statementType = DDL_DROP;
    }

    @Override
    public void enterDeleteNode(SqlDelete delete) {
        super.enterDeleteNode(delete);
        statementType = DML_DELETE;
    }

    @Override
    public void enterUpdateNode(SqlUpdate update) {
        super.enterUpdateNode(update);
        statementType = DML_UPDATE;
    }

    @Override
    public void enterShowNode(SqlShow node) {
        super.enterShowNode(node);
        statementType = node.isShowModels() ? SHOW_MODELS : SHOW_TABLES;
    }

    @Override
    public void enterShowSqlNode(SqlShowSql node) {
        super.enterShowSqlNode(node);
        statementType = SHOW_SQL;
    }

    @Override
    public void enterInsertNode(SqlInsert insert) {
        super.enterInsertNode(insert);
        if (insert.isOverwrite()) {
            statementType = DML_INSERT_OVERWRITE;
        } else {
            statementType = DML_INSERT_INTO;
        }
    }

    @Override
    public void enterExplainNode(SqlExplain explain) {
        super.enterExplainNode(explain);
        statementType = DML_EXPLAIN;
    }

    @Override
    protected Object getRetObj() {
        return statementType;
    }
}
