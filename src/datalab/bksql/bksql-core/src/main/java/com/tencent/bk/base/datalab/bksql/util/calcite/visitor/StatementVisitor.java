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

import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlUpdate;
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

public interface StatementVisitor<R> extends DefaultSqlVisitor<R> {

    default R visit(SqlDelete delete) {
        return null;
    }

    default R visit(SqlUpdate update) {
        return null;
    }

    default R visit(SqlInsert insert) {
        return null;
    }

    default R visit(SqlDropFunction dropFunction) {
        return null;
    }

    default R visit(SqlDropMaterializedView dropMaterializedView) {
        return null;
    }

    default R visit(SqlDropSchema dropSchema) {
        return null;
    }

    default R visit(SqlDropTable dropTable) {
        return null;
    }

    default R visit(SqlDropType dropType) {
        return null;
    }

    default R visit(SqlDropView dropView) {
        return null;
    }

    default R visit(SqlCreateForeignSchema createForeignSchema) {
        return null;
    }

    default R visit(SqlCreateFunction createFunction) {
        return null;
    }

    default R visit(SqlCreateMaterializedView createMaterializedView) {
        return null;
    }

    default R visit(SqlCreateSchema createSchema) {
        return null;
    }

    default R visit(SqlCreateTable createTable) {
        return null;
    }

    default R visit(SqlCreateType createType) {
        return null;
    }

    default R visit(SqlCreateView createView) {
        return null;
    }

    default R visit(SqlSetOption setOption) {
        return null;
    }

    default R visit(SqlMerge merge) {
        return null;
    }

    default R visit(SqlSelect select) {
        return null;
    }
}
