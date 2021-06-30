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

import static org.apache.calcite.sql.SqlKind.ALTER_TABLE;
import static org.apache.calcite.sql.SqlKind.CREATE_TABLE;
import static org.apache.calcite.sql.SqlKind.DELETE;
import static org.apache.calcite.sql.SqlKind.DROP_TABLE;
import static org.apache.calcite.sql.SqlKind.EXPLAIN;
import static org.apache.calcite.sql.SqlKind.INSERT;
import static org.apache.calcite.sql.SqlKind.INTERSECT;
import static org.apache.calcite.sql.SqlKind.MINUS;
import static org.apache.calcite.sql.SqlKind.ORDER_BY;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.apache.calcite.sql.SqlKind.TRAIN;
import static org.apache.calcite.sql.SqlKind.UNION;
import static org.apache.calcite.sql.SqlKind.UPDATE;
import static org.apache.calcite.sql.SqlKind.WITH;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.StatementVisitor;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;


public class StatementDeParser implements StatementVisitor<Void> {

    private static final List<SqlKind> SUPPORTED_SQL_KIND = Lists.newArrayList(WITH, ORDER_BY,
            UNION, INTERSECT, MINUS, SELECT, CREATE_TABLE, DROP_TABLE, ALTER_TABLE, DELETE, UPDATE,
            EXPLAIN, INSERT, TRAIN);
    private ExpressionDeParser expressionDeParser;
    private SelectDeParser selectDeParser;
    private StringBuilder buffer;

    public StatementDeParser(StringBuilder buffer) {
        this(new ExpressionDeParser(), new SelectDeParser(), buffer);
    }

    public StatementDeParser(ExpressionDeParser expressionDeParser, SelectDeParser selectDeParser,
            StringBuilder buffer) {
        this.expressionDeParser = expressionDeParser;
        this.selectDeParser = selectDeParser;
        this.buffer = buffer;
    }

    public ExpressionDeParser getExpressionDeParser() {
        return expressionDeParser;
    }

    public void setExpressionDeParser(ExpressionDeParser expressionDeParser) {
        this.expressionDeParser = expressionDeParser;
    }

    public SelectDeParser getSelectDeParser() {
        return selectDeParser;
    }

    public void setSelectDeParser(SelectDeParser selectDeParser) {
        this.selectDeParser = selectDeParser;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public Void visit(SqlLiteral literal) {
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        selectDeParser.setBuffer(buffer);
        selectDeParser.setStatementVisitor(this);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        expressionDeParser.setBuffer(buffer);
        expressionDeParser.setSelectVisitor(selectDeParser);
        SqlKind kind = call.getKind();
        if (!SUPPORTED_SQL_KIND.contains(kind)) {
            throw new RuntimeException(String.format("Not support SqlKind:%s", kind.name()));
        }
        call.accept(selectDeParser);
        return null;
    }

    @Override
    public Void visit(SqlNodeList statements) {
        return null;
    }

    @Override
    public Void visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec type) {
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
    public Void visit(SqlMerge merge) {
        return null;
    }

    @Override
    public Void visit(SqlSelect select) {
        return null;
    }

    @Override
    public Void visit(SqlCreateTable createTable) {
        return null;
    }
}
