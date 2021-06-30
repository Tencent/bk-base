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

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

public class TableNames extends SimpleListenerDeParser {

    private final Set<String> names = new HashSet<>();

    private boolean add(String name) {
        if (name == null) {
            throw new NullPointerException("null table name");
        }
        return names.add(name);
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        extractTableName(select.getFrom());
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        extractTableName(join.getLeft());
        extractTableName(join.getRight());
    }

    private void extractTableName(SqlNode sqlNode) {
        if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier tableNode = (SqlIdentifier) sqlNode;
            String tableName = getTableName(tableNode);
            add(tableName);
        } else if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            extractTableName(sqlSelect.getFrom());
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            extractTableName(sqlOrderBy.query);
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) sqlNode;
            if (call.getOperator() instanceof SqlAsOperator) {
                SqlNode beforeAs = call.getOperands()[0];
                if (beforeAs instanceof SqlIdentifier) {
                    SqlIdentifier tableNode = (SqlIdentifier) beforeAs;
                    String tableName = getTableName(tableNode);
                    add(tableName);
                } else {
                    extractTableName(beforeAs);
                }
            }
        }
    }

    private String getTableName(SqlIdentifier tableNode) {
        if (tableNode == null) {
            return "";
        }
        if (tableNode.names.size() >= 2) {
            return tableNode.names.get(tableNode.names.size() - 2);
        } else {
            return tableNode.toString();
        }
    }

    @Override
    protected Object getRetObj() {
        return names;
    }
}
