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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class TableNameRemoveSuffixOptimizer extends SimpleListenerWalker {


    @JsonCreator
    public TableNameRemoveSuffixOptimizer() {
        super();
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        SqlNode fromItem = select.getFrom();
        extractTableNameNode(fromItem);
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        analysisJoinNode(left);
        analysisJoinNode(right);
    }

    private void analysisJoinNode(SqlNode joinNode) {
        if (joinNode.getKind() == SqlKind.IDENTIFIER) {
            getTableNameWithoutStorage((SqlIdentifier) joinNode);
        } else if (joinNode.getKind() == SqlKind.AS) {
            SqlNode beforeAs = ((SqlBasicCall) joinNode).operand(0);
            if (beforeAs instanceof SqlIdentifier) {
                getTableNameWithoutStorage((SqlIdentifier) beforeAs);
            }
        }
    }

    private void getTableNameWithoutStorage(SqlIdentifier fromItem) {
        SqlIdentifier table = fromItem;
        if (table == null || table.names.size() == 0) {
            return;
        }
        String tableName = "";
        if (table.names.size() >= 2) {
            tableName = table.names.get(table.names.size() - 2);

        } else if (table.names.size() == 1) {
            tableName = table.toString();
        }
        table.setNames(Lists.newArrayList(tableName), Lists.newArrayList(SqlParserPos.ZERO));
    }

    private void extractTableNameNode(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            getTableNameWithoutStorage((SqlIdentifier) node);
        } else if (node instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) node;
            extractTableNameNode(sqlSelect.getFrom());
        } else if (node instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
            extractTableNameNode(sqlOrderBy.query);
        } else if (node instanceof SqlBasicCall) {
            SqlBasicCall fromCall = (SqlBasicCall) node;
            if (fromCall.getOperator() instanceof SqlAsOperator) {
                SqlNode beforeAs = fromCall.operand(0);
                if (beforeAs instanceof SqlIdentifier) {
                    getTableNameWithoutStorage((SqlIdentifier) beforeAs);
                } else {
                    extractTableNameNode(beforeAs);
                }
            }
        }
    }
}
