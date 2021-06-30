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

package com.tencent.bk.base.dataflow.bksql.deparser.subdeparser;

import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.SelectDeParser;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTrainModel;

public class SubQuerySelectDeParser extends SelectDeParser {

    private boolean hasSubQuery;
    private String operateType;
    private String targetEntityName;
    private boolean subQueryHasJoin;

    public SubQuerySelectDeParser(boolean hasSubQuery, String operateType, String targetEntityName,
            boolean subQueryHasJoin) {
        this.hasSubQuery = hasSubQuery;
        this.operateType = operateType;
        this.targetEntityName = targetEntityName;
        this.subQueryHasJoin = subQueryHasJoin;
    }

    /**
     * get hasSubQuery
     */
    public boolean isHasSubQuery() {
        return hasSubQuery;
    }

    /**
     * get operateType
     */
    public String getOperateType() {
        return operateType;
    }

    /**
     * get targetEntityName
     */
    public String getTargetEntityName() {
        return targetEntityName;
    }

    /**
     * get subQueryHasJoin
     */
    public boolean isSubQueryHasJoin() {
        return subQueryHasJoin;
    }

    @Override
    public Void visitCreateTable(SqlCreateTable createTable) {
        SqlNode tableName = createTable.operand(0);
        SqlNode query = createTable.operand(2);
        boolean isCreateTableAs = tableName instanceof SqlIdentifier
                && query instanceof SqlCall;
        if (isCreateTableAs) {
            hasSubQuery = true;
            visit((SqlCall) query);
        }
        return null;
    }

    @Override
    public Void visitCreateTableFromModel(SqlCreateTableFromModel createTableFromModel) {
        operateType = "create";
        targetEntityName = createTableFromModel.getName().toString();
        SqlNode query = createTableFromModel.operand(2);
        SqlSelect select = (SqlSelect) query;
        SqlNode from = select.getFrom();
        switch (from.getKind()) {
            case IDENTIFIER:
                visit((SqlIdentifier) from);
                break;
            case ORDER_BY:
                hasSubQuery = true;
                visit((SqlOrderBy) from);
                break;
            case JOIN:
                // lateral的情况
                SqlJoin join = (SqlJoin) from;
                visitJoin(join);
                break;
            case SELECT:
                hasSubQuery = true;
                visit((SqlSelect) from);
                break;
            default:
                throw new RuntimeException("Unsupported sub query type:" + from.getKind());

        }
        return null;
    }

    @Override
    public Void visit(SqlSelect select) {
        hasSubQuery = true;
        super.visit(select);
        return null;
    }

    @Override
    public Void visit(SqlJoin join) {
        subQueryHasJoin = true;
        return super.visit(join);
    }

    @Override
    public Void visit(SqlIdentifier identifier) {
        return super.visit(identifier);
    }

    @Override
    public Void visitSubSelect(SqlSelect select) {
        SqlNode from = select.getFrom();
        if (from instanceof SqlIdentifier) {
            visit((SqlIdentifier) from);
        } else {
            hasSubQuery = true;
            visit((SqlCall) from);
        }
        return null;
    }

    @Override
    public Void visitTrainModel(SqlTrainModel trainModel) {
        operateType = "train";
        targetEntityName = trainModel.getModelName().toString();
        SqlNode subQuery = trainModel.operand(2);
        SqlNode subQueryFrom = ((SqlSelect) subQuery).getFrom();
        SqlNode from = ((SqlSelect) subQueryFrom).getFrom();
        if (from instanceof SqlIdentifier) {
            visit((SqlIdentifier) from);
        } else {
            hasSubQuery = true;
            visit((SqlCall) from);
        }
        return null;
    }

    @Override
    public Void visitJoin(SqlJoin join) {
        SqlNode left = join.getLeft();
        switch (left.getKind()) {
            case IDENTIFIER:
                visit((SqlIdentifier) left);
                break;
            case ORDER_BY:
                hasSubQuery = true;
                visit((SqlOrderBy) left);
                break;
            case SELECT:
                hasSubQuery = true;
                visit((SqlSelect) left);
                break;
            default:
                throw new RuntimeException("Unsupported join left:" + left.getKind());
        }
        return null;
    }
}
