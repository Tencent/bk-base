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
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.commons.lang3.ObjectUtils;

public class ColumnsSelectDeparser extends SelectDeParser {

    private List<String> columns;

    public ColumnsSelectDeparser(List<String> columns) {
        this.columns = columns;
    }

    /**
     * 解析Join语法中的相关表信息，找到后对源表名称进行替换
     */
    public void parseJoinIdentifier(SqlNode node) {
        if (node.getKind() == SqlKind.IDENTIFIER) {
            this.getBuffer().append("__DATA_SOURCE_INPUT_SQL__");
        } else if (node.getKind() == SqlKind.AS) {
            SqlBasicCall call = (SqlBasicCall) node;
            SqlNode beforAs = call.operand(0);
            SqlNode afterAs = call.operand(1);
            if (beforAs.getKind() == SqlKind.IDENTIFIER) {
                this.getBuffer().append("__DATA_SOURCE_INPUT_SQL__");
                this.getBuffer().append(" AS ");
                afterAs.accept(this);
            } else {
                node.accept(this);
            }
        } else {
            node.accept(this);
        }
    }

    /**
     * 处理From节点的相关信息
     *
     * @param fromNode from节点信息
     */
    public void dealWithFromNode(SqlNode fromNode) {
        SqlKind kind = fromNode.getKind();
        switch (kind) {
            case SELECT:
                this.getBuffer().append("(");
                visit((SqlSelect) fromNode);
                this.getBuffer().append(")");
                break;
            case IDENTIFIER:
                //visit((SqlIdentifier) fromNode);
                this.getBuffer().append("__DATA_SOURCE_INPUT_SQL__");
                break;
            case JOIN:
                visit((SqlJoin) fromNode);
                break;
            case ORDER_BY:
                this.getBuffer().append("(");
                visit((SqlOrderBy) fromNode);
                this.getBuffer().append(")");
                break;
            default:
                if (fromNode instanceof SqlBasicCall) {
                    visit((SqlBasicCall) fromNode);
                } else {
                    fromNode.accept(this);
                }
        }
    }

    @Override
    public Void visit(SqlIdentifier identifier) {
        String idString = identifier.toString();
        if (idString.contains(".")) {
            String[] splits = idString.split("\\.");
            this.getBuffer().append(splits[1]);
            columns.add(splits[1]);
        } else {
            this.getBuffer().append(identifier.getSimple());
            columns.add(identifier.getSimple());
        }
        return null;
    }

    @Override
    public Void visit(SqlJoin join) {
        JoinType joinType = join.getJoinType();
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        JoinConditionType conditionType = join.getConditionType();
        SqlNode condition = join.getCondition();
        if (joinType == JoinType.COMMA) {
            parseJoinIdentifier(left);
            this.getBuffer().append(", ");
            parseJoinIdentifier(right);
        }
        if (joinType == JoinType.CROSS) {
            parseJoinIdentifier(left);
            this.getBuffer().append(" CROSS JOIN ");
            parseJoinIdentifier(right);
        } else if (joinType == JoinType.INNER) {
            parseJoinIdentifier(left);
            this.getBuffer().append(" INNER JOIN ");
            parseJoinIdentifier(right);
        } else if (joinType == JoinType.FULL) {
            parseJoinIdentifier(left);
            this.getBuffer().append(" FULL OUTER JOIN ");
            parseJoinIdentifier(right);
        } else if (joinType == JoinType.LEFT) {
            parseJoinIdentifier(left);
            if (join.isNatural()) {
                this.getBuffer().append(" NATURAL");
            }
            this.getBuffer().append(" LEFT OUTER JOIN ");
            parseJoinIdentifier(right);
        } else if (joinType == JoinType.RIGHT) {
            parseJoinIdentifier(left);
            if (join.isNatural()) {
                this.getBuffer().append(" NATURAL");
            }
            this.getBuffer().append(" RIGHT OUTER JOIN ");
            parseJoinIdentifier(right);
        }

        visitJoinCondition(conditionType, condition);

        return null;
    }

    @Override
    public Void visit(SqlSelect select) {
        this.getBuffer().append("SELECT ");
        if (select.isKeywordPresent(SqlSelectKeyword.DISTINCT)) {
            this.getBuffer().append("DISTINCT ");
        }
        Optional.ofNullable(select.getSelectList())
                .ifPresent(node -> node.accept(this));
        Optional.ofNullable(select.getFrom()).ifPresent(node -> {
            this.getBuffer().append(" FROM ");
            dealWithFromNode(node);

        });
        Optional.ofNullable(select.getWhere()).ifPresent(node -> {
            this.getBuffer().append(" WHERE ");
            node.accept(this);
        });
        Optional.ofNullable(select.getGroup()).ifPresent(node -> {
            this.getBuffer().append(" GROUP BY ");
            node.accept(this);
        });
        Optional.ofNullable(select.getHaving()).ifPresent(node -> {
            this.getBuffer().append(" HAVING ");
            node.accept(this);
        });
        if (ObjectUtils.anyNotNull(select.getOffset(), select.getFetch())) {
            this.getBuffer().append(" LIMIT ");
            Optional.ofNullable(select.getOffset()).ifPresent(
                    node -> this.getBuffer().append((((SqlNumericLiteral) node).getValue() + ", ")));
            Optional.ofNullable(select.getFetch()).ifPresent(
                    node -> this.getBuffer().append((((SqlNumericLiteral) node).getValue())));
        }
        return null;
    }
}
