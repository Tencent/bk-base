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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTrainModel;

public class MLSqlTableNamesDeParser implements DeParser {

    private final List<String> names = new ArrayList<>();

    /**
     * 获取node中所有可能的表
     */
    public void extractSourceTablesFromNode(SqlNode node) {
        if (node instanceof SqlSelect) {
            names.addAll(extractTablesFromSelectNode((SqlSelect) node));
        } else {
            names.addAll(extractTableFromInsertNode((SqlInsert) node));
        }
    }

    /**
     * 获取select中的所有可能表
     */
    public List<String> extractTablesFromSelectNode(SqlSelect select) {
        return extractTablesFromNode(select, false);
    }

    /**
     * 获取insert中所有可能表，与select相比多了目标表
     */
    public List<String> extractTableFromInsertNode(SqlInsert insert) {
        List<String> sources = extractTablesFromNode(insert.getSource(), false);
        SqlIdentifier identifier = (SqlIdentifier) insert.getTargetTable();
        sources.add(identifier.toString());
        return sources;
    }


    /**
     * 递归的获取sqlnode中的，identifier，即为所有可能访问的表
     */
    private List<String> extractTablesFromNode(SqlNode sqlNode, Boolean fromOrJoin) {
        if (sqlNode == null) {
            return Collections.EMPTY_LIST;
        } else {
            List<String> tableList = new ArrayList<>();
            switch (sqlNode.getKind()) {
                case SELECT:
                    SqlSelect select = (SqlSelect) sqlNode;
                    tableList.addAll(extractTablesFromNode(select.getFrom(), true));

                    List<SqlNode> sqlNodeList = select.getSelectList().getList();
                    sqlNodeList.forEach(node -> {
                        if (node instanceof SqlCall) {
                            tableList.addAll(extractTablesFromNode(node, false));
                        }
                    });

                    tableList.addAll(extractTablesFromNode(select.getWhere(), false));

                    tableList.addAll(extractTablesFromNode(select.getHaving(), false));
                    break;
                case JOIN:
                    SqlJoin join = (SqlJoin) sqlNode;
                    tableList.addAll(extractTablesFromNode(join.getLeft(), true));
                    tableList.addAll(extractTablesFromNode(join.getRight(), true));
                    break;
                case AS:
                    SqlCall as = (SqlCall) sqlNode;
                    tableList.addAll(extractTablesFromNode(as.operand(0), fromOrJoin));
                    break;
                case IDENTIFIER:
                    if (fromOrJoin) {
                        SqlIdentifier identifier = (SqlIdentifier) sqlNode;
                        String tableName = identifier.toString();
                        tableList.add(tableName.split("\\.")[0]);
                    }
                    break;
                default:
                    if (sqlNode instanceof SqlCall) {
                        SqlCall call = (SqlCall) sqlNode;
                        call.getOperandList().forEach(node -> {
                            tableList.addAll(extractTablesFromNode(node, false));
                        });
                    }
            }
            return tableList;
        }
    }

    @Override
    public Object deParse(SqlNode sqlNode) {
        SqlNode queryNode = null;
        switch (sqlNode.getKind()) {
            case TRAIN:
                queryNode = ((SqlTrainModel) sqlNode).getSubQuery();
                break;
            case CREATE_TABLE:
                queryNode = ((SqlCreateTableFromModel) sqlNode).getQuery();
                break;
            default:
                queryNode = sqlNode;
        }
        this.extractSourceTablesFromNode(queryNode);
        return names;
    }

}
