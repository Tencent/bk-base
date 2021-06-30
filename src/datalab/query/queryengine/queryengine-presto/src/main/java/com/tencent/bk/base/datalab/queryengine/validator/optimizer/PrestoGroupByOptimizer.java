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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PrestoGroupByOptimizer extends SimpleListenerWalker {

    private final Map<String, SqlNode> groupByReplaceMap = Maps.newHashMap();
    private final Map<String, SqlNode> havingReplaceMap = Maps.newHashMap();

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        replaceGroupAndHavingAlias(select);
    }

    @Override
    public void exitPlainSelectNode(SqlSelect select) {
        clearReplaceMap();
    }

    @Override
    public void enterSubSelectNode(SqlSelect subSelect) {
        super.enterSubSelectNode(subSelect);
        replaceGroupAndHavingAlias(subSelect);
    }

    @Override
    public void exitSubSelectNode(SqlSelect subSelect) {
        clearReplaceMap();
    }

    private void replaceGroupAndHavingAlias(SqlSelect select) {
        if (Objects.nonNull(select.getSelectList())) {
            for (SqlNode node : select.getSelectList()) {
                if (node instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) node;
                    if (call.getKind() == SqlKind.AS) {
                        SqlNode beforeAs = call.getOperands()[0];
                        SqlIdentifier alias = (SqlIdentifier) call.getOperands()[1];
                        groupByReplaceMap.putIfAbsent(alias.toString(), SqlNode.clone(beforeAs));
                        havingReplaceMap.putIfAbsent(alias.toString(), SqlNode.clone(beforeAs));
                    }
                }
            }
        }

        if (Objects.nonNull(select.getGroup())) {
            SqlNodeList replaceNodeList = new SqlNodeList(SqlParserPos.ZERO);
            for (SqlNode node : select.getGroup()) {
                if (node instanceof SqlIdentifier) {
                    String groupByName = node.toString();
                    SqlNode selectNode = groupByReplaceMap.get(groupByName);
                    if (selectNode != null) {
                        //说明是使用的别名，需要替换为原始字段或表达式
                        replaceNodeList.add(selectNode);
                    } else {
                        replaceNodeList.add(node);
                    }
                } else {
                    replaceNodeList.add(node);
                }
            }
            select.setGroupBy(replaceNodeList);
        }

        if (Objects.nonNull(select.getHaving())) {
            SqlNode havingNode = select.getHaving();
            if (havingNode instanceof SqlBasicCall) {
                replaceSqlCallOperand((SqlBasicCall) havingNode);
            }
        }
    }

    private void replaceSqlCallOperand(SqlBasicCall call) {
        SqlNode[] operands = call.operands;
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof SqlIdentifier) {
                String havingColumnName = operands[i].toString();
                SqlNode selectNode = havingReplaceMap.get(havingColumnName);
                if (selectNode != null) {
                    call.setOperand(i, selectNode);
                }
            } else if (operands[i] instanceof SqlBasicCall) {
                replaceSqlCallOperand((SqlBasicCall) operands[i]);
            }
        }
    }

    private void clearReplaceMap() {
        groupByReplaceMap.clear();
        havingReplaceMap.clear();
    }
}
