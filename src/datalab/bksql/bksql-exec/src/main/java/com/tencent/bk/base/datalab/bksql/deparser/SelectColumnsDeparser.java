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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;

public class SelectColumnsDeparser extends SimpleListenerDeParser {

    private static final Splitter DOT_SPLITTER = Splitter.on(".");
    private List<String> columns = new ArrayList<>();

    private void addToColumns(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            columns.add(getDisplayColumnName(node.toString()));
        } else if (node instanceof SqlCall) {
            SqlKind kind = node.getKind();
            if (kind == SqlKind.AS) {
                columns.add(getDisplayColumnName(((SqlCall) node).operand(1).toString()));
            } else {
                throw new RuntimeException(String.format("Not support sqlKind:%s", kind));
            }
        }
    }

    @Override
    public void exitQueryNode(SqlCall call) {
        super.exitQueryNode(call);
        SqlKind kind = call.getKind();
        if (kind == SqlKind.SELECT) {
            SqlNodeList selectList = ((SqlSelect) call).getSelectList();
            selectList.forEach(node -> addToColumns(node));
        } else if (kind == SqlKind.ORDER_BY) {
            SqlNode query = ((SqlOrderBy) call).query;
            if (query.getKind() == SqlKind.SELECT) {
                SqlNodeList selectList = ((SqlSelect) query).getSelectList();
                selectList.forEach(node -> addToColumns(node));
            }
        } else {
            throw new RuntimeException(String.format("Not support sqlKind:%s", kind));
        }
    }

    private String getDisplayColumnName(String name) {
        Preconditions.checkNotNull(name);
        List<String> names = DOT_SPLITTER.splitToList(name);
        return names.size() >= 1 ? names.get(names.size() - 1) : StringUtils.EMPTY;
    }

    @Override
    protected Object getRetObj() {
        return columns;
    }
}
