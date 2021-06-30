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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;

public class TableNamesWithStorage extends SimpleListenerDeParser {

    private final Map<String, String> tbNameWithStorageMap = Maps.newHashMap();

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        extractTableNameWithStorage(select.getFrom());
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        extractTableNameWithStorage(join.getLeft());
        extractTableNameWithStorage(join.getRight());
    }

    private void extractTableNameWithStorage(SqlNode node) {
        SqlKind nodeKind = node.getKind();
        switch (nodeKind) {
            case IDENTIFIER:
                getTbMapByIdentifer((SqlIdentifier) node);
                break;
            case SELECT:
                extractTableNameWithStorage(((SqlSelect) node).getFrom());
                break;
            case ORDER_BY:
                extractTableNameWithStorage(((SqlOrderBy) node).query);
                break;
            case AS:
                SqlNode beforeAs = ((SqlBasicCall) node).operands[0];
                if (beforeAs instanceof SqlIdentifier) {
                    getTbMapByIdentifer((SqlIdentifier) beforeAs);
                } else {
                    extractTableNameWithStorage(beforeAs);
                }
                break;
            default:
                break;
        }
    }

    private void getTbMapByIdentifer(SqlIdentifier node) {
        String tableName = "";
        String storageType = "";
        SqlIdentifier table = node;
        if (table.names.size() >= 2) {
            tableName = table.names.get(table.names.size() - 2);
            storageType = table.names.get(table.names.size() - 1);
        } else if (table.names.size() == 1) {
            tableName = table.toString();
        }
        if (StringUtils.isNoneBlank(tableName, storageType)) {
            tbNameWithStorageMap.putIfAbsent(tableName, storageType);
        }
    }

    @Override
    protected Object getRetObj() {
        return tbNameWithStorageMap;
    }
}
