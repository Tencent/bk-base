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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

public class TableNameOptimizer extends SimpleListenerWalker {

    private static final Pattern NUMERIC_PREFIX_PATTERN = Pattern.compile("\\d+_.+");

    private final boolean removeNumericPrefix;
    private final boolean moveNumericPrefixToTail;

    @JsonCreator
    public TableNameOptimizer(
            @JsonProperty(value = "removeNumericPrefix") boolean removeNumericPrefix,
            @JsonProperty(value = "moveNumericPrefixToTail") boolean moveNumericPrefixToTail
    ) {
        this.removeNumericPrefix = removeNumericPrefix;
        this.moveNumericPrefixToTail = moveNumericPrefixToTail;
    }

    @Override
    public void enterTableNameNode(SqlIdentifier table) {
        super.enterTableNameNode(table);
        String tableName = table.toString();
        SqlParserPos tableNameParserPos = table.getComponentParserPosition(0);
        if (tableName == null) {
            return;
        }
        if (removeNumericPrefix) {
            if (NUMERIC_PREFIX_PATTERN.matcher(tableName).matches()) {
                table.setNames(Lists.newArrayList(tableName.substring(tableName.indexOf("_") + 1)),
                        Lists.newArrayList(tableNameParserPos));
            }
        }
        if (moveNumericPrefixToTail) {
            if (NUMERIC_PREFIX_PATTERN.matcher(tableName).matches()) {
                int idx = tableName.indexOf("_");
                table.setNames(Lists.newArrayList(
                        tableName.substring(idx + 1) + "_" + tableName.substring(0, idx)),
                        Lists.newArrayList(tableNameParserPos));
            }
        }
    }
}
