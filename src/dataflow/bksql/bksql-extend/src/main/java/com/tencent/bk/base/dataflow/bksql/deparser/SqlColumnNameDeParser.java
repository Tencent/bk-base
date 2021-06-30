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

import com.tencent.blueking.bksql.deparser.SimpleListenerBasedDeParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;


public class SqlColumnNameDeParser extends SimpleListenerBasedDeParser {

    private List<String> result = new ArrayList<>();

    @Override
    public void enterNode(PlainSelect select) {
        if (result.size() == 0) {
            List<SelectItem> selectItems = select.getSelectItems();

            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    result.add(getColumnName((SelectExpressionItem)selectItem));
                }
            }
        }
    }

    private String getColumnName(SelectExpressionItem selectItem) {
        if (selectItem.getAlias() != null) {
            return formatColumnName(selectItem.getAlias().getName());
        }
        if (selectItem.getExpression() instanceof Column) {
            Column expression = (Column) selectItem.getExpression();
            return formatColumnName(expression.getColumnName());
        }
        throw new RuntimeException("Some expressions do not have an alias name, please check.");
    }

    private String formatColumnName(String columnName) {
        if (columnName.startsWith("`") && columnName.endsWith("`")) {
            return columnName.replaceAll("`", "");
        } else {
            return columnName;
        }
    }

    @Override
    protected Object getRetObj() {
        return result;
    }
}
