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

package com.tencent.bk.base.dataflow.bksql.validator.checker;


import static com.tencent.bk.base.dataflow.bksql.util.Constants.WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME;

import com.tencent.blueking.bksql.validator.SimpleListenerBasedWalker;
import java.util.List;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;

public class FlinkStartEndTimeChecker extends SimpleListenerBasedWalker {

    @Override
    public void enterNode(PlainSelect plainSelect) {
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        selectItems.stream()
                .filter(selectItem -> selectItem instanceof SelectExpressionItem)
                .forEach(selectItem -> {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (expression instanceof Column) {
                        String columnName = ((Column) expression).getColumnName();
                        Alias alias = ((SelectExpressionItem) selectItem).getAlias();
                        if (isStartEndTimeColumn(columnName, alias)) {
                            fail("selected.starttime.endtime.sql.error", columnName, FlinkStartEndTimeChecker.class);
                        }

                    }
                });
    }

    /**
     * 用户SQL不能显示指定 _startTime_ 或者 _endTime_
     * 如需使用，应当通过 AS 关键字设置重命名
     *
     * @param columnName 列名
     * @param alias 别名
     */
    private boolean isStartEndTimeColumn(String columnName, Alias alias) {
        if (StringUtils.equals(columnName, WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME) && alias == null) {
            return true;
        }
        if (StringUtils.equals(columnName, WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME) && alias == null) {
            return true;
        }
        if ((alias != null && StringUtils.equals(alias.getName(), WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME))) {
            return true;
        }
        if ((alias != null && StringUtils.equals(alias.getName(), WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME))) {
            return true;
        }
        return false;
    }
}
