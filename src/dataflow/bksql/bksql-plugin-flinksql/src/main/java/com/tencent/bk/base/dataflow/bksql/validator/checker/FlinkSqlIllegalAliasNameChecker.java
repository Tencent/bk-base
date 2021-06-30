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

import com.tencent.blueking.bksql.util.QuoteUtil;
import com.tencent.blueking.bksql.validator.SimpleListenerBasedWalker;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.TableFunction;

public class FlinkSqlIllegalAliasNameChecker extends SimpleListenerBasedWalker {

    private final Pattern commonPattern = Pattern.compile("[A-Za-z_]\\w*");
    private final Pattern udtfPattern0 = Pattern.compile("T\\([A-Za-z]\\w*\\)");
    private final Pattern udtfPattern = Pattern.compile("T\\((.*)\\)");

    @Override
    public void enterNode(PlainSelect plainSelect) {
        // select 中字段别名校验
        plainSelect.getSelectItems().stream()
                .filter(selectItem -> selectItem instanceof SelectExpressionItem)
                .filter(selectItem -> null != ((SelectExpressionItem) selectItem).getAlias())
                .forEach(selectItem -> {
                    String aliasName = ((SelectExpressionItem) selectItem).getAlias().getName();
                    String trimmed = QuoteUtil.trimQuotes(aliasName);
                    if (!commonPattern.matcher(trimmed).matches()) {
                        fail("illegal.alias", aliasName, commonPattern.pattern());
                    }
                });

        // udtf 中别名校验
        if (null != plainSelect.getJoins()) {
            plainSelect.getJoins()
                    .stream()
                    .filter(join -> join.getRightItem() instanceof TableFunction)
                    .filter(join -> ((TableFunction) join.getRightItem()).isLiteral())
                    .filter(join -> null != ((TableFunction) join.getRightItem()).getAlias())
                    .forEach(join -> {
                        String aliasName = ((TableFunction) join.getRightItem()).getAlias().getName();
                        Matcher matcher = udtfPattern.matcher(aliasName);
                        if (matcher.find()) {
                            String[] aliasNames = matcher.group(1).split(",");
                            for (String name : aliasNames) {
                                String trimmed = QuoteUtil.trimQuotes(name);
                                if (!commonPattern.matcher(trimmed).matches()) {
                                    fail("illegal.alias", aliasName, udtfPattern0.pattern());
                                }
                            }
                        } else {
                            fail("illegal.alias", aliasName, udtfPattern0.pattern());
                        }
                    });
        }
    }
}
