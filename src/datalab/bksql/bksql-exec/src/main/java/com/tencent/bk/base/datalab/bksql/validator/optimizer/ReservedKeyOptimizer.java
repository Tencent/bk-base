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
import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.bksql.enums.BkDataReservedFieldEnum;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

public class ReservedKeyOptimizer extends SimpleListenerWalker {

    private static final String UPPER = "upper";
    private static final String LOWER = "lower";
    private List<String> keyWords;
    private String quoting;
    private String caseFormat;

    @JsonCreator
    public ReservedKeyOptimizer(@JsonProperty("keyWords") List<String> keyWords,
            @JsonProperty("quoting") String quoting,
            @JsonProperty(value = "caseFormat") String caseFormat) {
        this.keyWords = keyWords;
        this.quoting = quoting;
        this.caseFormat = caseFormat;
    }

    @Override
    public void enterIdentifierNode(SqlIdentifier identifier) {
        super.enterIdentifierNode(identifier);
        quoteIdentifier(identifier);
    }

    /**
     * 字段转义处理方法
     *
     * @param column 字段SqlIdentifier实例
     */
    private void quoteIdentifier(SqlIdentifier column) {
        ImmutableList<String> names = column.names;
        List<String> newNames = Lists.newArrayList(names.size());
        for (int i = 0, size = names.size(); i < size; i++) {
            final String name = names.get(i);
            final SqlParserPos pos = column.getComponentParserPosition(i);
            String quotedName = quoting + name + quoting;
            if (StringUtils.isBlank(name)) {
                newNames.add(i, "");
                continue;
            }
            boolean addQuote =
                    shouldAddQuote(name, pos);
            if (isBkDataReservedField(name)) {
                newNames.add(i, getBkDataReservedQuotedName(quotedName));
            } else if (addQuote) {
                newNames.add(i, quotedName);
            } else {
                newNames.add(i, name);
            }
        }
        List<SqlParserPos> namesPosList = Lists.newArrayList();
        newNames.forEach(n -> namesPosList.add(SqlParserPos.ZERO));
        column.setNames(newNames, namesPosList);
    }

    /**
     * 判断字段是否需要添加转义符
     *
     * @param name 字段名
     * @param pos 字段位置
     * @return 是否需要添加转义符
     */
    private boolean shouldAddQuote(String name, SqlParserPos pos) {
        boolean isKeyWord = keyWords.contains(name.toUpperCase());
        boolean isQuoted = (pos.isQuoted() && !QuoteUtil
                .isQuoted(name));
        return isKeyWord || isQuoted;
    }

    /**
     * 获取内部保留字段转义后的字段名
     *
     * @param quotedName 转义字段
     * @return 转义后的字段名
     */
    private String getBkDataReservedQuotedName(String quotedName) {
        String newQuotedName;
        if (UPPER.equals(caseFormat)) {
            newQuotedName = StringUtils.upperCase(quotedName);
        } else if (LOWER.equals(caseFormat)) {
            newQuotedName = StringUtils.lowerCase(quotedName);
        } else {
            newQuotedName = quotedName;
        }
        return newQuotedName;
    }

    /**
     * 判断是否是平台保留字段
     *
     * @param name 列名
     * @return 是否是平台保留字段
     */
    private boolean isBkDataReservedField(String name) {
        return Enums.getIfPresent(BkDataReservedFieldEnum.class, name.toUpperCase(Locale.ENGLISH))
                .isPresent();
    }
}
