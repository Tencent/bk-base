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
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.util.Locale;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;


public class ColumnNameOptimizer extends ExpressionReplacementOptimizer {

    private static final String UPPER = "upper";
    private static final String LOWER = "lower";
    private String caseFormat;

    @JsonCreator
    public ColumnNameOptimizer(@JsonProperty(value = "caseFormat") String caseFormat) {
        this.caseFormat = caseFormat;
    }

    @Override
    protected SqlNode enterSelectColumn(SqlIdentifier column) {
        return addAliasName(column);
    }

    @Override
    protected SqlBasicCall enterAsExpression(SqlBasicCall asExpression) {
        SqlNode leftAs = asExpression.operand(0);
        if (leftAs instanceof SqlIdentifier) {
            convertColumnName((SqlIdentifier) leftAs);
        }
        SqlNode rightAs = asExpression.operand(1);
        if (rightAs instanceof SqlIdentifier) {
            SqlIdentifier alias = (SqlIdentifier) rightAs;
            String aliasName = alias.names.get(alias.names.size() - 1);
            aliasName = QuoteUtil.addDoubleQuotes(aliasName);
            alias.setNames(Lists.newArrayList(aliasName),
                    Lists.newArrayList(alias.getParserPosition()));
        }
        return asExpression;
    }

    private SqlBasicCall addAliasName(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        columnName = "\"" + columnName + "\"";
        SqlIdentifier alias = new SqlIdentifier(Lists.newArrayList(columnName), SqlParserPos.ZERO);
        SqlNode[] asParams = new SqlNode[2];
        asParams[1] = alias;
        asParams[0] = convertColumnName(column);

        return new SqlBasicCall(SqlStdOperatorTable.AS, asParams, SqlParserPos.ZERO);
    }

    private SqlIdentifier convertColumnName(SqlIdentifier column) {
        String columnName = column.names.get(column.names.size() - 1);
        String transColumnName = convertName(columnName);
        column.setNames(Lists.newArrayList(transColumnName),
                Lists.newArrayList(column.getParserPosition()));
        return column;
    }

    private String convertName(String columnName) {
        String transColumnName;
        if (UPPER.equalsIgnoreCase(caseFormat)) {
            transColumnName = columnName.toUpperCase(Locale.US);
        } else if (LOWER.equalsIgnoreCase(caseFormat)) {
            transColumnName = columnName.toLowerCase(Locale.US);
        } else {
            transColumnName = columnName;
        }
        return transColumnName;
    }
}