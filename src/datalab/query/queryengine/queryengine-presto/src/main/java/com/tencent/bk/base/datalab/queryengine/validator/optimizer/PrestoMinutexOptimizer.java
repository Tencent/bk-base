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

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.validator.optimizer.MinutexOptimizer;
import com.typesafe.config.Config;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PrestoMinutexOptimizer extends MinutexOptimizer {

    @JacksonInject("properties")
    private Config properties;

    @JsonCreator
    public PrestoMinutexOptimizer(@JsonProperty("dateFormat") String dateFormat,
            @JsonProperty("convertFunc") String convertFunc) {
        super(dateFormat, convertFunc);
    }

    @Override
    protected SqlNode[] getFuncParams(int minutes) {
        final SqlNode[] params = new SqlNode[2];
        SqlBasicCall fromUnixFunc = getFromUnixFunc(minutes);
        params[0] = fromUnixFunc;
        params[1] = SqlLiteral.createCharString(DATE_FORMAT, SqlParserPos.ZERO);
        return params;
    }

    /**
     * 生成 FROM_UNIXTIME 函数节点
     *
     * @param minutes 分钟数
     * @return FROM_UNIXTIME 函数节点
     */
    private SqlBasicCall getFromUnixFunc(int minutes) {
        SqlNode[] fromUnixParams = new SqlNode[1];
        SqlUnresolvedFunction function = new SqlUnresolvedFunction(
                new SqlIdentifier(Lists.newArrayList("FROM_UNIXTIME"), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
        SqlIdentifier modLeft = new SqlIdentifier(Lists.newArrayList(DTEVENTTIMESTAMP),
                SqlParserPos.ZERO);
        SqlNumericLiteral modRight = SqlLiteral
                .createExactNumeric(Integer.toString(minutes * 60000), SqlParserPos.ZERO);
        SqlBasicCall modExp = new SqlBasicCall(SqlStdOperatorTable.PERCENT_REMAINDER,
                SqlNodeList.of(modLeft, modRight)
                        .toArray(), SqlParserPos.ZERO);
        SqlNode minusLeft = new SqlIdentifier(Lists.newArrayList(DTEVENTTIMESTAMP),
                SqlParserPos.ZERO);
        SqlNode minusRight = modExp;
        SqlBasicCall minusExp = new SqlBasicCall(SqlStdOperatorTable.MINUS,
                SqlNodeList.of(minusLeft, minusRight)
                        .toArray(), SqlParserPos.ZERO);
        SqlNode divideLeft = minusExp;
        SqlNode divideRight = SqlLiteral.createExactNumeric("1000", SqlParserPos.ZERO);
        SqlBasicCall divideExp = new SqlBasicCall(SqlStdOperatorTable.DIVIDE,
                SqlNodeList.of(divideLeft, divideRight)
                        .toArray(), SqlParserPos.ZERO);
        fromUnixParams[0] = divideExp;
        return new SqlBasicCall(function, fromUnixParams, SqlParserPos.ZERO);
    }
}
