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

import static com.tencent.bk.base.datalab.bksql.constant.Constants.PATTERN_OF_YYYY_MM_DD_HH_MM_SS;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

public class DruidAggFunctionOptimizer extends ExpressionReplacementOptimizer {

    public static final String TIME_FORMAT = "TIME_FORMAT";
    public static final String TIME_PARSE = "TIME_PARSE";
    private static final List<String> NEED_CONVERT_FUNC_LIST = ImmutableList.of("MAX", "MIN");
    private String aggColumn;

    @JsonCreator
    public DruidAggFunctionOptimizer(@JsonProperty("aggColumn") String aggColumn) {
        this.aggColumn = aggColumn;
    }

    @Override
    protected SqlNode enterFunction(SqlBasicCall node) {
        String functionName = node.getOperator().getName();
        if (!NEED_CONVERT_FUNC_LIST.contains(StringUtils.upperCase(functionName))) {
            return node;
        }
        if (!checkOperands(node)) {
            return node;
        }
        SqlIdentifier paramNode = (SqlIdentifier) node.operands[0];
        String paramName = paramNode.names.get(paramNode.names.size() - 1);
        if (aggColumn.equalsIgnoreCase(paramName)) {
            SqlNode[] params = new SqlNode[2];
            params[0] = createAggFunctionNode(functionName, paramNode);
            params[1] = SqlLiteral
                    .createCharString(PATTERN_OF_YYYY_MM_DD_HH_MM_SS, SqlParserPos.ZERO);
            return new SqlBasicCall(createUdfSqlFunction(TIME_FORMAT),
                    params,
                    SqlParserPos.ZERO);
        }
        return node;
    }

    /**
     * 构建转换后的聚合函数
     *
     * @param functionName 聚合函数名
     * @param param 参数节点
     * @return 转换后的聚合函数节点
     */
    private SqlNode createAggFunctionNode(String functionName, SqlIdentifier param) {
        SqlNode timeFormatNode = new SqlBasicCall(createUdfSqlFunction(TIME_PARSE),
                new SqlNode[]{new SqlIdentifier(param.names, SqlParserPos.ZERO)},
                SqlParserPos.ZERO);
        return new SqlBasicCall(createUdfSqlFunction(functionName),
                new SqlNode[]{timeFormatNode},
                SqlParserPos.ZERO);
    }

    /**
     * 校验参数个数和参数类型是否符合转换规则
     *
     * @param node 函数节点
     * @return true:校验成功 false:校验失败
     */
    private boolean checkOperands(SqlBasicCall node) {
        return node.operands != null && node.operands.length == 1
                && (node.operands[0] instanceof SqlIdentifier);
    }

    /**
     * 生成自定义函数
     *
     * @param functionName 函数名
     * @return SqlFunction 实例
     */
    private SqlFunction createUdfSqlFunction(String functionName) {
        return new SqlUnresolvedFunction(
                new SqlIdentifier(Lists.newArrayList(functionName), SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
}