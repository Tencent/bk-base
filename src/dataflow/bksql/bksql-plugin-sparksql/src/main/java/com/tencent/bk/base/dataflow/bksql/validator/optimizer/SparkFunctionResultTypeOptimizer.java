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

package com.tencent.bk.base.dataflow.bksql.validator.optimizer;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.validator.ExpressionReplacementOptimizer;
import com.typesafe.config.Config;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SparkFunctionResultTypeOptimizer extends ExpressionReplacementOptimizer {

    public static final String PROPERTY_PREFIX = "spark";
    public static final String PROPERTY_KEY_RESULT_TABLE_NAME = "result_table_name";
    public static final String PROPERTY_KEY_ERROR_COLUMN = "error_column";

    protected final Set<MyChange> funs;
    protected final Config config;
    protected final String outputTableName;
    protected final String errorColumn;
    private final BlueKingTrtTableMetadataConnector generalTableMetadataConnector;
    private final List<ColumnMetadata> metadataList;


    public SparkFunctionResultTypeOptimizer(
            @JacksonInject("properties")
                    Config properties,
            @JsonProperty("names") List<MyChange> funs,
            @JsonProperty(value = "trtTableMetadataUrlPattern", required = true)
                    String trtTableMetadataUrlPattern) {
        super();
        this.funs = new HashSet<>(null != funs ? funs : Collections.EMPTY_LIST);
        config = properties.getConfig(PROPERTY_PREFIX);
        outputTableName = config.getString(PROPERTY_KEY_RESULT_TABLE_NAME);
        errorColumn = config.getString(PROPERTY_KEY_ERROR_COLUMN);
        generalTableMetadataConnector = BlueKingTrtTableMetadataConnector.forUrl(trtTableMetadataUrlPattern);
        metadataList = generalTableMetadataConnector.fetchTableMetadata(outputTableName).listColumns();
    }

    @Override
    protected Expression enterExpressionNode(Function function) {
        String name = function.getName();
        if (null != function.getParameters() && function.getParameters().getExpressions().size() == 1) {
            for (MyChange fun : funs) {
                if (fun.getName().equalsIgnoreCase(name)) {
                    return addCastFunction(function, fun.getResult());
                }
            }
        }
        return function;
    }

    @Override
    public void enterNode(SelectExpressionItem selectExpressionItem) {
        // 判断是否元数据不一致
        if ("select_expr_item".equalsIgnoreCase(selectExpressionItem.getType())
                && (null == selectExpressionItem.getAlias()
                || !selectExpressionItem.getAlias().isUseAs())) {
            if ("column".equalsIgnoreCase(
                    selectExpressionItem.getExpression().getExpressionType())) {
                String name = ((Column) selectExpressionItem.getExpression()).getColumnName();
                if (name.equals(errorColumn)) {
                    // 如果需要统计错误字段可以写在这里
                    throw new RuntimeException("元数据类型不一致。");
                }

            }
        }

        Alias alias = selectExpressionItem.getAlias();
        String type = null;
        if (null != alias && alias.isUseAs()) {
            for (ColumnMetadata metadata : metadataList) {
                if (alias.getName().equals(metadata.getColumnName())) {
                    type = metadata.getDataType().toString();
                    break;
                }
            }
        }
        if (null == type) {
            return;
        }
        Expression expression = selectExpressionItem.getExpression();
        String exp = expression.toString().toLowerCase();
        this.setFloorCast(selectExpressionItem, type, expression, exp);
        this.setCeilCast(selectExpressionItem, type, expression, exp);
        this.setCountCast(selectExpressionItem, type, expression, exp);
        this.setSumCast(selectExpressionItem, type, expression, exp);
        this.setMaxCast(selectExpressionItem, type, expression, exp);
        this.setMinCast(selectExpressionItem, type, expression, exp);
        this.setRankCast(selectExpressionItem, type, expression, exp);
    }

    private void setFloorCast(
        SelectExpressionItem selectExpressionItem,
        String type,
        Expression expression,
        String exp) {
        if (exp.contains("floor(")) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setCeilCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.contains("ceil(")) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setCountCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.startsWith("count(") && "double".equalsIgnoreCase(type)) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setSumCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.startsWith("sum(") && "double".equalsIgnoreCase(type)) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setMaxCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.startsWith("max(") && "double".equalsIgnoreCase(type)) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setMinCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.startsWith("min(") && "double".equalsIgnoreCase(type)) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setRankCast(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        if (exp.startsWith("rank(") && "long".equalsIgnoreCase(type)) {
            this.setCastExpression(selectExpressionItem, type, expression, exp);
        }
    }

    private void setCastExpression(
            SelectExpressionItem selectExpressionItem,
            String type,
            Expression expression,
            String exp) {
        CastExpression cast = new CastExpression();
        cast.setLeftExpression(expression);
        ColDataType colDataType1 = new ColDataType();
        colDataType1.setDataType(type.toLowerCase());
        cast.setType(colDataType1);
        selectExpressionItem.setExpression(cast);
        System.err.println("--");
        System.err.println(exp + "\r\n" + cast.toString());
    }


    private CastExpression addCastFunction(Function function, String toDataType) {
        CastExpression cast = new CastExpression();
        cast.setLeftExpression(function);
        ColDataType colDataType1 = new ColDataType();
        colDataType1.setDataType(toDataType);
        cast.setType(colDataType1);
        return cast;
    }

    public static class MyChange {

        private final String name;

        private final String result;

        @JsonCreator
        public MyChange(
                @JsonProperty("name") String name,
                @JsonProperty("result") String result
        ) {
            this.name = name;
            this.result = result;
        }

        public String getResult() {
            return result;
        }

        public String getName() {
            return name;
        }

    }
}
