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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.blueking.bksql.deparser.SimpleListenerBasedDeParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class DataFlowDimensionDeParserV3 extends SimpleListenerBasedDeParser {

    private static final Set<String> CONSTANT_TYPE = new HashSet<>(Arrays.asList("string_value",
            "double_value",
            "long_value"));

    private Map<String, String> selectExpressions = new HashMap<>();
    private Set<String> groupExpressions = new HashSet<>();
    private List<String> fields = new ArrayList<>();
    private List<DimensionTask> results = new ArrayList<>();
    private Set<String> constantFields = new HashSet<>();


    @Override
    public void enterNode(Select select) {
        super.enterNode(select);
        if (select.getSelectBody() instanceof PlainSelect) {
            List<SelectItem> selectItems = ((PlainSelect) select.getSelectBody()).getSelectItems();
            List<Expression> groupByColumnReferences = ((PlainSelect) select.getSelectBody())
                    .getGroupByColumnReferences();
            if (groupByColumnReferences != null) {
                for (Expression expression : groupByColumnReferences) {
                    groupExpressions.add(formatColumnName(expression.toString().toLowerCase()));
                }
            }

            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (CONSTANT_TYPE.contains(expression.getExpressionType())) {
                        constantFields.add(getColumnName((SelectExpressionItem) selectItem));
                    }
                    selectExpressions.put(formatColumnName(expression.toString().toLowerCase()),
                            getColumnName((SelectExpressionItem) selectItem));
                    fields.add(getColumnName((SelectExpressionItem) selectItem));
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

    private void collectResult() {
        Set<String> dimensionFields = new HashSet<>();
        groupExpressions.forEach(expression -> {
            if (selectExpressions.containsKey(expression)) {
                dimensionFields.add(selectExpressions.get(expression));
            }
            if (selectExpressions.containsValue(expression)) {
                dimensionFields.add(expression);
            }
        });

        fields.forEach(field -> {
            DimensionTask.Builder builder = DimensionTask.builder();
            if (dimensionFields.contains(field) || (dimensionFields.size() > 0 && constantFields.contains(field))) {
                builder.buildFieldName(field);
                builder.buildIsDimension(true);
            } else {
                builder.buildFieldName(field);
                builder.buildIsDimension(false);
            }
            results.add(builder.create());
        });
    }

    @Override
    protected Object getRetObj() {
        collectResult();
        return results;
    }

    public static final class DimensionTask {
        private final String fieldName;
        private final boolean isDimension;

        public DimensionTask(String fieldName, boolean isDimension) {
            this.fieldName = fieldName;
            this.isDimension = isDimension;
        }

        @JsonProperty("field_name")
        public String getFieldName() {
            return fieldName;
        }

        @JsonProperty("is_dimension")
        public boolean isDimension() {
            return isDimension;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String fieldName;
            private boolean isDimension;

            public Builder buildFieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            public Builder buildIsDimension(boolean isDimension) {
                this.isDimension = isDimension;
                return this;
            }

            public DimensionTask create() {
                return new DimensionTask(fieldName, isDimension);
            }
        }
    }
}