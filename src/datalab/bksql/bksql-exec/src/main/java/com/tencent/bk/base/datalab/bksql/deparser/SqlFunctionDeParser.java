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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;

public class SqlFunctionDeParser extends SimpleListenerDeParser {

    public static final String CHECK_PARAMS = "check_params";
    private static final String COLUMN_TYPE = "column";
    private static final String STRING_TYPE = "CHAR";
    private static final String DECIMAL_TYPE = "DECIMAL";

    private Map<String, List<FunctionParamTask>> functions = new HashMap<>();

    private boolean checkParams = false;

    @JsonCreator
    public SqlFunctionDeParser(
            @JacksonInject("properties")
                    Config properties
    ) {
        if (properties.hasPath(CHECK_PARAMS) && properties.getBoolean(CHECK_PARAMS)) {
            checkParams = true;
        }
    }

    @Override
    protected Object getRetObj() {
        if (checkParams) {
            return functions;
        } else {
            return functions.keySet();
        }
    }

    @Override
    public void enterFunctionNode(SqlBasicCall function) {
        super.enterFunctionNode(function);
        SqlOperator funcOpt = function.getOperator();
        if (functions.containsKey(funcOpt.getName())) {
            return;
        }
        if (!checkParams) {
            functions.put(funcOpt.getName(), null);
            return;
        }
        SqlNode[] params = function.getOperands();
        List<FunctionParamTask> functionParams = new ArrayList<>();
        for (SqlNode param : params) {
            FunctionParamTask.Builder builder = FunctionParamTask.builder();
            if (param instanceof SqlIdentifier) {
                builder.type("column");
                builder.value(param.toString());
                functionParams.add(builder.create());
                break;
            } else if (param instanceof SqlNumericLiteral) {
                builder.type(DECIMAL_TYPE);
            } else if (param instanceof SqlCharStringLiteral) {
                builder.type(STRING_TYPE);
            } else {
                throw new RuntimeException("Not support expression type " + param.getKind());
            }
        }
        functions.put(funcOpt.getName(), functionParams);
    }

    public static final class FunctionParamTask {

        private final String type;
        private final Object value;

        public FunctionParamTask(String type, Object value) {
            this.type = type;
            this.value = value;
        }

        public static Builder builder() {
            return new Builder();
        }

        @JsonProperty("type")
        public String getType() {
            return type;
        }

        @JsonProperty("value")
        public Object getValue() {
            return value;
        }

        public static final class Builder {

            private String type;
            private Object value;

            public Builder type(String type) {
                this.type = type;
                return this;
            }

            public Builder value(Object value) {
                this.value = value;
                return this;
            }

            public FunctionParamTask create() {
                return new FunctionParamTask(type, value);
            }
        }
    }
}

