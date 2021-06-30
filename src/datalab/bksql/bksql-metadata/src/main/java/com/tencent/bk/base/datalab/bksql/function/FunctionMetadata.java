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

package com.tencent.bk.base.datalab.bksql.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.bksql.util.DataType;
import java.util.List;
import java.util.Objects;

public final class FunctionMetadata {

    private final String functionName;
    private final String description;
    private final List<ParameterMetadata> parameterList;
    private final DataType returnType;
    private final String namespace;

    @JsonCreator
    public FunctionMetadata(
            @JsonProperty("functionName") String functionName,
            @JsonProperty("description") String description,
            @JsonProperty("parameterList") List<ParameterMetadata> parameterList,
            @JsonProperty("returnType") DataType returnType,
            @JsonProperty("namespace") String namespace
    ) {
        this.functionName = functionName;
        this.description = description;
        this.parameterList = parameterList;
        this.returnType = returnType;
        this.namespace = namespace;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getDescription() {
        return description;
    }

    public List<ParameterMetadata> getParameterList() {
        return parameterList;
    }

    public DataType getReturnType() {
        return returnType;
    }

    public String getNamespace() {
        return namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionMetadata that = (FunctionMetadata) o;
        return Objects.equals(functionName, that.functionName)
                && Objects.equals(description, that.description)
                && Objects.equals(parameterList, that.parameterList)
                && returnType == that.returnType
                && Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, description, parameterList, returnType, namespace);
    }

    @Override
    public String toString() {
        return "FunctionMetadata{"
                + "functionName='" + functionName + '\''
                + ", description='" + description + '\''
                + ", parameterList=" + parameterList
                + ", returnType=" + returnType
                + ", namespace='" + namespace + '\''
                + '}';
    }
}
