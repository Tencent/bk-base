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

package com.tencent.bk.base.dataflow.udf.common;

/**
 * The information which needed to create a user-defined functions
 */
public class FunctionContext {

    private String functionName;
    private String imports;
    private String udfClassName;
    private String returnType;
    private String returnTypeSize;
    private String inputParams;
    private String userClass;
    private String returnDataTypes;
    private String inputData;
    private String userFuncFileName;
    private String typeParameters;
    private String containerState;
    private String castStateMapType;
    private String stateClass;
    private String jepPath;
    private String pythonScriptPath;
    private String resultType;
    private String cpythonPackages;

    private FunctionContext(Builder builder) {
        this.imports = builder.imports;
        this.udfClassName = builder.udfClassName;
        this.returnType = builder.returnType;
        this.inputParams = builder.inputParams;
        this.userClass = builder.userClass;
        this.returnDataTypes = builder.returnDataTypes;
        this.inputData = builder.inputData;
        this.userFuncFileName = builder.userFuncFileName;
        this.typeParameters = builder.typeParameters;
        this.containerState = builder.containerState;
        this.castStateMapType = builder.castStateMapType;
        this.stateClass = builder.stateClass;
        this.functionName = builder.functionName;
        this.jepPath = builder.jepPath;
        this.pythonScriptPath = builder.pythonScriptPath;
        this.returnTypeSize = builder.returnTypeSize;
        this.resultType = builder.resultType;
        this.cpythonPackages = builder.cpythonPackages;
    }

    public String getCpythonPackages() {
        return cpythonPackages;
    }

    public String getImports() {
        return imports;
    }

    public String getUdfClassName() {
        return udfClassName;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getInputParams() {
        return inputParams;
    }

    public String getUserClass() {
        return userClass;
    }

    public String getReturnDataTypes() {
        return returnDataTypes;
    }

    public String getInputData() {
        return inputData;
    }

    public String getUserFuncFileName() {
        return userFuncFileName;
    }

    public String getTypeParameters() {
        return typeParameters;
    }

    public String getCastStateMapType() {
        return castStateMapType;
    }

    public String getContainerState() {
        return containerState;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getStateClass() {
        return stateClass;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getJepPath() {
        return jepPath;
    }

    public String getPythonScriptPath() {
        return pythonScriptPath;
    }

    public String getReturnTypeSize() {
        return returnTypeSize;
    }

    public String getResultType() {
        return resultType;
    }

    public static final class Builder {

        private String functionName;
        private String imports;
        private String udfClassName;
        private String returnType;
        private String inputParams;
        private String userClass;
        private String returnDataTypes;
        private String inputData;
        private String userFuncFileName;
        private String typeParameters;
        private String containerState;
        private String castStateMapType;
        private String stateClass;
        private String jepPath;
        private String pythonScriptPath;
        private String returnTypeSize;
        private String resultType;
        private String cpythonPackages;

        public Builder imports(String imports) {
            this.imports = imports;
            return this;
        }

        public Builder setUdfClassName(String udfClassName) {
            this.udfClassName = udfClassName;
            return this;
        }

        public Builder setReturnType(String returnType) {
            this.returnType = returnType;
            return this;
        }

        public Builder setInputParams(String inputParams) {
            this.inputParams = inputParams;
            return this;
        }

        public Builder setUserClass(String userClass) {
            this.userClass = userClass;
            return this;
        }

        public Builder setReturnDataTypes(String returnDataTypes) {
            this.returnDataTypes = returnDataTypes;
            return this;
        }

        public Builder setInputData(String inputData) {
            this.inputData = inputData;
            return this;
        }

        public Builder setUserFuncFileName(String userFuncFileName) {
            this.userFuncFileName = userFuncFileName;
            return this;
        }

        public Builder setTypeParameters(String typeParameters) {
            this.typeParameters = typeParameters;
            return this;
        }

        public Builder setContainerState(String containerState) {
            this.containerState = containerState;
            return this;
        }

        public Builder setCastStateMapType(String castStateMapType) {
            this.castStateMapType = castStateMapType;
            return this;
        }

        public Builder setStateClass(String stateClass) {
            this.stateClass = stateClass;
            return this;
        }

        public Builder setFunctionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        public Builder setJepPath(String jepPath) {
            this.jepPath = jepPath;
            return this;
        }

        public Builder pythonScriptPath(String pythonScriptPath) {
            this.pythonScriptPath = pythonScriptPath;
            return this;
        }

        public Builder returnTypeSize(String returnTypeSize) {
            this.returnTypeSize = returnTypeSize;
            return this;
        }

        public Builder setResultType(String resultType) {
            this.resultType = resultType;
            return this;
        }

        public Builder setCpythonPackages(String cpythonPackages) {
            this.cpythonPackages = cpythonPackages;
            return this;
        }

        /**
         * 实例化 FunctionContext
         *
         * @return FunctionContext 实例化对象
         */
        public FunctionContext create() {
            return new FunctionContext(this);
        }
    }


}
