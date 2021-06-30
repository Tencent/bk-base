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

import java.util.List;

public class FunctionInfo {

    /**
     * Programming language, java/python.
     */
    private String codeLanguage;

    /**
     * The function type, udf/udtf/udaf.
     */
    private String type;

    /**
     * The user function name.
     */
    private String name;

    /**
     * Function input types.
     * Support String, Integer, Long, Double, Float.
     */
    private List<String> inputTypes;

    /**
     * Function return types.
     * Support String, Integer, Long, Double, Float.
     */
    private List<String> outputTypes;

    private String jepPath;

    /**
     * Stream or batch or all.
     */
    private List<String> roleTypes;

    private String userFuncFileName;

    private String pythonScriptPath;

    private String cpythonPackages;

    private FunctionInfo(Builder builder) {
        this.codeLanguage = builder.codeLanguage;
        this.type = builder.type;
        this.name = builder.name;
        this.inputTypes = builder.inputTypes;
        this.outputTypes = builder.outputTypes;
        this.roleTypes = builder.roleTypes;
        this.userFuncFileName = builder.userFuncFileName;
        this.jepPath = builder.jepPath;
        this.pythonScriptPath = builder.pythonScriptPath;
        this.cpythonPackages = builder.cpythonPackages;
    }

    public String getCodeLanguage() {
        return codeLanguage;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public List<String> getInputTypes() {
        return inputTypes;
    }

    public List<String> getOutputTypes() {
        return outputTypes;
    }

    public List<String> getRoleTypes() {
        return roleTypes;
    }

    public String getUserFuncFileName() {
        return userFuncFileName;
    }

    public String getJepPath() {
        return jepPath;
    }

    public String getPythonScriptPath() {
        return pythonScriptPath;
    }

    public String getCpythonPackages() {
        return cpythonPackages;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String codeLanguage;
        private String type;
        private String name;
        private List<String> inputTypes;
        private List<String> outputTypes;
        private List<String> roleTypes;
        private String userFuncFileName;
        private String jepPath;
        private String pythonScriptPath;
        private String cpythonPackages;

        public Builder codeLanguage(String codeLanguage) {
            this.codeLanguage = codeLanguage;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder inputTypes(List<String> inputTypes) {
            this.inputTypes = inputTypes;
            return this;
        }

        public Builder outputTypes(List<String> outputTypes) {
            this.outputTypes = outputTypes;
            return this;
        }

        public Builder roleTypes(List<String> roleTypes) {
            this.roleTypes = roleTypes;
            return this;
        }

        public Builder userFuncFileName(String userFuncFileName) {
            this.userFuncFileName = userFuncFileName;
            return this;
        }

        public Builder jepPath(String jepPath) {
            this.jepPath = jepPath;
            return this;
        }

        public Builder pythonScriptPath(String pythonScriptPath) {
            this.pythonScriptPath = pythonScriptPath;
            return this;
        }

        public Builder cpythonPackages(String cpythonPackages) {
            this.cpythonPackages = cpythonPackages;
            return this;
        }

        public FunctionInfo create() {
            return new FunctionInfo(this);
        }
    }
}
