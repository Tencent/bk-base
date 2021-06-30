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

package com.tencent.bk.base.dataflow.core.topo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserDefinedFunctions implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Map<String, Object>> debugSourceData;

    private List<UserDefinedFunctionConfig> functions;

    public UserDefinedFunctions(List<Map<String, Object>> debugSourceData, List<UserDefinedFunctionConfig> functions) {
        this.debugSourceData = debugSourceData;
        this.functions = functions;
    }

    public List<Map<String, Object>> getDebugSourceData() {
        return debugSourceData;
    }

    public List<UserDefinedFunctionConfig> getFunctions() {
        return functions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private List<Map<String, Object>> debugSourceData;

        private List<UserDefinedFunctionConfig> functions = new ArrayList<>();

        public Builder setDebugSourceData(List<Map<String, Object>> debugSourceData) {
            this.debugSourceData = debugSourceData;
            return this;
        }

        public Builder addFunction(UserDefinedFunctionConfig function) {
            this.functions.add(function);
            return this;
        }

        public UserDefinedFunctions create() {
            return new UserDefinedFunctions(debugSourceData, functions);
        }
    }

}
