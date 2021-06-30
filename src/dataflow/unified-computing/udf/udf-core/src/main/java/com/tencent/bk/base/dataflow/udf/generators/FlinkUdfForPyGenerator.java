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

package com.tencent.bk.base.dataflow.udf.generators;

import com.google.common.base.CaseFormat;
import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import com.tencent.bk.base.dataflow.udf.template.FlinkPyUdfTemplate;
import java.text.MessageFormat;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlinkUdfForPyGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        FunctionContext functionContext = createFunctionContext(functionInfo);
        String code = new FlinkPyUdfTemplate().generateFuncCode(functionContext);
        String className = getClassName(functionInfo.getUserFuncFileName());

        String javaFile = String.format("%s/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.FLINK_UDF_DIRECTORY, className).replace(".", "/")
                + ".java";
        UdfUtils.createUdfFile(javaFile, code);
    }

    private FunctionContext createFunctionContext(FunctionInfo functionInfo) {
        FunctionContext.Builder builder = FunctionContext.builder();
        builder.setUdfClassName(getClassName(functionInfo.getUserFuncFileName()));
        builder.setReturnType(functionInfo.getOutputTypes().stream().collect(Collectors.joining(", ")));
        // set input params
        builder.setInputParams(
                IntStream.range(0, functionInfo.getInputTypes().size())
                        .mapToObj(i -> MessageFormat.format("{0} {1}", functionInfo.getInputTypes().get(i), "args" + i))
                        .collect(Collectors.joining(", ")));

        builder.setUserFuncFileName(functionInfo.getUserFuncFileName());

        builder.setInputData(
                IntStream.range(0, functionInfo.getInputTypes().size())
                        .mapToObj(i -> "args" + i)
                        .collect(Collectors.joining(", ")));
        builder.setFunctionName(functionInfo.getName());
        builder.setJepPath(functionInfo.getJepPath());
        builder.pythonScriptPath(functionInfo.getPythonScriptPath());
        builder.setCpythonPackages(functionInfo.getCpythonPackages());
        return builder.create();
    }

    /**
     * format java class name
     *
     * @param name eg. test_udf
     * @return eg. FlinkPyTestUdf
     */
    private String getClassName(String name) {
        String standardizedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name);
        return "FlinkPy" + standardizedName;
    }

}
