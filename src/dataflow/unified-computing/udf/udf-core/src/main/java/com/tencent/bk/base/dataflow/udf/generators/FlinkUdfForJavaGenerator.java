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

import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractUdf;
import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.template.FlinkJavaUdfTemplate;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.text.MessageFormat;
import java.util.Arrays;

public class FlinkUdfForJavaGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        AbstractUdf udf = UdfUtils.getUdfInstance(functionInfo.getUserFuncFileName());

        String code = generatorCode(udf);
        String flinkClass = getFunctionSimpleClass(udf);
        String javaFile = String.format("%s/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.FLINK_UDF_DIRECTORY, flinkClass).replace(".", "/")
                + ".java";
        UdfUtils.createUdfFile(javaFile, code);
    }

    private FunctionContext createFunctionContext(AbstractUdf udf) {
        // get all method in udf class
        Method[] methods = udf.getClass().getMethods();
        Arrays.sort(methods, (method1, method2) -> {
            int parameterDiff = method1.getParameterCount() - method2.getParameterCount();
            if (0 == parameterDiff) {
                return Arrays.toString(method1.getParameters()).compareTo(Arrays.toString(method2.getParameters()));
            } else {
                return parameterDiff;
            }
        });

        FunctionContext.Builder builder = FunctionContext.builder();
        // imports
        String imports = "import " + udf.getClass().getName() + ";";
        builder.imports(imports);
        // userFuncClass
        builder.setUserClass(udf.getClass().getSimpleName());
        // simpleClassName
        builder.setUdfClassName(getFunctionSimpleClass(udf));
        //
        for (Method method : methods) {
            if ("call".equalsIgnoreCase(method.getName())
                    && "public".equalsIgnoreCase(Modifier.toString(method.getModifiers()))) {
                builder.setReturnType(method.getReturnType().getName());
                // 获取方法的所有参数
                Parameter[] parameters = method.getParameters();
                StringBuilder funcInputParams = new StringBuilder();
                StringBuilder funcInputData = new StringBuilder();
                for (Parameter parameter : parameters) {
                    String paraType = parameter.getType().getName();
                    String paramName = parameter.getName();
                    funcInputParams.append(paraType).append(" ").append(paramName).append(", ");
                    funcInputData.append(paramName).append(", ");
                }
                builder.setInputParams(funcInputParams.substring(0, funcInputParams.length() - 2));
                builder.setInputData(funcInputData.substring(0, funcInputData.length() - 2));
            }
        }
        return builder.create();
    }

    private String getFunctionSimpleClass(AbstractUdf udf) {
        return "FlinkJava" + udf.getClass().getSimpleName();
    }

    private String generatorCode(AbstractUdf udf) {
        FunctionContext context = createFunctionContext(udf);
        return new FlinkJavaUdfTemplate().generateFuncCode(context);
    }
}
