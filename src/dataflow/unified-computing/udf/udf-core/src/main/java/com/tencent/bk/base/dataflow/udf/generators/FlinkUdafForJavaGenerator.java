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

import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractUdaf;
import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.template.FlinkJavaUdafTemplate;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.text.MessageFormat;

public class FlinkUdafForJavaGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        FunctionContext context = createFunctionContext(functionInfo);
        String code = new FlinkJavaUdafTemplate().generateFuncCode(context);

        String flinkClass = getFunctionSimpleClass(UdfUtils.getUdafInstance(functionInfo.getUserFuncFileName()));
        String javaFile = MessageFormat.format("{0}/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.FLINK_UDF_DIRECTORY, flinkClass).replace(".", "/")
                + ".java";
        UdfUtils.createUdfFile(javaFile, code);
    }

    private FunctionContext createFunctionContext(FunctionInfo task) {
        AbstractUdaf udaf = UdfUtils.getUdafInstance(task.getUserFuncFileName());

        FunctionContext.Builder builder = FunctionContext.builder();

        // imports
        String imports = "import " + udaf.getClass().getName() + ";";
        builder.imports(imports);

        // get state class and return type
        for (Method method : udaf.getClass().getMethods()) {
            if ("getValue".equals(method.getName())
                    && "public".equalsIgnoreCase(Modifier.toString(method.getModifiers()))) {
                // set return type
                builder.setReturnType(method.getReturnType().getName().replace("java.lang.", ""));
                //
                Parameter[] parameters = method.getParameters();
                if (parameters.length != 1) {
                    throw new RuntimeException("The method getValue must have one parameter.");
                }
                for (Parameter parameter : parameters) {
                    builder.setStateClass(parameter.getType().getName()
                            .replace("com.tencent.bk.base.dataflow.udf.functions.", "")
                            .replace("$", "."));
                }
            }

            if ("accumulate".equals(method.getName())
                    && "public".equalsIgnoreCase(Modifier.toString(method.getModifiers()))) {
                Parameter[] parameters = method.getParameters();
                StringBuilder funcInputParams = new StringBuilder();
                StringBuilder funcInputData = new StringBuilder();
                for (int i = 0; i < parameters.length; i++) {
                    if (i == 0) {
                        continue;
                    }
                    String paraType = parameters[i].getType().getName();
                    String paramName = parameters[i].getName();
                    funcInputParams.append(paraType).append(" ").append(paramName).append(", ");
                    funcInputData.append(paramName).append(", ");
                }
                builder.setInputParams(funcInputParams.substring(0, funcInputParams.length() - 2));
                builder.setInputData(funcInputData.substring(0, funcInputData.length() - 2));
            }
        }

        // userFuncClass
        builder.setUserClass(udaf.getClass().getSimpleName());
        // simpleClassName
        builder.setUdfClassName(getFunctionSimpleClass(udaf));
        return builder.create();
    }

    private String getFunctionSimpleClass(AbstractUdaf udtf) {
        return "FlinkJava" + udtf.getClass().getSimpleName();
    }
}
