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

import com.tencent.bk.base.dataflow.core.function.base.udtf.AbstractUdtf;
import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.template.HiveJavaUdtfTemplate;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HiveUdtfForJavaGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        FunctionContext context = createFunctionContext(functionInfo);
        String code = new HiveJavaUdtfTemplate().generateFuncCode(context);

        String hiveClass = getFunctionSimpleClass(UdfUtils.getUdtfInstance(functionInfo.getUserFuncFileName()));
        String javaFile = MessageFormat.format("{0}/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.HIVE_UDF_DIRECTORY, hiveClass).replace(".", "/")
                + ".java";
        UdfUtils.createUdfFile(javaFile, code);
    }

    private FunctionContext createFunctionContext(FunctionInfo functionInfo) {
        AbstractUdtf udtf = UdfUtils.getUdtfInstance(functionInfo.getUserFuncFileName());
        // get all method in udtf class
        Method[] methods = udtf.getClass().getMethods();
        Arrays.sort(methods, (method1, method2) -> {
            int parameterDiff = method1.getParameterCount() - method2.getParameterCount();
            if (0 == parameterDiff) {
                return Arrays.toString(method1.getParameters()).compareTo(Arrays.toString(method2.getParameters()));
            } else {
                return parameterDiff;
            }
        });

        // imports
        FunctionContext.Builder builder = FunctionContext.builder();
        String imports = "import " + udtf.getClass().getName() + ";";
        builder.imports(imports);
        // userFuncClass
        builder.setUserClass(udtf.getClass().getSimpleName());
        // simpleClassName
        builder.setUdfClassName(getFunctionSimpleClass(udtf));
        // get input args info
        for (Method method : methods) {
            if (("call".equalsIgnoreCase(method.getName()))
                    && "public".equalsIgnoreCase(Modifier.toString(method.getModifiers()))) {
                Parameter[] parameters = method.getParameters();
                StringBuilder funcInputParams = new StringBuilder();
                StringBuilder funcInputData = new StringBuilder();
                List<String> funcInputTypes = new ArrayList<>();
                for (Parameter parameter : parameters) {
                    String paraType = parameter.getType().getName();
                    String paramName = parameter.getName();
                    funcInputParams.append(paraType).append(" ").append(paramName).append(", ");
                    funcInputData.append(paramName).append(", ");
                    funcInputTypes.add(paraType);
                }
                builder.setInputParams(funcInputParams.substring(0, funcInputParams.length() - 2));
                builder.setInputData(funcInputData.substring(0, funcInputData.length() - 2));
                builder.setTypeParameters(
                        IntStream.range(0, funcInputTypes.size())
                                .mapToObj(i -> {
                                    switch (funcInputTypes.get(i).replace("java.lang.", "")) {
                                        case "String":
                                            return MessageFormat.format("args[{0}].toString()", i);
                                        case "Integer":
                                            return MessageFormat.format("Integer.valueOf(args[{0}].toString())", i);
                                        case "Long":
                                            return MessageFormat.format("Long.valueOf(args[{0}].toString())", i);
                                        case "Float":
                                            return MessageFormat.format("Float.valueOf(args[{0}].toString())", i);
                                        case "Double":
                                            return MessageFormat.format("Double.valueOf(args[{0}].toString())", i);
                                        default:
                                            throw new RuntimeException(
                                                    "Not support input parameter " + funcInputTypes.get(i));
                                    }
                                })
                                .collect(Collectors.joining(", ")));
            }
        }

        builder.setReturnDataTypes(functionInfo.getOutputTypes().stream().collect(Collectors.joining(", ")));
        return builder.create();
    }

    private String getFunctionSimpleClass(AbstractUdtf udtf) {
        return "HiveJava" + udtf.getClass().getSimpleName();
    }
}
