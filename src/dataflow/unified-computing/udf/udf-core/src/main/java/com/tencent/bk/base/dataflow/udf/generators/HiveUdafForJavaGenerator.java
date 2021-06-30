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

import com.tencent.bk.base.dataflow.udf.common.FunctionContext;
import com.tencent.bk.base.dataflow.udf.common.FunctionInfo;
import com.tencent.bk.base.dataflow.udf.util.UdfUtils;
import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractUdaf;
import com.tencent.bk.base.dataflow.udf.template.HiveJavaUdafTemplate;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

public class HiveUdafForJavaGenerator implements FunctionGenerator {

    @Override
    public void generate(FunctionInfo functionInfo) {
        FunctionContext context = createFunctionContext(functionInfo);
        String code = new HiveJavaUdafTemplate().generateFuncCode(context);

        String hiveClass = getFunctionSimpleClass(UdfUtils.getUdafInstance(functionInfo.getUserFuncFileName()));
        String javaFile = MessageFormat.format("{0}/src/main/java", System.getProperty("user.dir")) + "/"
                + MessageFormat.format("{0}.{1}", UdfUtils.HIVE_UDF_DIRECTORY, hiveClass).replace(".", "/")
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
                builder.setReturnType(method.getReturnType().getName());
                //
                Parameter[] parameters = method.getParameters();
                if (parameters.length != 1) {
                    throw new RuntimeException("The method getValue must have one parameter.");
                }
                for (Parameter parameter : parameters) {
                    List<String> actualTypeArguments = null;
                    Type typeArguments = parameter.getParameterizedType();
                    if (typeArguments instanceof ParameterizedTypeImpl) {
                        Type[] actualTypeArguments1 = ((ParameterizedTypeImpl) typeArguments).getActualTypeArguments();

                        actualTypeArguments = Arrays
                                .asList(((ParameterizedTypeImpl) typeArguments).getActualTypeArguments())
                                .stream()
                                .map((Function<Type, String>) Type::getTypeName).collect(Collectors.toList());
                    }
                    if (actualTypeArguments != null && actualTypeArguments.size() > 0) {
                        builder.setStateClass(MessageFormat.format("{0}<{1}>", parameter.getType().getName(),
                                String.join(", ", actualTypeArguments)));
                    } else {
                        builder.setStateClass(parameter.getType().getName());
                    }
                }
            }
        }

        // set type parameters
        builder.setTypeParameters(
                IntStream.range(0, task.getInputTypes().size())
                        .mapToObj(i -> {
                            switch (task.getInputTypes().get(i).replace("java.lang.", "")) {
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
                                            "Not support input parameter " + task.getInputTypes().get(i));
                            }
                        }).collect(Collectors.joining(", ")));

        // userFuncClass
        builder.setUserClass(udaf.getClass().getSimpleName());
        // simpleClassName
        builder.setUdfClassName(getFunctionSimpleClass(udaf));

        return builder.create();
    }

    private String getFunctionSimpleClass(AbstractUdaf udtf) {
        return "HiveJava" + udtf.getClass().getSimpleName();
    }
}
