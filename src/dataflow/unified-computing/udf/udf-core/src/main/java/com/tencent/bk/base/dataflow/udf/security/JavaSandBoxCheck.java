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

package com.tencent.bk.base.dataflow.udf.security;

import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractUdaf;
import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractUdf;
import com.tencent.bk.base.dataflow.core.function.base.udtf.AbstractUdtf;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class JavaSandBoxCheck {

    /**
     * Do security check
     *
     * @param args The first args is class name, The second args is input params's type,the other args is the
     *         udf's params.
     */
    public static void main(String[] args) {
        securityCheck(args);
    }

    private static Object[] getParams(String[] args, List<String> typeOfArgs) {
        return IntStream.range(0, args.length)
                .filter(i -> i != 0 && i != 1)
                .mapToObj(i -> {
                    switch (typeOfArgs.get(i - 2).toLowerCase()) {
                        case "string":
                            return args[i];
                        case "integer":
                        case "int":
                            return Integer.valueOf(args[i]);
                        case "long":
                            return Long.valueOf(args[i]);
                        case "float":
                            return Float.valueOf(args[i]);
                        case "double":
                            return Double.valueOf(args[i]);
                        default:
                            throw new RuntimeException("Not support the type " + typeOfArgs.get(i - 2));
                    }
                }).toArray(Object[]::new);
    }

    private static void securityCheck(String[] args) {
        try {
            String className = args[0];
            List<String> typeOfArgs = Arrays.asList(args[1].split(","));

            Object[] params = getParams(args, typeOfArgs);

            Object udf = Class
                    .forName(MessageFormat.format("com.tencent.bk.base.dataflow.udf.functions.{0}", className))
                    .newInstance();
            if (udf instanceof AbstractUdf || udf instanceof AbstractUdtf) {
                for (Method method : udf.getClass().getMethods()) {
                    if ("call".equals(method.getName())
                            && "public".equals(Modifier.toString(method.getModifiers()))) {
                        method.invoke(udf, params);
                    }
                }
            } else if (udf instanceof AbstractUdaf) {
                checkUdaf(udf, params);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkUdaf(Object udaf, Object[] params)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Method createAccumulator = udaf.getClass().getDeclaredMethod("createAccumulator");

        Object createAccumulatorResult1 = createAccumulator.invoke(udaf);
        Object createAccumulatorResult2 = createAccumulator.invoke(udaf);

        // invoke methods
        for (Method method : udaf.getClass().getMethods()) {
            if ("accumulate".equals(method.getName())
                    && "public".equalsIgnoreCase(Modifier.toString(method.getModifiers()))) {

                List<Object> thisParams = new ArrayList<>();
                thisParams.add(createAccumulatorResult1);
                thisParams.addAll(Arrays.asList(params));

                method.invoke(udaf, thisParams.stream().toArray(Object[]::new));
            } else if ("merge".equals(method.getName())
                    && "public".equals(Modifier.toString(method.getModifiers()))) {
                // invoke merge method
                method.invoke(udaf, new Object[]{createAccumulatorResult1, createAccumulatorResult2});
            } else if ("resetAccumulator".equals(method.getName())
                    && "public".equals(Modifier.toString(method.getModifiers()))) {
                // invoke resetAccumulator method
                method.invoke(udaf, createAccumulatorResult2);
            } else if ("getValue".equals(method.getName())
                    && "public".equals(Modifier.toString(method.getModifiers()))) {
                method.invoke(udaf, createAccumulatorResult1);
            }
        }
    }
}
