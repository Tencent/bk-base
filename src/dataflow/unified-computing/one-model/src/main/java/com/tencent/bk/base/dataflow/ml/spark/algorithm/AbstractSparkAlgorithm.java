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

package com.tencent.bk.base.dataflow.ml.spark.algorithm;

import com.google.common.base.CaseFormat;
import com.tencent.bk.base.dataflow.ml.algorithm.AbstractAlgorithm;
import com.tencent.bk.base.dataflow.ml.exceptions.ParamNotFoundModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.ParamSetModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.ParamTypeNotSupportModelException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSparkAlgorithm extends AbstractAlgorithm implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSparkAlgorithm.class);

    protected void invokeMethod(Method method, Class<?> parameterType, Params algorithm, Object value)
            throws IllegalAccessException, InvocationTargetException {
        if (parameterType.isAssignableFrom(String.class)) {
            method.invoke(algorithm, value.toString());
        } else if (parameterType.isAssignableFrom(Integer.class) || parameterType
                .isAssignableFrom(int.class)) {
            method.invoke(algorithm, Integer.valueOf(value.toString()));
        } else if (parameterType.isAssignableFrom(Double.class) || parameterType
                .isAssignableFrom(double.class)) {
            method.invoke(algorithm, Double.valueOf(value.toString()));
        } else if (parameterType.isAssignableFrom(Float.class) || parameterType
                .isAssignableFrom(float.class)) {
            method.invoke(algorithm, Float.valueOf(value.toString()));
        } else if (parameterType.isAssignableFrom(Long.class) || parameterType
                .isAssignableFrom(long.class)) {
            method.invoke(algorithm, Long.valueOf(value.toString()));
        } else if (parameterType.isAssignableFrom(Boolean.class) || parameterType
                .isAssignableFrom(boolean.class)) {
            method.invoke(algorithm, Boolean.valueOf(value.toString()));
        } else if (parameterType.isAssignableFrom(String[].class)) {
            Object[] valueArray = ((List<Object>) value).stream().toArray();
            Object[] arrayParam = {Arrays.copyOf(valueArray, valueArray.length, String[].class)};
            method.invoke(algorithm, arrayParam);
        } else if (parameterType.isAssignableFrom(Integer[].class) || parameterType
                .isAssignableFrom(int[].class)) {
            int[] ints = ((List<Object>) value).stream()
                    .mapToInt(x -> Integer.parseInt(x.toString()))
                    .toArray();
            Object[] arrayParam = {ints};
            method.invoke(algorithm, arrayParam);
        } else if (parameterType.isAssignableFrom(Double[].class) || parameterType
                .isAssignableFrom(double[].class)) {
            double[] doubles = ((List<Object>) value).stream()
                    .mapToDouble(x -> Double.parseDouble(x.toString()))
                    .toArray();
            Object[] arrayParam = {doubles};
            method.invoke(algorithm, arrayParam);
        } else if (parameterType.isAssignableFrom(Double[][].class) || parameterType
                .isAssignableFrom(double[][].class)) {
            double[][] doubleValues = ((List<List<Object>>) value).stream()
                    .map(x -> x.stream()
                            .mapToDouble(y -> Double.parseDouble(y.toString()))
                            .toArray())
                    .toArray(double[][]::new);
            Object[] arrayParam = {doubleValues};
            method.invoke(algorithm, arrayParam);
        } else if (parameterType.isAssignableFrom(Vector.class)) {
            Vector dense = Vectors.dense(((List<Object>) value).stream()
                    .mapToDouble(x -> Double.parseDouble(x.toString()))
                    .toArray());
            method.invoke(algorithm, dense);
        } else {
            throw new ParamTypeNotSupportModelException(parameterType.toString());
        }
    }

    protected void configureAlgorithm(Params algorithm, Map<String, Object> params) {
        String simpleName = algorithm.getClass().getSimpleName();
        // 将参数的 key 下划线格式转为驼峰格式，如 input_col -> inputCol
        Map<String, Object> convertParams = params.entrySet()
                .stream()
                .collect(Collectors.toMap(x ->
                                CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, x.getKey()),
                        Map.Entry::getValue));
        Arrays.stream(algorithm.params()).forEach(f -> {
            if (convertParams.containsKey(f.name())) {
                Object value = convertParams.get(f.name());
                List<Method> methodList = Arrays.stream(algorithm.getClass().getMethods())
                        .filter(m -> m.getName().equals("set" + StringUtils.capitalize(f.name())))
                        .collect(Collectors.toList());
                if (methodList.size() == 0) {
                    throw new ParamNotFoundModelException(f.name(), simpleName);
                    /*throw new RuntimeException(MessageFormat.format("Not found the parameter {0} in {1}",
                            f.name(), simpleName));*/
                }
                Method method = methodList.get(0);
                Class<?> parameterType = method.getParameterTypes()[0];
                try {
                    invokeMethod(method, parameterType, algorithm, value);
                } catch (IllegalAccessException e) {
                    LOGGER.error(MessageFormat
                            .format("Failed to set parameter {0} in {1}, because illegal access exception.", f.name(),
                                    simpleName), e);
                    throw new ParamSetModelException(f.name(), simpleName, e);
                } catch (InvocationTargetException e1) {
                    LOGGER.error(MessageFormat
                            .format("Failed to set parameter {0} in {1}, because invocation target exception.",
                                    f.name(), simpleName), e1);
                    throw new ParamSetModelException(f.name(), simpleName, e1);
                }
            }
        });
    }
}
