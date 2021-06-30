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

package com.tencent.bk.base.datalab.bksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class TypeInferenceUtil {

    private static final Map<Set<DataType>, DataType> ARITHMETIC_ADDITION_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.INTEGER, DataType.INTEGER), DataType.INTEGER)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.LONG), DataType.LONG)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.FLOAT), DataType.FLOAT)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.LONG, DataType.LONG), DataType.LONG)
            .put(ImmutableSet.of(DataType.LONG, DataType.FLOAT), DataType.FLOAT)
            .put(ImmutableSet.of(DataType.LONG, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.FLOAT), DataType.FLOAT)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.DOUBLE, DataType.DOUBLE), DataType.DOUBLE)
            .build();
    private static final Map<Set<DataType>, DataType> TEXT_ADDITION_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.STRING, DataType.STRING), DataType.STRING)
            .build();
    private static final Map<Set<DataType>, DataType> ARITHMETIC_DIVISION_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.INTEGER, DataType.INTEGER), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.LONG), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.FLOAT), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.LONG, DataType.LONG), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.LONG, DataType.FLOAT), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.LONG, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.FLOAT), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.DOUBLE, DataType.DOUBLE), DataType.DOUBLE)
            .build();
    private static final Map<Set<DataType>, DataType> ARITHMETIC_MULTIPLICATION_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.LONG, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.LONG, DataType.LONG), DataType.LONG)
            .put(ImmutableSet.of(DataType.LONG, DataType.FLOAT), DataType.FLOAT)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.FLOAT), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.DOUBLE, DataType.DOUBLE), DataType.DOUBLE)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.INTEGER), DataType.LONG)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.LONG), DataType.LONG)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.FLOAT), DataType.FLOAT)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.DOUBLE), DataType.DOUBLE)
            .build();
    private static final Map<Set<DataType>, DataType> ARITHMETIC_MODULO_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.INTEGER, DataType.INTEGER), DataType.INTEGER)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.LONG), DataType.LONG)
            .put(ImmutableSet.of(DataType.LONG, DataType.LONG), DataType.LONG)
            .build();
    private static final Map<Set<DataType>, DataType> EQUALS_TO_RULE = ImmutableMap
            .<Set<DataType>, DataType>builder()
            .put(ImmutableSet.of(DataType.INTEGER, DataType.INTEGER), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.LONG), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.FLOAT), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.INTEGER, DataType.DOUBLE), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.LONG, DataType.LONG), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.LONG, DataType.FLOAT), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.LONG, DataType.DOUBLE), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.FLOAT), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.FLOAT, DataType.DOUBLE), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.DOUBLE, DataType.DOUBLE), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.STRING, DataType.STRING), DataType.BOOLEAN)
            .put(ImmutableSet.of(DataType.BOOLEAN, DataType.BOOLEAN), DataType.BOOLEAN)
            .build();

    private TypeInferenceUtil() {

    }

    @SafeVarargs
    private static DataType inferOutputType(DataType type, DataType type2,
            Map<Set<DataType>, DataType>... rules) {
        Set<DataType> inputs = ImmutableSet.of(type, type2);
        Map<Set<DataType>, DataType> ruleMap = Arrays.stream(rules)
                .flatMap(rule -> rule.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (ruleMap.containsKey(inputs)) {
            return ruleMap.get(inputs);
        }
        throw new IllegalArgumentException(
                "no output type found for inputs: " + type + " | " + type2);
    }

    public static DataType inferAddition(DataType type, DataType type2) {
        return inferOutputType(type, type2, ARITHMETIC_ADDITION_RULE, TEXT_ADDITION_RULE);
    }

    public static DataType inferDivision(DataType type, DataType type2) {
        return inferOutputType(type, type2, ARITHMETIC_DIVISION_RULE);
    }

    public static DataType inferMultiplication(DataType type, DataType type2) {
        return inferOutputType(type, type2, ARITHMETIC_MULTIPLICATION_RULE);
    }

    public static DataType inferEqualsTo(DataType type, DataType type2) {
        return inferOutputType(type, type2, EQUALS_TO_RULE);
    }

    public static DataType inferTextAddition(DataType type, DataType type2) {
        return inferOutputType(type, type2, TEXT_ADDITION_RULE);
    }

    public static DataType inferModulo(DataType type, DataType type2) {
        return inferOutputType(type, type2, ARITHMETIC_MODULO_RULE);
    }
}
