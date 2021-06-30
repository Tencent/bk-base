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

package com.tencent.bk.base.datalab.queryengine.server.util;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.annotation.FunctionDesc;
import com.tencent.bk.base.datalab.bksql.func.BkFunction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.reflections.Reflections;

@Slf4j
public class FunctionFactory {

    private static Map<String, List<Map<String, ScalarFunction>>> bkFunctionMap = Maps
            .newConcurrentMap();
    private static Reflections functionClasses = ReflectionUtil
            .getReflections("com.tencent.bk.base.datalab.queryengine.func", ".*.*\\.class");

    /**
     * 初始化自定义函数库
     */
    public static void initRepository() {
        Set<Class<? extends BkFunction>> classes = functionClasses.getSubTypesOf(BkFunction.class);
        Set<Class<? extends BkFunction>> filteredClasses = Sets.newHashSet();
        for (Class<? extends BkFunction> c : classes) {
            if (c.isAnnotationPresent(FunctionDesc.class)) {
                filteredClasses.add(c);
            }
        }
        for (Class<? extends BkFunction> fc : filteredClasses) {
            FunctionDesc functionDesc = fc.getAnnotation(FunctionDesc.class);
            String functionName = functionDesc.functionName();
            String sqlType = functionDesc.sqlType();
            List<Map<String, ScalarFunction>> functionList = bkFunctionMap.get(sqlType);
            if (functionList == null) {
                functionList = Lists.newArrayList();
                bkFunctionMap.putIfAbsent(sqlType, functionList);
            }
            HashMap<String, ScalarFunction> functionNameMap = Maps.newHashMap();
            functionNameMap.putIfAbsent(functionName, ScalarFunctionImpl.create(fc, functionName));
            functionList.add(functionNameMap);
            log.info("Register BkFunction sqlType:{} functionType:{} functionName:{}", sqlType,
                    functionDesc.functionType(), functionName);
        }
    }

    /**
     * 根据 sqlType 获取函数列表
     *
     * @param sqlType 存储类别
     * @return 指定存储的函数库
     */
    public static List<Map<String, ScalarFunction>> getRepository(String sqlType) {
        Preconditions
                .checkNotNull(sqlType, "Failed to resolve functions caused by sqlType is null!");
        List<Map<String, ScalarFunction>> result = bkFunctionMap.get(sqlType);
        return result;
    }

}
