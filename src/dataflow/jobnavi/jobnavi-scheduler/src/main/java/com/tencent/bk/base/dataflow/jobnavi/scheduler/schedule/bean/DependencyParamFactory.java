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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class DependencyParamFactory {

    private static final Map<DependencyParamType, Class<? extends AbstractDependencyParam>>
            paramClassMap = new HashMap<>();

    static {
        register(DependencyParamType.fixed, FixedDependencyParam.class);
        register(DependencyParamType.range, RangeDependencyParam.class);
        register(DependencyParamType.accumulate, AccumulateDependencyParam.class);
    }

    private static void register(DependencyParamType paramType, Class<? extends AbstractDependencyParam> paramClass) {
        paramClassMap.put(paramType, paramClass);
    }

    public static AbstractDependencyParam parseParam(DependencyParamType paramType, String paramStr, long baseTimeMills,
            TimeZone timeZone) {
        if (paramClassMap.containsKey(paramType)) {
            Class<? extends AbstractDependencyParam> paramClass = paramClassMap.get(paramType);
            try {
                Constructor<? extends AbstractDependencyParam> constructor = paramClass
                        .getConstructor(String.class, Long.TYPE, TimeZone.class);
                return constructor.newInstance(paramStr, baseTimeMills, timeZone);
            } catch (NoSuchMethodException
                    | IllegalAccessException
                    | InstantiationException
                    | InvocationTargetException e) {
                throw new IllegalArgumentException(
                        "Invalid dependency param:" + paramStr + " for dependency param type:" + paramType);
            }
        }
        throw new UnsupportedOperationException("DependencyParamType:" + paramType + " not support yet.");
    }

    public static AbstractDependencyParam parseParam(DependencyParamType paramType, String paramStr, long baseTimeMills,
            TimeZone timeZone, Period parentPeriod) {
        if (paramClassMap.containsKey(paramType)) {
            Class<? extends AbstractDependencyParam> paramClass = paramClassMap.get(paramType);
            try {
                Constructor<? extends AbstractDependencyParam> constructor = paramClass
                        .getConstructor(String.class, Long.TYPE, TimeZone.class, Period.class);
                return constructor.newInstance(paramStr, baseTimeMills, timeZone, parentPeriod);
            } catch (NoSuchMethodException
                    | IllegalAccessException
                    | InstantiationException
                    | InvocationTargetException e) {
                throw new IllegalArgumentException(
                        "Invalid dependency param:" + paramStr + " for dependency param type:" + paramType, e);
            }
        }
        throw new UnsupportedOperationException("DependencyParamType:" + paramType + " not support yet.");
    }
}
