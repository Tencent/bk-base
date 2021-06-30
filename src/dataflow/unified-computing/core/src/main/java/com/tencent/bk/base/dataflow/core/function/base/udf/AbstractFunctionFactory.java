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

package com.tencent.bk.base.dataflow.core.function.base.udf;


import com.google.common.collect.Sets;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.RegisterException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.time.FastDateFormat;

public abstract class AbstractFunctionFactory<T> {

    public static Map<String, FastDateFormat> DT_FORMAT_POOL = new HashMap<String, FastDateFormat>();

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

    public static final Map<String, Integer> TRANS_TO_MILLISEC = new HashMap<String, Integer>() {{
        put("day", 24 * 60 * 60 * 1000);
        put("hour", 60 * 60 * 1000);
        put("minute", 60 * 1000);
        put("second", 1000);
        put("millisec", 1);
    }};

    public String timeZone;

    // 每种计算引擎注册自己实现的udf
    public Map<String, Class<? extends Serializable>> functions = new HashMap<>();
    // 依赖timeZone实现的功能的udf
    public Map<String, Class<? extends Serializable>> timeZoneFunctions = new HashMap<>();

    public boolean isFunction(final String functionName) {
        return functions.containsKey(functionName) || timeZoneFunctions.containsKey(functionName);
    }

    public abstract void register(String transformerName) throws RegisterException;

    public void registerAll() {
        Set<String> functionKeys = Sets.newHashSet();
        functionKeys.addAll(functions.keySet());
        functionKeys.addAll(timeZoneFunctions.keySet());
        functionKeys.forEach(key -> {
            try {
                this.register(key);
            } catch (RegisterException e) {
                e.printStackTrace();
            }
        });
    }

    protected T buildUDF(String transformerName) throws RegisterException {
        if (transformerName == null) {
            throw new RegisterException("transformerName为空.");
        }
        if (!isFunction(transformerName)) {
            throw new RegisterException(String.format("当前引擎未注册%s方法.", transformerName));
        }
        Class transformerClass = null;
        T udfInstance = null;
        try {
            if (functions.containsKey(transformerName)) {
                transformerClass = functions.get(transformerName);
                udfInstance = (T) transformerClass.getConstructor().newInstance();
            } else if (timeZoneFunctions.containsKey(transformerName)) {
                transformerClass = timeZoneFunctions.get(transformerName);
                udfInstance = (T) transformerClass.getConstructor(String.class).newInstance(timeZone);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return udfInstance;
    }
}
