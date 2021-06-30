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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public final class ReflectUtil {

    private ReflectUtil() {

    }

    public static <T> T injectPropertiesAndCreate(JsonNode config, Class<T> clazz,
            Config properties) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper
                .setInjectableValues(new InjectableValues.Std().addValue("properties", properties));
        try {
            T t = objectMapper.readValue(new TreeTraversingParser(config), clazz);
            Preconditions.checkNotNull(t,
                    "error instantiating clazz " + clazz.getSimpleName() + " within config: "
                            + config);
            return t;
        } catch (IOException e) {
            throw new RuntimeException("error deserializing json node: " + config, e);
        }
    }

    /**
     * 修改 final 成员变量
     *
     * @param clazz Class实例
     * @param filedName 成员变量名
     * @param target 目标对象
     * @param value 修改后的值
     */
    public static void modifyFinalField(Class<?> clazz, String filedName, Object target,
            Object value) {
        try {
            Field field = clazz.getDeclaredField(filedName);
            field.setAccessible(true);
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(target, value);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Error modify final filed: NoSuchFieldException", e);
        } catch (SecurityException e) {
            throw new RuntimeException("Error modify final filed: SecurityException", e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error modify final filed: IllegalArgumentException", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error modify final filed: IllegalAccessException", e);
        }
    }

    /**
     * 修改 static 成员变量
     *
     * @param clazz Class实例
     * @param filedName 成员变量名
     * @param value 修改后的值
     */
    public static void modifyStaticFinalField(Class<?> clazz, String filedName, Object value) {
        try {
            Field field = clazz.getDeclaredField(filedName);
            field.setAccessible(true);
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, value);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Error modify static final filed: NoSuchFieldException", e);
        } catch (SecurityException e) {
            throw new RuntimeException("Error modify static final filed: SecurityException", e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error modify static final filed: IllegalArgumentException",
                    e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error modify static final filed: IllegalAccessException",
                    e);
        }
    }


}
