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
import com.tencent.bk.base.datalab.bksql.annotation.DataTypeDesc;
import com.tencent.bk.base.datalab.bksql.func.BkType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.reflections.Reflections;

@Slf4j
public class TypeFactory {

    private static Map<String, List<Map<String, RelProtoDataType>>> bkTypeMap = Maps
            .newConcurrentMap();
    private static Reflections typeClasses = ReflectionUtil
            .getReflections("com.tencent.bk.base.datalab.queryengine.type", ".*.*\\.class");

    /**
     * 初始化自定义类型库
     */
    public static void initRepository() {
        Set<Class<? extends BkType>> classes = typeClasses.getSubTypesOf(BkType.class);
        Set<Class<? extends BkType>> filteredClasses = Sets.newHashSet();
        for (Class<? extends BkType> c : classes) {
            if (c.isAnnotationPresent(DataTypeDesc.class)) {
                filteredClasses.add(c);
            }
        }

        for (Class<? extends BkType> fc : filteredClasses) {
            try {
                DataTypeDesc dataTypeDesc = fc.getAnnotation(DataTypeDesc.class);
                String typeName = dataTypeDesc.typeName();
                String sqlType = dataTypeDesc.sqlType();
                List<Map<String, RelProtoDataType>> typeList = bkTypeMap.get(sqlType);
                if (typeList == null) {
                    typeList = Lists.newArrayList();
                    bkTypeMap.putIfAbsent(sqlType, typeList);
                }
                Object typeObject = fc.newInstance();
                Method method = fc.getMethod("getDataType");
                Object result = method.invoke(typeObject);
                HashMap<String, RelProtoDataType> typeHashMap = Maps.newHashMap();
                typeHashMap.putIfAbsent(typeName, (RelProtoDataType) result);
                typeList.add(typeHashMap);
                log.info("Register BkType sqlType:{} typeName:{}", sqlType,
                        dataTypeDesc.typeName());
            } catch (InstantiationException e) {
                log.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                log.error(e.getMessage(), e);
            } catch (NoSuchMethodException e) {
                log.error(e.getMessage(), e);
            } catch (InvocationTargetException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 根据 sqlType 获取自定义类型列表
     *
     * @param sqlType 存储类别
     * @return 指定存储的类型库
     */
    public static List<Map<String, RelProtoDataType>> getRepository(String sqlType) {
        Preconditions.checkNotNull(sqlType, "Failed to resolve types caused by sqlType is null!");
        List<Map<String, RelProtoDataType>> result = bkTypeMap.get(sqlType);
        return result;
    }
}
