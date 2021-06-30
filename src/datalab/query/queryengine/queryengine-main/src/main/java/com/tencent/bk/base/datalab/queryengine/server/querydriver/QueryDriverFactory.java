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

package com.tencent.bk.base.datalab.queryengine.server.querydriver;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.server.util.ReflectionUtil;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

@Slf4j
public class QueryDriverFactory {

    private static final Map<String, QueryDriver> driverMap = Maps.newHashMap();
    private static final Reflections driverClasses = ReflectionUtil
            .getReflections("com.tencent.bk.base.datalab.queryengine.server.querydriver", ".*.*\\.class");

    /**
     * 初始化 driverMap
     */
    public static void initDriverMap() {
        Set<Class<? extends QueryDriver>> classes = driverClasses.getSubTypesOf(QueryDriver.class);
        Set<Class<? extends QueryDriver>> filteredClasses = Sets.newHashSet();
        for (Class<? extends QueryDriver> clazz : classes) {
            if (clazz.isAnnotationPresent(QueryDriverDesc.class)) {
                filteredClasses.add(clazz);
            }
        }
        for (Class<? extends QueryDriver> clazz : filteredClasses) {
            try {
                QueryDriverDesc queryDriverDesc = clazz.getAnnotation(QueryDriverDesc.class);
                String driverName = queryDriverDesc.name();
                checkNotDuplicated(driverName);
                QueryDriver instance = clazz.newInstance();
                driverMap.put(driverName, instance);
                log.info("Register QueryDriver driverName:{}", driverName);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Failed to create instance for class: " + clazz, e);
            }
        }
    }

    private static void checkNotDuplicated(String driverName) {
        if (driverMap.containsKey(driverName)) {
            throw new RuntimeException("QueryDriver " + driverName + " already exists");
        }
    }

    /**
     * 简单工厂获取 QueryDriver
     *
     * @param pickedStorage 存储引擎
     * @return 返回 QueryDriver 实例
     */
    public static QueryDriver getQueryDriver(String pickedStorage) {
        Preconditions.checkNotNull(pickedStorage, "pickedStorage can not be null");
        return driverMap.get(pickedStorage);
    }
}
