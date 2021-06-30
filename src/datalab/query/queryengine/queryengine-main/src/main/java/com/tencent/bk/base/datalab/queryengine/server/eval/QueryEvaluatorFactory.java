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

package com.tencent.bk.base.datalab.queryengine.server.eval;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.server.util.ReflectionUtil;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

@Slf4j
public class QueryEvaluatorFactory {

    private static final Map<String, QueryEvaluator> evaluatorMap = Maps.newHashMap();
    private static final Reflections evaluatorClasses = ReflectionUtil
            .getReflections("com.tencent.bk.base.datalab.queryengine.server.eval", ".*.*\\.class");

    /**
     * 初始化 evaluatorMap
     */
    public static void initEvaluatorMap() {
        Set<Class<? extends QueryEvaluator>> classes = evaluatorClasses.getSubTypesOf(QueryEvaluator.class);
        Set<Class<? extends QueryEvaluator>> filteredClasses = Sets.newHashSet();
        for (Class<? extends QueryEvaluator> clazz : classes) {
            if (clazz.isAnnotationPresent(QueryEvaluatorDesc.class)) {
                filteredClasses.add(clazz);
            }
        }
        for (Class<? extends QueryEvaluator> clazz : filteredClasses) {
            try {
                QueryEvaluatorDesc evaluatorDesc = clazz.getAnnotation(QueryEvaluatorDesc.class);
                String name = evaluatorDesc.name();
                checkNotDuplicated(name);
                QueryEvaluator instance = clazz.newInstance();
                evaluatorMap.put(name, instance);
                log.info("Register QueryEvaluator name:{}", name);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Failed to create instance for class: " + clazz, e);
            }
        }
    }

    private static void checkNotDuplicated(String name) {
        if (evaluatorMap.containsKey(name)) {
            throw new RuntimeException("Evaluator " + name + " already exists");
        }
    }

    /**
     * 简单工厂获取 QueryEvaluator
     *
     * @param pickedStorage 存储引擎
     * @return 返回 QueryEvaluator 实例
     */
    public static QueryEvaluator getQueryEvaluator(String pickedStorage) {
        Preconditions.checkNotNull(pickedStorage, "pickedStorage can not be null");
        return evaluatorMap.get(pickedStorage);
    }
}
