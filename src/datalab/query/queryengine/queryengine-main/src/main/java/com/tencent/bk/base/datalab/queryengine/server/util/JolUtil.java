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

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

/**
 * JOL 工具类，获取对象 shallow-size/deep-size/class-layout/object-graph-layout
 */
public class JolUtil {

    /**
     * 获取 shallow-size
     *
     * @param clazz Class 实例
     * @return shallow-size
     */
    public static long shallowSize(Class clazz) {
        Preconditions.checkArgument(clazz != null);
        return ClassLayout.parseClass(clazz).instanceSize();
    }

    /**
     * 获取 shallow-size
     *
     * @param obj Object 实例
     * @return shallow-size
     */
    public static long shallowSize(Object obj) {
        Preconditions.checkArgument(obj != null);
        return ClassLayout.parseInstance(obj).instanceSize();
    }

    /**
     * 获取 deep-size
     *
     * @param obj Object 实例
     * @return deep-size
     */
    public static long deepSize(Object obj) {
        Preconditions.checkArgument(obj != null);
        return GraphLayout.parseInstance(obj).totalSize();
    }

    /**
     * 获取 class-layout
     *
     * @param obj Object 实例
     * @return class-layout
     */
    public static String printClassLayOut(Object obj) {
        Preconditions.checkArgument(obj != null);
        return ClassLayout.parseInstance(obj).toPrintable();
    }

    /**
     * 获取 class-layout
     *
     * @param clazz Class 实例
     * @return class-layout
     */
    public static String printClassLayOut(Class clazz) {
        Preconditions.checkArgument(clazz != null);
        return ClassLayout.parseClass(clazz).toPrintable();
    }

    /**
     * 获取 Object Graph Layout
     *
     * @param obj Object 实例
     * @return Object Graph Layout
     */
    public static String printObjectGraphLayOut(Object obj) {
        Preconditions.checkArgument(obj != null);
        return GraphLayout.parseInstance(obj).toFootprint();
    }
}
