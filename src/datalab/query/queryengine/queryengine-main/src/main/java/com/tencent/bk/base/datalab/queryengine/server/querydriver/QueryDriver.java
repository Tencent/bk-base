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

import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.Connection;

public interface QueryDriver {

    /**
     * 查询模板
     *
     * @param queryTaskContext 查询上下文
     * @throws Exception 抛出异常
     */
    void query(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 获取连接
     *
     * @param queryTaskContext 查询上下文
     * @return connection
     * @throws Exception 抛出异常
     */
    default Connection getConnection(QueryTaskContext queryTaskContext) throws Exception {
        return null;
    }

    /**
     * 执行查询
     *
     * @param queryTaskContext 查询上下文
     * @throws Exception 抛出异常
     */
    void executeQuery(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 处理执行时异常
     *
     * @param e 异常实例
     */
    default void processExecuteException(QueryTaskContext queryTaskContext, Throwable e) {
    }

    /**
     * 判断是否是内部保留字段
     *
     * @param columnName 字段名
     * @return true:是内部保留字段,false:不是内部保留字段
     */
    default boolean isInternalColumn(String columnName) {
        return false;
    }
}
