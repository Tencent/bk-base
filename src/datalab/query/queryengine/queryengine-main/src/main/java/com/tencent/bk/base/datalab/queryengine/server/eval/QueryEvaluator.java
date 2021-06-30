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

import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;

/**
 * 查询资源评估接口
 */
public interface QueryEvaluator {

    /**
     * 评估查询耗费的资源
     *
     * @param queryTaskContext 查询任务上下文
     * @return 查询耗费评估值
     */
    QueryCost eval(QueryTaskContext queryTaskContext);

    /**
     * 预估读取的行数
     *
     * @param queryTaskContext 查询上下文
     * @return 读取的行数
     */
    double getRows(QueryTaskContext queryTaskContext);

    /**
     * 预估耗费的 cpu 时长
     *
     * @param queryTaskContext 查询上下文
     * @return 耗费的cpu资源
     */
    double getCpu(QueryTaskContext queryTaskContext);

    /**
     * 预估耗费的 io 资源
     *
     * @param queryTaskContext 查询上下文
     * @return 耗费的 io 资源
     */
    double getIo(QueryTaskContext queryTaskContext);
}
