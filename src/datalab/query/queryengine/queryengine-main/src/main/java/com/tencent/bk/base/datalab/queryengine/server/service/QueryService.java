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

package com.tencent.bk.base.datalab.queryengine.server.service;

import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;

/**
 * 查询基础流程
 * <ul>
 * <li> 1、checkQuerySyntax 校验 sql 语法 </li>
 * <li> 2、checkPermission 校验权限 </li>
 * <li> 3、pickQueryStorage 选取存储 </li>
 * <li> 4、matchQueryForbiddenConfig 匹配查询禁用规则 </li>
 * <li> 5、checkQuerySemantic校验sql 语义 </li>
 * <li> 6、matchQueryRoutingRule 匹配查询路由规则 </li>
 * <li> 7、convertQueryStatement sql 转换和优化 </li>
 * <li> 8、getQueryDriver 获取查询驱动 </li>
 * <li> 9、executeQuery 执行查询 </li>
 * </ul>
 */
public interface QueryService {

    /**
     * 提交查询
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行异常
     */
    void query(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 提交查询
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行异常
     */
    default void doQuery(QueryTaskContext queryTaskContext) throws Exception {
        checkQuerySyntax(queryTaskContext);
        checkPermission(queryTaskContext);
        pickValidStorage(queryTaskContext);
        matchQueryForbiddenConfig(queryTaskContext);
        checkQuerySemantic(queryTaskContext);
        matchQueryRoutingRule(queryTaskContext);
        convertQueryStatement(queryTaskContext);
        getQueryDriver(queryTaskContext);
        executeQuery(queryTaskContext);
    }

    /**
     * 校验 sql 语法
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void checkQuerySyntax(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 校验结果表查询权限
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void checkPermission(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 选取合适的存储/计算引擎
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void pickValidStorage(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 获取存储集群负载
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void getStorageLoad(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 计算查询执行成本
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void calQueryExecCost(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 匹配查询路由规则
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void matchQueryRoutingRule(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 执行查询路由
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void executeQueryRule(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 获取查询驱动
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void getQueryDriver(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 匹配查询禁用配置
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void matchQueryForbiddenConfig(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * 执行查询
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void executeQuery(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * sql 转换和优化
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void convertQueryStatement(QueryTaskContext queryTaskContext) throws Exception;

    /**
     * sql 语义校验
     *
     * @param queryTaskContext 查询任务上下文
     * @throws Exception 运行时异常
     */
    void checkQuerySemantic(QueryTaskContext queryTaskContext) throws Exception;
}
