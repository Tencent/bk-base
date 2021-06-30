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

package com.tencent.bk.base.datalab.queryengine.server.third;

/**
 * 权限校验第三方接口
 */
public interface AuthApiService {

    /**
     * 权限校验
     *
     * @param resultTableId 查询结果表 Id
     * @param authMethod 蓝鲸基础计算平台认证方式
     * @param bkAppCode 蓝鲸基础计算平台应用编码
     * @param bkAppSecret 蓝鲸基础计算平台应用私密 key secret
     * @param token 蓝鲸基础计算平台生成授权码
     * @param bkUserName 蓝鲸基础计算平台用户名
     * @return 校验结果
     */
    boolean checkAuth(String resultTableId, String authMethod, String bkAppCode, String bkAppSecret,
            String token, String bkUserName);

    /**
     * 权限校验
     *
     * @param rtArray 需要校验的结果表 Id 列表
     * @param actionId 需要校验的 actionId
     * @throws Exception 运行时异常
     */
    void checkAuth(String[] rtArray, String actionId) throws Exception;
}
