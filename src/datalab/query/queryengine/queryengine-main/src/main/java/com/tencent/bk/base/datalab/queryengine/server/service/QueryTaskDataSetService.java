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

import com.tencent.bk.base.datalab.queryengine.server.base.ModelService;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

public interface QueryTaskDataSetService extends ModelService<QueryTaskDataSet> {

    /**
     * 根据 queryId 获取查询结果集
     *
     * @param queryId 查询 Id
     * @return 查询结果集
     */
    QueryTaskDataSet loadByQueryId(String queryId);

    /**
     * 根据 queryId 获取查询结果集
     *
     * @param secretKey 密钥
     * @return 查询结果集
     */
    String[] extractDownloadElements(String secretKey);

    /**
     * 校验用户是否有数据下载的权限
     *
     * @param queryId 查询 Id
     * @param bkUserName 用户名
     */
    void checkDownloadPermission(String queryId, String bkUserName);

    /**
     * 下载文件
     *
     * @param queryId 查询 Id
     * @param downloadFormat 下载格式
     * @return 数据流
     */
    ResponseEntity<StreamingResponseBody> downloadFile(String queryId, String downloadFormat);
}