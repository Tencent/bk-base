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

package com.tencent.bk.base.datalab.queryengine.server.constant;

public class ThirdApiConstants {

    /**
     * 默认连接超时时长 (单位：毫秒)
     */
    public static final long DEFAULT_CONNECT_TIMEOUT = 60 * 1000;

    /**
     * 默认读取超时时长(单位：毫秒)
     */
    public static final long DEFAULT_SOCKET_TIMEOUT = 60 * 1000;

    /**
     * 默认最大重试次数
     */
    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    /**
     * 默认重试间隔(单位：毫秒)
     */
    public static final long DEFAULT_INITIAL_DELAY = 1000;

    /**
     * 默认最大重试间隔(单位：毫秒)
     */
    public static final long DEFAULT_MAX_DELAY = 60 * 1000;

    /**
     * 默认重试递增倍数
     */
    public static final double DEFAULT_MULTIPLIER = 2.0;

    /**
     * 笔记任务 notebook_id
     */
    public static final String NOTEBOOK_ID = "notebook_id";

    /**
     * 笔记任务 cell_id
     */
    public static final String CELL_ID = "cell_id";

}
