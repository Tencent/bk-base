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

package com.tencent.bk.base.datahub.hubmgr.job.aspect;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class RetryProcessor {

    private static final Logger log = LoggerFactory.getLogger(RetryProcessor.class);

    private static final int DEFAULT_RETRY_TIME = 0;
    private static final int DEFAULT_INTERVAL = 0;

    private int retryTime = DEFAULT_RETRY_TIME;
    // 重试的睡眠时间
    private long interval = DEFAULT_INTERVAL;
    private Exception exception;


    /**
     * 设置重试间隔，单位ms
     *
     * @param interval 重试间隔，单位ms
     */
    public RetryProcessor setInterval(long interval) {
        if (interval < 0) {
            throw new IllegalArgumentException("sleepTime should equal or bigger than 0");
        }

        this.interval = interval;
        return this;
    }

    /**
     * 设置重试次数
     *
     * @param retryTime 重试次数
     */
    public RetryProcessor setRetryTime(int retryTime) {
        if (retryTime < 0) {
            throw new IllegalArgumentException("retryTime should bigger than 0");
        }

        this.retryTime = retryTime;
        return this;
    }

    /**
     * 重试的业务执行代码 失败时请抛出一个异常
     */
    protected abstract Object process() throws Exception;

    /**
     * 执行重试逻辑
     *
     * @throws Exception 异常
     */
    public Object execute() throws Exception {
        for (int i = 0; i <= retryTime; i++) {
            try {
                return process();
            } catch (Exception e) {
                LogUtils.warn(log,
                        String.format("process is error, try to again! retryTime: %s, interval: %s", retryTime - i,
                                interval),
                        e);
                Thread.sleep(interval);
                exception = e;
            }
        }

        throw exception;
    }
}
