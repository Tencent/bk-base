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

package com.tencent.bk.base.datahub.hubmgr.utils;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 使用分布式锁构建的worker，在集群中，只会有一个worker获取到锁并执行，其他worker处于等待锁的状态中。
 */
public class DistributeLock implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DistributeLock.class);
    private InterProcessMutex mutex;
    private boolean holdLock = false;

    /**
     * 构造函数
     *
     * @param path zk上的路径
     */
    public DistributeLock(String path) {
        mutex = new InterProcessMutex(DatabusMgr.getZkClient(), path);
    }

    /**
     * 获取一个分布式锁，返回成功与否。
     *
     * @param timeoutSec 获取锁最多等待时间，秒为单位。
     * @return 是否成功获取锁。
     * @throws Exception 异常
     */
    public boolean lock(long timeoutSec) throws Exception {
        if (!holdLock) {
            // 只有当本对象不持有锁时，尝试获取锁
            holdLock = mutex.acquire(timeoutSec, TimeUnit.SECONDS);
        }
        return holdLock;
    }

    /**
     * 释放锁，如果本对象持有锁，则释放，否则什么也不做。
     *
     * @throws Exception 异常
     */
    public void release() throws Exception {
        if (holdLock) {
            mutex.release();
            holdLock = false;
        }
    }

    /**
     * JDK1.7自动调用close
     */
    public void close() throws IOException {
        try {
            release();
        } catch (Exception e) {
            LogUtils.warn(log, "failed to release lock!", e);
            throw new IOException(e.getMessage());
        }
    }
}
