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
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 分布式任务分配器。
 */
public class DistributeWorkers {

    private static final Logger log = LoggerFactory.getLogger(DistributeWorkers.class);

    // worker 注册默认路径
    private String workerPath;

    // 当前总共的databusmgr worker默认数量
    private AtomicInteger workerCnt = new AtomicInteger(1);
    // 当前databusmgr worker在worker列表中的索引编号，从0开始。
    private AtomicInteger workerIdx = new AtomicInteger(0);

    private CuratorFramework curator = DatabusMgr.getZkClient();

    /**
     * 构造函数，注册地址
     */
    public DistributeWorkers(String workerPath, int sleepMillis) {
        Preconditions.checkArgument(!StringUtils.isBlank(workerPath), "workerPath can not be blank");
        this.workerPath = workerPath;
        // 注册当前worker，并监听节点变化
        registerWorker(sleepMillis);
    }

    /**
     * 注册worker。
     */
    private void registerWorker(int sleepMillis) {
        try {
            // 首先在zk的固定路径上创建节点，节点内容为IP地址，临时节点。
            String path = String.format("%s/%s", workerPath, Utils.getInnerIp());
            curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path);

            LogUtils.info(log, "register worker {} success!", path);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to create zk node! {}", e.getMessage());
        }

        // 等待一段时间，保证worker全部注册完毕
        try {
            Thread.sleep(sleepMillis);
            setWorker();
        } catch (Exception e) {
            LogUtils.warn(log, "failed to register worker! {}", e.getMessage());
        }
    }

    /**
     * 设置当前worker信息
     */
    public List<String> setWorker() {
        // 获取worker信息
        List<String> workers = null;
        try {
            workers = curator.getChildren().forPath(workerPath);
            LogUtils.info(log, "going to set worker: {}", workers);
            workers.sort(null);
            for (int i = 0; i < workers.size(); i++) {
                if (workers.get(i).equals(Utils.getInnerIp())) {
                    workerCnt.set(workers.size());
                    workerIdx.set(i);
                }
            }
            LogUtils.info(log, "{}: {} workers with index {}, {}", Utils.getInnerIp(), workerCnt, workerIdx, workers);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to set worker!", e);
        }
        return workers;
    }

    /**
     * 获取 workerCnt
     */
    public int getWorkerCnt() {
        return this.workerCnt.get();
    }

    /**
     * 获取 workerIdx
     */
    public int getWorkerIdx() {
        return this.workerIdx.get();
    }
}
