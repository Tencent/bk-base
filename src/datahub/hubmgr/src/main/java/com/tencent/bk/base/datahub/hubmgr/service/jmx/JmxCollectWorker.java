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

package com.tencent.bk.base.datahub.hubmgr.service.jmx;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.service.Service;
import com.tencent.bk.base.datahub.hubmgr.utils.JmxCollectorService;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * jmx 采集worker
 */
public class JmxCollectWorker implements Service {

    private static final Logger log = LoggerFactory.getLogger(JmxCollectWorker.class);

    private String clusterName;
    private String clusterType;
    private List<String> jmxAddress;
    private JmxCollectorService jmxCollectorService;
    private AtomicBoolean isStop = new AtomicBoolean(false);

    /**
     * 定时采集线程
     */
    private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName("jmx-collect-thread");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * jmx采集worker
     *
     * @param clusterName 集群名称
     * @param clusterType 集群类型
     * @param jmxAddress jmx地址
     * @param beanConf 集群jmx配置
     */
    public JmxCollectWorker(String clusterName, String clusterType, List<String> jmxAddress, List<Object> beanConf) {
        this.clusterName = clusterName;
        this.clusterType = clusterType;
        this.jmxAddress = jmxAddress;
        this.jmxCollectorService = new JmxCollectorService(clusterName, clusterType, jmxAddress, beanConf);
    }

    /**
     * 获取服务名称
     *
     * @return 返回服务名称
     */
    public String getServiceName() {
        return String.format("JmxCollectWorker: [clusterName: %s, clusterType: %s, jmxAddress: %s]",
                clusterName, clusterType, jmxAddress);
    }

    /**
     * 启动worker采集
     */
    public void start() {
        // 初始化通过JMX拉取各个集群的jmx指标信息，每分钟采集一次
        exec.scheduleAtFixedRate(jmxCollectorService, 0, 1, TimeUnit.MINUTES);
        isStop.set(false);
    }

    /**
     * 停止worker
     */
    public void stop() {
        LogUtils.info(log, "going to stop {} ", getServiceName());
        try {
            // 首先标记状态，然后触发executor的shutdown
            isStop.set(true);
            exec.shutdown();

            // 等待executor结束执行，30s超时
            if (!exec.awaitTermination(30, TimeUnit.SECONDS)) {
                LogUtils.warn(log, "failed to shutdown executor in timeout seconds");
            }
        } catch (InterruptedException e) {
            LogUtils.warn(log, "get interrupted exception!", e);
        } finally {
            if (!exec.isShutdown()) {
                exec.shutdown();
            }
            if (jmxCollectorService != null) {
                jmxCollectorService = null;
            }
        }
    }
}
