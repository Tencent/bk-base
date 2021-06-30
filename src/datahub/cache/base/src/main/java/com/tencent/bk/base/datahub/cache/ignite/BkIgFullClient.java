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

package com.tencent.bk.base.datahub.cache.ignite;

import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkIgFullClient {

    private static final Logger log = LoggerFactory.getLogger(BkIgFullClient.class);

    private static final Map<String, Ignite> CLIENTS = new ConcurrentHashMap<>();

    private static final ReentrantLock LOCK = new ReentrantLock();

    static {
        // 增加进程shutdown时清理资源的逻辑
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LogUtils.info(log, "going to shutdown all ignite full clients in 15 seconds {}", CLIENTS.keySet());
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ignore) {
                    // ignore
                }
                CLIENTS.keySet().forEach(BkIgFullClient::closeClient);
            }
        });
    }

    /**
     * 初始化ignite full client
     *
     * @param clusterName ignite集群名称
     * @return ignite集群对应的full client
     */
    private static Ignite initFullClient(String clusterName) {
        // 通过配置文件加载ignite集群的配置，避免查询storekit接口，对外部接口有依赖。集群配置文件固定为 集群名称 + .xml
        String dir = DatabusProps.getInstance()
                .getOrDefault(CacheConsts.IGNITE_CONF_DIR, System.getProperty("user.dir"));
        String file = String.format("%s/%s.xml", dir, clusterName);
        LogUtils.info(log, "{} going to load ignite conf file {}", clusterName, file);

        return start(file, clusterName);
    }

    /**
     * 启动ignite
     *
     * @param springCfgPath 配置文件地址
     * @param instanceName 实例名称
     */
    public static Ignite start(@Nullable String springCfgPath, String instanceName) {
        Ignite ignite;
        try {
            ignite = springCfgPath == null ? IgnitionEx.start() : IgnitionEx.start(springCfgPath, instanceName);
        } catch (IgniteCheckedException e) {
            LogUtils.info(log, "{}: failed to start ignite instance {}, {}", springCfgPath, instanceName,
                    e.getMessage());
            throw U.convertException(e);
        }
        return ignite;
    }

    /**
     * 获取ignite full client
     *
     * @param clusterName 集群名称
     * @return ignite集群对应的full client
     */
    public static Ignite getClient(String clusterName) {
        LOCK.lock();
        try {
            if (CLIENTS.containsKey(clusterName)) {
                return CLIENTS.get(clusterName);
            } else {
                Ignite ignite = initFullClient(clusterName);
                CLIENTS.put(clusterName, ignite);
                return ignite;
            }
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * 关闭ignite full client。仅仅在shutdown流程，或者清理此集群时需要调用。
     *
     * @param clusterName ignite集群名称
     */
    public static void closeClient(String clusterName) {
        Ignite ig = CLIENTS.remove(clusterName);
        if (ig != null) {
            ig.close();
        }
    }

}