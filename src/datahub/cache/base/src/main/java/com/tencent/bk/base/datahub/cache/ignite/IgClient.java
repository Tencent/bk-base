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
import com.tencent.bk.base.datahub.cache.CacheUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgClient {

    private static final Logger log = LoggerFactory.getLogger(IgClient.class);

    private static final Map<String, ArrayBlockingQueue<IgniteClient>> CLIENTS = new ConcurrentHashMap<>();

    private final String cluster;
    private final String host;
    private final String port;
    private final String user;
    private final String pass;

    public IgClient(Map<String, String> props) {
        // 从配置中获取默认的连接池大小、Ignite集群地址、集群账号信息等
        cluster = props.getOrDefault(CacheConsts.IGNITE_CLUSTER, CacheConsts.CLUSTER_DFT);
        host = props.getOrDefault(CacheConsts.IGNITE_HOST, CacheConsts.IGNITE_HOST_DFT);
        port = props.getOrDefault(CacheConsts.IGNITE_PORT, CacheConsts.IGNITE_PORT_DFT);
        user = props.getOrDefault(CacheConsts.IGNITE_USER, CacheConsts.IGNITE_USER_DFT);
        pass = props.getOrDefault(CacheConsts.IGNITE_PASS, CacheConsts.IGNITE_PASS_DFT);

        if (!CLIENTS.containsKey(cluster)) {
            // 初始化连接池
            int poolSize = CacheUtils
                    .parseInt(props.get(CacheConsts.IGNITE_POOL_SIZE), CacheConsts.IGNITE_POOL_SIZE_DFT);
            CLIENTS.putIfAbsent(cluster, new ArrayBlockingQueue<>(poolSize));
        }
        LogUtils.info(log, "{}: ignite client pool initialized {}:{}", cluster, host, port);
    }

    /**
     * 关闭ignite client，释放资源
     *
     * @param client client对象
     */
    public static void closeQuietly(IgniteClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LogUtils.warn(log, "close ignite client failed!", e);
            }
        }
    }

    /**
     * 获取一个ignite的连接。
     *
     * @return ignite的连接
     */
    public IgniteClient getClient() {
        ArrayBlockingQueue<IgniteClient> queue = CLIENTS.get(cluster);
        IgniteClient client = queue == null ? null : queue.poll();
        if (client == null) {
            client = initClient();
        }

        return client;
    }

    /**
     * 将ignite的连接放入队列中
     *
     * @param client ignite连接
     */
    public void returnToPool(IgniteClient client) {
        if (client != null) {
            ArrayBlockingQueue<IgniteClient> queue = CLIENTS.get(cluster);
            if (queue != null) {
                boolean succ = queue.offer(client);
                if (!succ) {
                    LogUtils.info(log, "{} ignite client pool is full, discard this client.", cluster);
                    closeQuietly(client);
                }
            } else {
                LogUtils.warn(log, "{} ignite client pool not found!", cluster);
                closeQuietly(client);
            }
        }
    }

    /**
     * 初始化ignite的连接
     *
     * @return ignite的连接
     */
    private IgniteClient initClient() {
        // 支持域名解析为多个主机地址
        Set<String> hostSet = CacheUtils.parseHosts(host);
        String[] hosts = new String[hostSet.size()];
        AtomicInteger i = new AtomicInteger(0);
        hostSet.forEach(h -> hosts[i.getAndIncrement()] = h + ":" + port);

        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(hosts);
        if (StringUtils.isNotBlank(user)) {
            clientCfg.setUserName(user).setUserPassword(pass);
        }
        LogUtils.info(log, "{}: init ignite cluster {}", cluster, StringUtils.join(hostSet, ","));
        return Ignition.startClient(clientCfg);
    }

}