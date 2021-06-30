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

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkUtil {

    private static Logger log = LoggerFactory.getLogger(ZkUtil.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = 1000;

    /**
     * 创建zk路径
     *
     * @param curator zk客户端
     * @param mode 节点模式
     * @param path 路径
     */
    public static void createPath(CuratorFramework curator, CreateMode mode, String path) {
        Preconditions.checkArgument(curator != null, "curator not be null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(path), "path not be empty");

        try {
            curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(mode == null ? CreateMode.EPHEMERAL : mode)
                    .forPath(path);
        } catch (Exception e) {
            LogUtils.warn(log, "create zk path failed, exception: {}", e);
            throw new IllegalStateException("create zk path failed");
        }
    }

    /**
     * zk path 添加监听事件
     *
     * @param curator zk客户端
     * @param path 路径
     */
    public static void watch(CuratorFramework curator, String path, BiFunction bif) {
        Preconditions.checkArgument(curator != null, "curator not be null");
        Preconditions.checkArgument(StringUtils.isNotEmpty(path), "path not be empty");
        try {
            PathChildrenCache watcher = new PathChildrenCache(curator, path, true);
            watcher.getListenable().addListener((cf, event) -> {
                LogUtils.info(log, "got zk event {} for path {}", event, path);
                bif.apply(cf, event);
            });

            LogUtils.info(log, "start watcher for {} on zookeeper", path);
            watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            LogUtils.warn(log, "add watcher failed, exception: {}", e);
            throw new IllegalStateException("add watcher failed");
        }
    }

    /**
     * 查询数据
     *
     * @param curator zk客户端
     * @param path 路径
     * @param <T> 类型
     * @throws Exception 异常
     */
    public static <T> T getDataForObj(CuratorFramework curator, String path) throws Exception {
        byte[] bytes = curator.getData().forPath(path);
        Optional<T> res = bytesToObject(bytes);
        return res.orElse(null);
    }

    /**
     * 查询数据
     *
     * @param curator zk客户端
     * @param path 路径
     * @throws Exception 异常
     */
    public static String getData(CuratorFramework curator, String path) throws Exception {
        byte[] bytes = curator.getData().forPath(path);
        return bytes.length <= 0 ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 更新数据
     *
     * @param curator zk客户端
     * @param path 路径
     * @param <T> 类型
     * @throws Exception 异常
     */
    public static <T> void update(CuratorFramework curator, String path, T obj) throws Exception {
        curator.setData().forPath(path, objectToBytes(obj).orElse(null));
    }

    /**
     * 更新字符串数据
     *
     * @param curator zk客户端
     * @param path 路径
     * @throws Exception 异常
     */
    public static void update(CuratorFramework curator, String path, String obj) throws Exception {
        curator.setData().forPath(path, obj.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 检查路径是否存在
     *
     * @param curator zk客户端
     * @param path 路径
     * @throws Exception 异常
     */
    public static Stat checkExists(CuratorFramework curator, String path) throws Exception {
        return curator.checkExists().forPath(path);
    }

    /**
     * 对象转数组
     *
     * @param obj 对象
     * @param <T> 类型
     * @return 字段数组
     */
    private static <T> Optional<byte[]> objectToBytes(T obj) throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream sOut = new ObjectOutputStream(out)) {
            sOut.writeObject(obj);
            sOut.flush();
            bytes = out.toByteArray();
        }

        return Optional.of(bytes);
    }

    /**
     * bytes 转对象
     *
     * @param bytes 字节数组
     * @param <T> 类型
     * @return 对象
     */
    private static <T> Optional<T> bytesToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        T t;
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                ObjectInputStream sIn = new ObjectInputStream(in)) {
            t = (T) sIn.readObject();
        }

        return Optional.ofNullable(t);
    }

    /**
     * 创建客户端
     *
     * @param zkAddress zk地址
     */
    public static CuratorFramework newClient(String zkAddress) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(DEFAULT_MAX_SLEEP_MS, DEFAULT_MAX_RETRIES);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();

        return client;
    }
}
