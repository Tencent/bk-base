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

package com.tencent.bk.base.datahub.databus.commons.utils;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;

import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ZkUtils {

    private static final Logger log = LoggerFactory.getLogger(ZkUtils.class);

    private static final String ZK_RT_CONFIG_CHANGE_PATH = "/config/databus/rt_config_change";
    private static final String WATCHER_ZK_HOST = "watcher.zk.host";


    private static final ConcurrentHashMap<String, ConcurrentHashMap<TaskContextChangeCallback, Boolean>> TASKS =
            new ConcurrentHashMap<>(
            100);
    private static final ConcurrentHashMap<String, Boolean> TO_DELETE_PATHS = new ConcurrentHashMap<>(10);
    private static final CuratorFramework client;


    static {
        // 初始化zk的连接
        String connectString = BasicProps.getInstance().getClusterProps().get(WATCHER_ZK_HOST);
        if (StringUtils.isBlank(connectString)) {
            LogUtils.warn(log, "watcher.zk.host for cluster is empty, will not start zk utils");
            client = null;
        } else {
            LogUtils.info(log, "Connecting to zk {} ...", connectString);
            client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(500, 3));
            client.start();

            // 尝试在zk上创建rt变化的path
            createNodeWithParents(CreateMode.PERSISTENT, ZK_RT_CONFIG_CHANGE_PATH);

            // 在zk上设置watcher
            initEtlChangeWatcher();

            // 定时触发删除节点的逻辑，按照顺序删除
            ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    deleteZkPaths();
                }
            }, 30, 60, TimeUnit.MINUTES);

            // 增加shutdown hook清理资源
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        client.close();
                        exec.shutdown();
                    } catch (Exception ignore) {
                        LogUtils.warn(log, "exception in shutdown hook for ZkUtils: ", ignore);
                    }
                }
            });
        }
    }

    /**
     * 在zk上按照节点类型创建节点和其父节点
     *
     * @param mode 节点类型
     * @param path 此节点在zk上的路径
     * @return true/false
     */
    private static boolean createNodeWithParents(CreateMode mode, String path) {
        try {
            // 如果节点不存在则先创建节点
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(mode)
                    .forPath(path);

            return true;
        } catch (Exception ignore) {
            if (ignore instanceof KeeperException.NodeExistsException) {
                LogUtils.info(log, "{} is already exists on zk", path);
            } else {
                LogUtils.warn(log, String.format("failed to create %s path %s on zk", mode, path), ignore);
            }
        }

        return false;
    }

    /**
     * 监听zk节点上的/config/databus/etl_change上子节点创建的事件，读取节点内容并触发回调
     */
    private static void initEtlChangeWatcher() {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, ZK_RT_CONFIG_CHANGE_PATH, true);
        try {
            // 监听子节点变化
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                        ChildData data = event.getData();
                        String rtId = new String(data.getData(), StandardCharsets.UTF_8);
                        String path = data.getPath();
                        LogUtils.info(log, "event: {}, path: {}, content: {}", event.getType(), data.getPath(), rtId);
                        callCtxChangeCallback(rtId);
                        TO_DELETE_PATHS.put(path, true);
                    } else {
                        LogUtils.warn(log, "ignore child node event {}", event.toString());
                    }
                }
            });
        } catch (Exception ignore) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.ZK_ERR,
                    "failed to handle children node changes for " + ZK_RT_CONFIG_CHANGE_PATH, ignore);
        }
    }

    /**
     * 调用注册的回调，触发指定的rtId对应的全部回调任务（可能有多个）
     *
     * @param rtId rtId
     */
    private static void callCtxChangeCallback(String rtId) {
        // 触发回调，更新相关task的context
        ConcurrentHashMap<TaskContextChangeCallback, Boolean> callbacks = TASKS.get(rtId);
        if (callbacks != null) {
            for (TaskContextChangeCallback callback : callbacks.keySet()) {
                callback.markBkTaskCtxChanged(); // 调用回调
            }
        }
    }

    /**
     * 定期删除无用的zk节点
     */
    private static void deleteZkPaths() {
        try {
            Set<String> paths = new HashSet<>(TO_DELETE_PATHS.keySet());
            TO_DELETE_PATHS.clear();
            LogUtils.info(log, "going to delete zk nodes: {}", paths);
            for (String path : paths) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, "failed to delete some nodes in zk", ignore);
        }
    }

    /**
     * 注册task context的变化事件的回调
     *
     * @param rtId task的rtId
     * @param callback 事件的回调
     */
    public static void registerCtxChangeCallback(String rtId, TaskContextChangeCallback callback) {
        try {
            TASKS.putIfAbsent(rtId, new ConcurrentHashMap<>(1));
            TASKS.get(rtId).put(callback, true);
        } catch (Exception ignore) {
            LogUtils.warn(log, String.format("%s failed to register ctx change callback!", rtId), ignore);
        }
    }

    /**
     * 取消task context的变化事件的回调
     *
     * @param rtId task的rtId
     * @param callback 事件的回调
     */
    public static void removeCtxChangeCallback(String rtId, TaskContextChangeCallback callback) {
        try {
            ConcurrentHashMap<TaskContextChangeCallback, Boolean> map = TASKS.get(rtId);
            if (map != null) {
                map.remove(callback);
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, String.format("%s failed to remove ctx change callback!", rtId), ignore);
        }
    }

}
