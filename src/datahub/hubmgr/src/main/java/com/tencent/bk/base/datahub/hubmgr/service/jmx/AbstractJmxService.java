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

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.service.Service;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.ZkUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.jsr166.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractJmxService implements Service {

    private static Logger log = LoggerFactory.getLogger(AbstractJmxService.class);

    // 所有的服务对象，按照注册的先后顺序处理
    // 在节点变更时，可能出现并发修改当前节点分配任务列表
    private ConcurrentLinkedHashMap<String, JmxCollectWorker> services = new ConcurrentLinkedHashMap<>();
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(3);
    private String rootJmxPath = "/databusmgr/jmx/";
    private String zkJmxPath = "/databusmgr/jmx/common";
    private String jmxFilePath;
    // 当前总共的databusmgr worker数量
    private int workerCnt = 1;
    // 当前databusmgr worker在worker列表中的索引编号，从0开始。
    private int workerIdx = 0;
    // 默认排序
    private TreeSet<String> allJmxClusters = new TreeSet<>();
    private Map<String, Object> allClusterConf = new HashMap<>();

    @Override
    public String getServiceName() {
        return "jmxService";
    }

    // 定义类型
    public abstract String type();

    @Override
    public void start() {
        jmxFilePath = DatabusProps.getInstance().getProperty(Consts.JMX_CONF_PATH);
        Preconditions.checkArgument(StringUtils.isNotEmpty(jmxFilePath), "not found jmx file");
        String type = type();
        this.zkJmxPath = String.format("%s%s", rootJmxPath, type);
        // 首先在zk的固定路径（jmx/jvm）上创建节点，节点内容为IP地址，临时节点。
        CuratorFramework curator = DatabusMgr.getZkClient();
        try {
            LogUtils.info(log, "{}: going to register on zookeeper, path: {}", Utils.getInnerIp(), zkJmxPath);
            ZkUtil.createPath(curator, CreateMode.EPHEMERAL, zkJmxPath + "/" + Utils.getInnerIp());
            // 监听zk上jvm目录下的内容，当目录下节点发生变化时，就是集群内有节点增删，此时重新指定worker地址并分配任务
            ZkUtil.watch(curator, zkJmxPath, (cf, event) -> {
                if ((((PathChildrenCacheEvent) event).getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
                        || ((PathChildrenCacheEvent) event).getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED))
                        && DatabusMgr.isRunning()) {
                    LogUtils.info(log, "node happened change, ready to reset worker");
                    // 当前机器指定worker地址
                    List<String> workers = setWorker(curator);
                    LogUtils.info(log, "{}: {} workers with index {}, {}, type: {}",
                            Utils.getInnerIp(), workerCnt, workerIdx, workers, type);
                    // 重新分配worker任务
                    reassignJvmCluster();
                }
                return null;
            });
        } catch (Exception e) {
            LogUtils.warn(log, "{}: failed to play with zookeeper, exiting jmx service!", type, e);
            throw new RuntimeException(e);
        }
        exec.scheduleAtFixedRate(() -> {
            try {
                // 每个小时执行一次，获取最新的集群列表
                // 读取jvm json文件列表
                List<String> jmxFiles = CommUtils.filterFile(jmxFilePath, ".json");
                LogUtils.info(log, "{}: reload jmx file: {}", type, jmxFiles);

                // 需要排序，否则各机器节点获取到的集群列表顺序不一致
                TreeSet<String> clusters = new TreeSet<>();
                jmxFiles.stream().filter(filePath -> filePath.contains(type)).forEach(filePath -> {
                    Map<Object, Object> clusterMap = JsonUtils.readJsonFileWithoutException(filePath);
                    // 不处理错误配置的集群
                    if (clusterMap != null && clusterMap.size() > 0) {
                        String clusterKey = getClusterKey((String) clusterMap.get("clusterType"),
                                (String) clusterMap.get("clusterName"));
                        allClusterConf.put(clusterKey, clusterMap);
                        clusters.add(clusterKey);
                    }
                });

                if (!allJmxClusters.equals(clusters)) {
                    // 集群信息发生变化，重新分配本worker上采集的jmx服务端列表
                    allJmxClusters = clusters;
                    List<String> workers = setWorker(curator);
                    LogUtils.info(log, "{}: {} workers with index {}, {}, {}",
                            Utils.getInnerIp(), workerCnt, workerIdx, workers, type);
                    reassignJvmCluster();
                }

                LogUtils.info(log, "{}: Monitoring Cluster List: {}", type, services.keySet());
            } catch (Exception e) {
                LogUtils.warn(log, "{}: Failed to update jvm cluster", type, e);
            }
        }, 0, 3600, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        Exception lastException = null;
        for (Map.Entry<String, JmxCollectWorker> entry : services.entrySet()) {
            LogUtils.info(log, "{}: Stopping JmxCollectWorker {}", type(), entry.getKey());
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                lastException = e;
                LogUtils.error(MgrConsts.ERRCODE_STOP_SERVICE, log,
                        String.format("%s: stopping JmxCollectWorker %s failed!", type(), entry.getKey()), e);
            }
        }

        if (lastException != null) {
            throw lastException;
        }
    }

    private List<String> setWorker(CuratorFramework curator) {
        // 读取所有的path，确定有多少个databusmgr，将ip排序
        try {
            List<String> workers = curator.getChildren().forPath(zkJmxPath);
            LogUtils.info(log, "{}: going to set worker: {}", type(), workers);
            workers.sort(null);
            for (int i = 0; i < workers.size(); i++) {
                if (workers.get(i).equals(Utils.getInnerIp())) {
                    workerCnt = workers.size();
                    workerIdx = i;
                }
            }
            return workers;
        } catch (Exception e) {
            LogUtils.warn(log, "{}: failed to set worker, exiting jvm service!", type(), e);
            throw new RuntimeException(e);
        }
    }

    private String getClusterKey(String clusterType, String clusterName) {
        return String.format("%s-%s", clusterType, clusterName);
    }


    /**
     * 重新分配kafka集群offset信息采集worker
     */
    private void reassignJvmCluster() {
        // 删除已下架集群的监控
        services.forEach((k, v) -> {
            if (!allJmxClusters.contains(k)) {
                LogUtils.info(log, "{}: begin to remove jvm offline: {}", type(), k);
                services.remove(k).stop();
            }
        });
        AtomicInteger ai = new AtomicInteger(0);
        allJmxClusters.forEach(cluster -> {
            // 当此集群分配到当前worker上，且没有已经存在在SERVICES时，添加jvm信息采集
            if (ai.get() % workerCnt == workerIdx) {
                if (!services.containsKey(cluster)) {
                    Map<Object, Object> clusterConf = (Map<Object, Object>) allClusterConf.get(cluster);
                    String clusterName = (String) clusterConf.get("clusterName");
                    String clusterType = (String) clusterConf.get("clusterType");
                    List<String> jmxAddress = (List<String>) clusterConf.get("address");
                    List<Object> beanConf = (List<Object>) clusterConf.get("conf");
                    JmxCollectWorker worker = new JmxCollectWorker(clusterName, clusterType, jmxAddress, beanConf);
                    try {
                        worker.start();
                        services.put(cluster, worker);
                    } catch (Exception e) {
                        LogUtils.warn(log, "{}: fail to start " + worker.getServiceName(), type(), e);
                    }
                }
            } else {
                // 当不应该在本worker上执行的集群已经在本worker上启动了，则停掉
                if (services.containsKey(cluster)) {
                    services.remove(cluster).stop();
                }
            }
            ai.incrementAndGet();
        });
        LogUtils.info(log, "{} workers for {}, index {} for {}, type {}",
                workerCnt, allJmxClusters, workerIdx, services.keySet(), type());
    }
}
