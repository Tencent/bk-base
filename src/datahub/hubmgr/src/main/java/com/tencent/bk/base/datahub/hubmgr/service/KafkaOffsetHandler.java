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

package com.tencent.bk.base.datahub.hubmgr.service;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ALARM_INTERVAL;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_CHANNEL;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_CHANNEL_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAHUB_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.BATCH_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.CONCERN_KAFKA_CLUSTERS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUS_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ERRCODE_STOP_SERVICE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ES_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STREAM_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.WARN_IGNORE_GROUPS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.WARN_IGNORE_KAFKA_CLUSTERS;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import com.tencent.bk.base.datahub.hubmgr.service.kafka.offset.GroupLag;
import com.tencent.bk.base.datahub.hubmgr.service.kafka.offset.KafkaOffsetWorker;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeWorkers;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOffsetHandler implements Service {

    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetHandler.class);
    // 所有的服务对象，按照注册的先后顺序处理
    private static final String KAFKA_MONITOR_PATH = "/databusmgr/kafkamonitor";
    private static final Map<String, KafkaOffsetWorker> SERVICES = new LinkedHashMap<>();

    private static DistributeWorkers disWorkers = new DistributeWorkers(KAFKA_MONITOR_PATH, 10_000);

    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(3);
    private String warnReceiver;
    private String streamWarnReceiver;
    private String batchWarnReceiver;
    private String esWarnReceiver;
    private String storekitWarnReceiver;
    private LoadingCache<String, GroupLag> cache;
    private Set<String> ignoredClusters;
    private Set<String> ignoreGroups;
    private LinkedHashSet<String> allKafkaClusters = new LinkedHashSet<>();
    private PathChildrenCache watcher = null;

    @Override
    public String getServiceName() {
        return "KafkaOffsetHandler";
    }

    @Override
    public void start() {
        int alarmInterval = DatabusProps.getInstance().getOrDefault(ALARM_INTERVAL, 15);
        // 计算cache过期时间，flow计算每10分钟提交一次offset，尽可能保留长时间告警信息
        int cacheTimeout = alarmInterval - 1;
        // 用于记录消费组下延迟过大的消费信息，当超过2分钟没有被访问或者更新会被刷新（消费组已经不再延迟）
        cache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, TimeUnit.MINUTES)
                .build(new CacheLoader<String, GroupLag>() {
                    @Override
                    public GroupLag load(String pattern) {
                        return new GroupLag(pattern);
                    }
                });

        // 消费延迟告警
        exec.scheduleAtFixedRate(this::sendWarn, alarmInterval, alarmInterval, TimeUnit.MINUTES);

        DatabusProps props = DatabusProps.getInstance();
        warnReceiver = props.getOrDefault(DATABUS_ADMIN, "");
        streamWarnReceiver = props.getOrDefault(STREAM_ADMIN, "");
        batchWarnReceiver = props.getOrDefault(BATCH_ADMIN, "");
        storekitWarnReceiver = props.getOrDefault(STOREKIT_ADMIN, "");
        esWarnReceiver = props.getOrDefault(ES_ADMIN, "");

        String warnIgnoreConfig = props.getOrDefault(WARN_IGNORE_KAFKA_CLUSTERS, "");
        ignoredClusters = new HashSet<>(Arrays.asList(warnIgnoreConfig.split(",")));

        String ignoreGroupsConfig = props.getOrDefault(WARN_IGNORE_GROUPS, "");
        ignoreGroups = new HashSet<>(Arrays.asList(ignoreGroupsConfig.split(",")));

        monitorZkNode();
        executeSchedule(props);
    }

    /**
     * 定时执行集群状态采集任务
     *
     * @param props 总线配置
     */
    private void executeSchedule(DatabusProps props) {
        String apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        String path = props.getOrDefault(API_CHANNEL, API_CHANNEL_DEFAULT);
        List<String> concernClusters = Arrays.asList(props.getOrDefault(CONCERN_KAFKA_CLUSTERS, "").split(","));
        exec.scheduleAtFixedRate(() -> {
            try {
                // 每个小时执行一次，获取最新的集群列表
                // 获取当前kafka集群列表信息
                String restUrl = String.format("http://%s%s", apiDns, path);
                ApiResult result = HttpUtils.getApiResult(restUrl);
                if (result.isResult()) {
                    LogUtils.info(log, "get databus channel list: {}",
                            JsonUtils.toJsonWithoutException(result.getData()));
                    LinkedHashSet<String> clusters = new LinkedHashSet<>();
                    LogUtils.info(log, "begin to filter concern cluster role: {}", concernClusters);
                    for (Map<String, Object> cluster : (List<Map<String, Object>>) result.getData()) {
                        // 过滤需要进行监控的集群
                        if (concernClusters.contains(cluster.get("cluster_role").toString())
                                && cluster.get("cluster_type").toString().equalsIgnoreCase("kafka")) {
                            clusters.add(cluster.get("cluster_domain") + ":" + cluster.get("cluster_port"));
                        } else {
                            LogUtils.info(log, "channel cluster is not in monitor list, skipped: {}", cluster);
                        }
                    }

                    if (!allKafkaClusters.equals(clusters)) {
                        // kafka集群信息发生变化，重新分配本worker上采集的kafka集群
                        allKafkaClusters = clusters;
                        reassignKafkaCluster();
                    }
                }

                LogUtils.info(log, "Monitoring Cluster List: {}", SERVICES.keySet());
            } catch (Exception e) {
                LogUtils.warn(log, "Failed to update kafka cluster", e);
            }
        }, 0, 3600, TimeUnit.SECONDS);
    }

    /**
     * 创建ZK监听节点
     */
    private void monitorZkNode() {
        CuratorFramework curator = DatabusMgr.getZkClient();
        try {
            // 监听zk上kafkamonitor目录下的内容，当目录下节点发生变化时，就是集群内有节点增删，此时重新分配kafka集群
            watcher = new PathChildrenCache(curator, KAFKA_MONITOR_PATH, true);
            watcher.getListenable().addListener(new PathChildrenCacheListener() {
                public void childEvent(CuratorFramework cf, PathChildrenCacheEvent event) throws Exception {
                    LogUtils.info(log, "got zk event {} for path {}", event, KAFKA_MONITOR_PATH);
                    if ((event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
                            || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED))
                            && DatabusMgr.isRunning()) {
                        // 读取所有的path，确定有多少个databusmgr，将ip排序
                        disWorkers.setWorker();
                        reassignKafkaCluster();
                    }
                }
            });

            LogUtils.info(log, "start watcher for {} on zookeeper", KAFKA_MONITOR_PATH);
            watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            // 是否需要抛出异常，中断整个databusmgr进程？
            LogUtils.warn(log, "failed to play with zookeeper, exiting kafka offset service!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() throws Exception {
        Exception lastException = null;
        for (Map.Entry<String, KafkaOffsetWorker> entry : SERVICES.entrySet()) {
            LogUtils.info(log, "Stopping KafkaOffsetWorker {}", entry.getKey());
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                lastException = e;
                LogUtils.error(ERRCODE_STOP_SERVICE, log,
                        String.format("Stopping KafkaOffsetWorker %s failed! ", entry.getKey()), e);
            }
        }

        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * 重新分配kafka集群offset信息采集worker
     */
    private void reassignKafkaCluster() {
        // 删除已下架集群的监控
        SERVICES.forEach((k, v) -> {
            if (!allKafkaClusters.contains(k)) {
                LogUtils.info(log, "begin to remove kafka offline: {}", k);
                SERVICES.remove(k).stop();
            }
        });

        AtomicInteger ai = new AtomicInteger(0);
        allKafkaClusters.forEach(cluster -> {
            // 当此集群分配到当前worker上，且没有已经存在在SERVICES时，添加kafka offset信息采集
            if (ai.get() % disWorkers.getWorkerCnt() == disWorkers.getWorkerIdx()) {
                if (!SERVICES.containsKey(cluster)) {
                    KafkaOffsetWorker handler = new KafkaOffsetWorker(cluster, cache);
                    try {
                        handler.start();
                        SERVICES.put(cluster, handler);
                    } catch (Exception e) {
                        LogUtils.warn(log, "fail to start " + handler.getServiceName(), e);
                    }
                }
            } else {
                // 当不应该在本worker上执行的集群已经在本worker上启动了，则停掉
                if (SERVICES.containsKey(cluster)) {
                    SERVICES.remove(cluster).stop();
                }
            }
            ai.incrementAndGet();
        });
        LogUtils.info(log, "{} workers for {}, index {} for {}",
                disWorkers.getWorkerCnt(), allKafkaClusters, disWorkers.getWorkerIdx(), SERVICES.keySet());
    }

    /**
     * 将消费延迟告警发送给相应接收人 calculate-开头发送实时计算 connect-hdfs-开头发送离线计算和算法模型 connect-es开头发送总线相关人员加dba 其他非特殊发送总线相关人员
     */
    private void sendWarn() {
        try {
            LogUtils.info(log, "begin to send alarm, total {}", cache.size());
            for (Map.Entry<String, GroupLag> entry : cache.asMap().entrySet()) {
                // 过滤部分集群
                String message = entry.getValue().getMsg(ignoredClusters);
                if ("".equals(message)) {
                    // 已过期告警，跳过
                    continue;
                }

                if (entry.getKey().startsWith("calculate-")) {
                    if (entry.getKey().equals("calculate-flink-debug") || entry.getKey().equals("flink-seek")) {
                        // debug消费组，忽略
                        continue;
                    }
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), streamWarnReceiver, message);
                } else if (entry.getKey().startsWith("connect-hdfs-")) {
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), batchWarnReceiver, message);
                } else if (entry.getKey().startsWith("connect-druid-")
                        || entry.getKey().startsWith("connect-ignite-")) {
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), storekitWarnReceiver, message);
                } else if (entry.getKey().startsWith("connect-es")) {
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), warnReceiver + "," + esWarnReceiver, message);
                } else if (ignoreGroups.contains(entry.getKey())) {
                    LogUtils.info(log, "{} doesnot need to send a warnning!", entry);
                } else {
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), warnReceiver, message);
                }
            }
        } catch (Exception e) {
            LogUtils.warn(log, "fail to send kafka lag monitor warnning", e);
        }
    }
}
