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

package com.tencent.bk.base.datahub.hubmgr.job;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ALARM_TAG;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_CLUSTER_STATUS_CHECK;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_CLUSTER_STATUS_CHECK_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAHUB_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_START_CONNECTOR;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_START_CONNECTOR_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_START_DATANODE_CONNECTOR;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_START_DATANODE_CONNECTOR_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_CONNECTOR_REPORT_TOPIC;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_CONNECTOR_REPORT_TOPIC_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUS_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUS_SHIPPER_STORAGES;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUS_SHIPPER_STORAGES_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.KAFKA_BOOTSTRAP_SERVERS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ADMIN;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ALARM_SHIPPER_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ALARM_SHIPPER_TYPE_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.TASK_FAIL_MAX;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.TASK_FAIL_MAX_DEFAULT;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeLock;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStatusCheck implements Job {

    private static final Logger log = LoggerFactory.getLogger(ClusterStatusCheck.class);
    private static final long LOCK_SEC = 5;
    private final int taskFailMax = DatabusProps.getInstance().getOrDefault(TASK_FAIL_MAX, TASK_FAIL_MAX_DEFAULT);

    private Set<String> storages;
    private Map<String, String> abnormalClusters;
    private List<String> unsignedList;
    private String checkRestUrl;
    private String startRestUrl;
    private String startDatanodeUrl;
    private String receivers;
    private String tag;
    private String topic;
    private String extraShipperType;

    /**
     * 初始化必要的变量
     */
    private void initVariables() {
        DatabusProps props = DatabusProps.getInstance();
        // 执行实际的检查集群逻辑
        String apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        String path = props.getOrDefault(API_CLUSTER_STATUS_CHECK, API_CLUSTER_STATUS_CHECK_DEFAULT);
        String startPath = props.getOrDefault(API_START_CONNECTOR, API_START_CONNECTOR_DEFAULT);
        String shipperStorages = props.getOrDefault(DATABUS_SHIPPER_STORAGES, DATABUS_SHIPPER_STORAGES_DEFAULT);

        storages = Arrays.stream(StringUtils.split(shipperStorages, ",")).collect(Collectors.toSet());
        checkRestUrl = String.format("http://%s%s", apiDns, path);
        startRestUrl = String.format("http://%s%s", apiDns, startPath);

        String startDatanodePath = props
                .getOrDefault(API_START_DATANODE_CONNECTOR, API_START_DATANODE_CONNECTOR_DEFAULT);
        startDatanodeUrl = String.format("http://%s%s", apiDns, startDatanodePath);

        receivers = props.getOrDefault(DATABUS_ADMIN, "");
        extraShipperType = props.getOrDefault(STOREKIT_ALARM_SHIPPER_TYPE, STOREKIT_ALARM_SHIPPER_TYPE_DEFAULT);
        tag = props.getOrDefault(ALARM_TAG, "");
        topic = props.getOrDefault(DATABUSMGR_CONNECTOR_REPORT_TOPIC, DATABUSMGR_CONNECTOR_REPORT_TOPIC_DEFAULT);

        abnormalClusters = new HashMap<>();
        unsignedList = new ArrayList<>();
    }

    /**
     * 获取存储名称
     *
     * @param originalStorage 原始存储名称
     * @return 存储名称
     */
    private String mappingStorage(String originalStorage) {
        if ("eslog".equals(originalStorage)) {
            return "es";
        } else if ("clean".equals(originalStorage)) {
            return "kafka";
        } else {
            return originalStorage;
        }
    }

    /**
     * 构造kafka producer
     *
     * @return kafka producer
     */
    private Producer<String, String> buildProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", DatabusProps.getInstance().getOrDefault(KAFKA_BOOTSTRAP_SERVERS, ""));
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        LogUtils.info(log, "initProducer: kafka_producer: {}", producerProps);

        return new KafkaProducer<String, String>(producerProps);
    }

    /**
     * 发送kafka消息
     *
     * @param msgValue 消息内容
     */
    private void sendKafkaMessage(String msgValue) {
        try {
            Producer<String, String> producer = buildProducer();
            ProducerRecord<String, String> msg = new ProducerRecord<>(topic, msgValue);
            producer.send(msg);
            producer.close(10, TimeUnit.SECONDS);

        } catch (Exception e) {
            LogUtils.warn(log, "failed to send info to kafka", e);
        }
    }

    /**
     * 根据集群名返回警告接收人
     *
     * @param clusterName 总线集群名称
     * @param defaultReceivers 默认警告接收人
     * @return 警告接收人
     */
    private String getClusterReceivers(String clusterName, String defaultReceivers) {
        String[] typeArray = extraShipperType.split(",");
        return Stream.of(typeArray).anyMatch(clusterName::startsWith)
                ? DatabusProps.getInstance().getOrDefault(STOREKIT_ADMIN, "") : defaultReceivers;
    }

    /**
     * 处理单个集群检查结果
     *
     * @param clusterName 集群名称
     * @param cluster 集群检查结果
     * @param rec 告警接收人
     */
    private void processClusterResult(String clusterName, ClusterCheckResult cluster, String rec) {
        Map<String, List<String>> abnormalConnectors = new HashMap<>();
        for (Map.Entry<String, String> entry1 : cluster.badConnectors.entrySet()) {
            abnormalConnectors.computeIfAbsent(entry1.getValue(), k -> new ArrayList<>()).add(entry1.getKey());
        }

        if (abnormalConnectors.containsKey("FAILED")) {
            if (abnormalConnectors.get("FAILED").size() > taskFailMax) {
                MgrNotifyUtils.sendFatalAlert(this.getClass(), rec, String.format("[%s]总线集群 %s 检查发现失败任务数超过%s个，请关注！！",
                        CommUtils.getDatetime(), clusterName, taskFailMax));
            }
        }

        // 状态异常任务数大于0, 检查失败任务数大于0, 丢失任务数大于0
        if (abnormalConnectors.size() > 0 || cluster.fail.size() > 0 || cluster.missing.size() > 0) {
            StringBuilder sb = buildInitMessage(clusterName, cluster);
            // 丢失任务告警
            if (cluster.missing.size() > 0) {
                // 正常丢失任务会自动重启
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), rec, tag + "总线集群 " + clusterName + " 丢失任务，请关注!");

                if (cluster.connectorCount > 0) {
                    // 当集群任务数大于0时重启丢失任务；注意，等于0时，系统可能出现问题，没有返回信息
                    for (String taskName : cluster.missing) {
                        LogUtils.info(log, "start to start missing task: {}", taskName);
                        String[] arr = taskName.split("-");
                        if (storages.contains(arr[0])) {
                            String storage = mappingStorage(arr[0]);
                            Map<String, Object> param = ImmutableMap.of("result_table_id", arr[1].substring(6),
                                    "storages", Arrays.asList(storage));
                            HttpUtils.postAndCheck(startRestUrl, param);
                        } else if (clusterName.startsWith("puller-datanode")) {
                            int idx = arr[1].indexOf("_");
                            HttpUtils.postAndCheck(startDatanodeUrl,
                                    ImmutableMap.of("result_table_id", arr[1].substring(idx + 1)));
                        } else {
                            sb.append("【未重启】");
                        }
                    }
                }
                sb.append("丢失任务列表(").append(cluster.missing.size()).append(")\n");
                sb.append(StringUtils.join(cluster.missing, ",")).append("\n");
            }

            if (cluster.ghost.size() > 0) {
                sb.append("未正常停止任务数：").append(cluster.ghost.size()).append("\n");
            }

            buildAbnormalMessage(sb, clusterName, abnormalConnectors);
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), rec, sb.toString());
        }
    }

    /**
     * 构造初始信息
     *
     * @param name 集群名称
     * @param cluster 集群检查结果
     * @return 字符串builder
     */
    private StringBuilder buildInitMessage(String name, ClusterCheckResult cluster) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(CommUtils.getDatetime()).append("][").append(tag).append("]总线集群 ");
        sb.append(name).append(" 检查发现失败任务，请关注!\n");
        sb.append("集群总任务数：").append(cluster.connectorCount).append("\n");
        if (!cluster.restartResult) {
            sb.append("重启\n").append(cluster.restartConnector).append("：").append(cluster.restartMessage).append("\n");
        }

        return sb;
    }

    /**
     * 构造异常信息
     *
     * @param sb 字符串builder
     * @param name 集群名称
     * @param abnormalConnectors 异常任务
     */
    private void buildAbnormalMessage(StringBuilder sb, String name, Map<String, List<String>> abnormalConnectors) {
        // 异常任务告警
        if (abnormalConnectors.size() > 0) {
            sb.append("状态异常任务信息\n");
            if (abnormalConnectors.containsKey("UNASSIGNED")) {
                int size = abnormalConnectors.get("UNASSIGNED").size();
                sb.append("UNASSIGNED任务数：").append(size).append("\n");
                unsignedList.add("|" + name + "|" + size);
                abnormalConnectors.remove("UNASSIGNED");
            }
            for (Map.Entry<String, List<String>> entry1 : abnormalConnectors.entrySet()) {
                sb.append(entry1.getKey()).append("任务(").append(entry1.getValue().size());
                sb.append(")：").append(StringUtils.join(entry1.getValue(), ",")).append("\n");
            }
        }
    }

    /**
     * 总线集群状态检查作业，用于触发检查所有总线集群中的任务状态，并解析检查结果。
     *
     * @param context 作业执行上下文
     * @throws JobExecutionException 作业执行异常
     */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        initVariables();
        long triggerTime = context.getFireTime().getTime();
        LogUtils.info(log, "databus cluster status check triggered at {}", triggerTime);
        // 集群检查是怕他性任务，只能在一个实例上被触发，这里首先通过zk获取锁
        String lockPath = String.format("/databusmgr/lock/%s", ClusterStatusCheck.class.getSimpleName());
        try (DistributeLock lock = new DistributeLock(lockPath)) {
            if (lock.lock(LOCK_SEC)) {
                ApiResult result = HttpUtils.getApiResult(checkRestUrl);
                if (result.isResult()) {
                    LogUtils.info(log, "finish databus cluster status check. {}",
                            JsonUtils.toJsonWithoutException(result.getData()));
                    // 对应检查失败的集群，告警出来
                    Map<String, Map<String, Object>> data = (Map<String, Map<String, Object>>) result.getData();
                    for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
                        String clusterName = entry.getKey();
                        ClusterCheckResult cluster = new ClusterCheckResult(entry.getValue());
                        if (cluster.warning) {
                            // 集群检查异常
                            abnormalClusters.put(clusterName, cluster.errorMsg);
                            continue;
                        }

                        processClusterResult(clusterName, cluster, getClusterReceivers(clusterName, receivers));
                    }

                    if (abnormalClusters.size() > 0) {
                        MgrNotifyUtils.sendFatalAlert(this.getClass(), receivers, tag + " 总线集群检查，检查失败集群列表： "
                                + JsonUtils.toJsonWithoutException(abnormalClusters));
                    }

                    if (unsignedList.size() > 0) {
                        long tm = System.currentTimeMillis() / 1000;
                        String msg = unsignedList.stream().map(item -> tm + item).collect(Collectors.joining("`"));
                        sendKafkaMessage(msg);
                    }
                } else {
                    String msg = JsonUtils.toJsonWithoutException(result);
                    LogUtils.warn(log, "failed to check databus cluster status, bad response! {}", msg);
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, tag + " 总线集群检查失败 " + msg);
                }

                // 确保占用锁的时间超过LOCK_SEC的时间，避免释放锁太快导致其他进程里的job获取到锁，重复执行
                long duration = System.currentTimeMillis() - triggerTime;
                if (duration < (10 + LOCK_SEC) * 1000) {
                    try {
                        Thread.sleep((10 + LOCK_SEC) * 1000 - duration);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            } else {
                // 获取执行锁失败
                LogUtils.info(log, "unable to get a lock to execute job logic!");
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to run databus cluster status check job!", e);
            // 集群状态检查失败时，需要通知管理员
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                    tag + " 总线集群检查失败 " + ExceptionUtils.getStackTrace(e));
            throw new JobExecutionException(e);
        }
    }


    static class ClusterCheckResult {

        private final boolean warning;
        private final String errorMsg;
        private final int connectorCount;
        private final List<String> fail;
        private final List<String> missing;
        private final List<String> ghost;
        private final Map<String, String> badConnectors;
        private final Map<String, Object> restarted;
        private final boolean restartResult;
        private final String restartConnector;
        private final String restartMessage;

        /**
         * 构造函数
         *
         * @param cluster 单个集群检查结果
         */
        ClusterCheckResult(Map<String, Object> cluster) {
            warning = (boolean) cluster.get("warnning");
            errorMsg = (String) cluster.get("error_msg");
            connectorCount = (int) cluster.get("connector_count");
            fail = (List<String>) cluster.get("check_failed");
            missing = (List<String>) cluster.get("check_missing");
            ghost = (List<String>) cluster.get("ghost_tasks");
            badConnectors = (Map<String, String>) cluster.get("bad_connectors");
            if (cluster.containsKey("restarted")) {
                restarted = (Map) cluster.get("restarted");
                restartResult = (boolean) restarted.get("result");
                restartConnector = (String) restarted.get("connector");
                restartMessage = (String) restarted.get("message");
            } else {
                restarted = new HashMap<>();
                restartResult = true;
                restartConnector = "";
                restartMessage = "";
            }
        }
    }
}
