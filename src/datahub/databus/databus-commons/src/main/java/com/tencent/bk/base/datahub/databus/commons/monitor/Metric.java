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

package com.tencent.bk.base.datahub.databus.commons.monitor;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ClusterStatBean;
import com.tencent.bk.base.datahub.databus.commons.bean.ErrorLogBean;
import com.tencent.bk.base.datahub.databus.commons.bean.EventBean;
import com.tencent.bk.base.datahub.databus.commons.bean.IdBean;
import com.tencent.bk.base.datahub.databus.commons.callback.MetricProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnPool;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;

import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Metric {

    private static final Logger log = LoggerFactory.getLogger(Metric.class);

    private ConcurrentHashMap<String, ResultTableStat> connStats;
    private ConcurrentHashMap<String, Map<String, Object>> badMsgStats;
    private KafkaProducer<String, String> producer;
    private String metricTopic;
    private String badMsgTopic;
    private String clusterStatTopic;
    private String eventTopic;
    private String errorLogTopic;
    private QueueToKafka eventQueue;
    private QueueToKafka errorLogQueue;
    private String cluster = "";
    private String module = "";
    private String component = "";
    private String msgKeyPrefix;
    private long maxMessageSize;


    /**
     * 创建metric对象,初始化
     */
    private Metric() {
        connStats = new ConcurrentHashMap<>();
        badMsgStats = new ConcurrentHashMap<>();
        metricTopic = BasicProps.getInstance().getMetricTopic();
        badMsgTopic = BasicProps.getInstance().getBadMsgTopic();
        clusterStatTopic = BasicProps.getInstance().getDatabusClusterStatTopic();
        eventTopic = BasicProps.getInstance().getDatabusEventTopic();
        errorLogTopic = BasicProps.getInstance().getDatabusErrorLogTopic();
        maxMessageSize = Long.parseLong(
                BasicProps.getInstance().getMonitorProps().getOrDefault("producer.max.request.size", "3145728"));

        // 检查打点数据kafka的配置，如果配置异常，则将数据记录到日志中。
        Map<String, Object> producerProps = new HashMap<>();
        try {
            producerProps.putAll(BasicProps.getInstance().getMetricKafkaProps());
            producer = new KafkaProducer<String, String>(producerProps);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to construct the producer for metric data. {}", producerProps);
        }
        Map<String, String> metricProps = BasicProps.getInstance().getMonitorProps();
        // 通过配置获取此集群的模块名称、组件名称等
        module = metricProps.getOrDefault(Consts.MODULE_NAME, Consts.MODULE_NAME_DEFAULT);
        component = metricProps.getOrDefault(Consts.COMP_NAME, Consts.COMP_DEFAULT);
        cluster = BasicProps.getInstance().getClusterProps().getOrDefault(BkConfig.GROUP_ID, Consts.CLUSTER_DEFAULT);

        // 初始化缓存总线事件和错误消息的对象
        msgKeyPrefix = cluster + "|" + Utils.getInnerIp() + "|";
        eventQueue = new QueueToKafka(msgKeyPrefix, eventTopic, 200);
        errorLogQueue = new QueueToKafka(msgKeyPrefix, errorLogTopic, 500);

        // 启动定期上报数据线程
        LogUtils.debug(log, "start the reporting logic");
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ConcurrentHashMap<String, ResultTableStat> metricStat = cloneMetricStat();
                    storeMetricData(metricStat);
                    eventQueue.flush();
                    errorLogQueue.flush();
                    sendBadEtlMsg();
                } catch (Exception e) {
                    LogUtils.warn(log, "failed to report metric data in one loop!!" + e.getMessage());
                }
            }
        }, 15, 60, TimeUnit.SECONDS);

        // 增加shutdown hook清理资源
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    // 提交还未提交的打点信息
                    ConcurrentHashMap<String, ResultTableStat> metricStat = cloneMetricStat();
                    storeMetricData(metricStat);
                    eventQueue.flush();
                    errorLogQueue.flush();
                    sendBadEtlMsg();
                    // 清理数据库连接，kafka producer等资源
                    exec.shutdown();
                    producer.close(10, TimeUnit.SECONDS);
                    ConnPool.shutdown();
                } catch (Exception ignore) {
                    LogUtils.warn(log, "exception in shutdown hook for Metric: ", ignore);
                }
            }
        });
    }

    // 单例模式
    private static class MetricHolder {

        private static final Metric INSTANCE = new Metric();
    }

    public static final Metric getInstance() {
        return MetricHolder.INSTANCE;
    }

    /**
     * 初始化rtStat统计对象
     */
    public void initRtStatIfNecessary(String rtId, String connector, String topic, String clusterType) {
        ResultTableStat stat = connStats.get(connector);
        if (stat == null) {
            stat = new ResultTableStat(rtId, connector, topic, clusterType);
            connStats.putIfAbsent(connector, stat);
        }
    }

    /**
     * 更新topic中数据处理进度，上报一些统计数据.
     */
    public void updateStat(String rtId, String connector, String topic, String clusterType, int count, long msgSize,
            String inputTag, String outTag) {
        ResultTableStat stat = connStats.get(connector);
        if (stat == null) {
            synchronized (this) {
                connStats.putIfAbsent(connector, new ResultTableStat(rtId, connector, topic, clusterType));
                stat = connStats.get(connector);
            }
        }
        stat.processedRecords(count, msgSize, inputTag, outTag);
    }

    /**
     * 更新错误信息
     */
    public void updateTopicErrInfo(String connector, List<String> errors) {
        ResultTableStat stat = connStats.get(connector);
        if (stat == null) {
            // 不应该发生这种异常情况，忽略掉设置
            LogUtils.warn(log, "updateTopicErrInfo failed, stat for {} is null", connector);
        } else {
            stat.updateErrorCount(errors);
        }
    }

    /**
     * 更新延迟信息
     */
    public void updateDelayInfo(String connector, long now, long max, long min) {
        ResultTableStat stat = connStats.get(connector);
        if (stat == null) {
            // 不应该发生这种异常情况，忽略掉设置
            LogUtils.warn(log, "updateDelayInfo failed, stat for {} is null", connector);
        } else {
            stat.updateDelayCount(now, max, min);
        }
    }

    /**
     * 更新清洗失败的消息记录
     *
     * @param rtId rtId
     * @param dataId dataId
     * @param msgKey kafka消息的key
     * @param msgVal kafka消息的value
     * @param partition kafka消息的分区
     * @param offset kafka消息的offset
     * @param error 清洗失败的error信息
     */
    public void setBadEtlMsg(String rtId, int dataId, String msgKey, String msgVal, int partition, long offset,
            String error, long timestamp) {
        if (!badMsgStats.containsKey(rtId)) {
            // 一个上报周期内，只接受一条清洗失败消息的上报，如已存在此类消息，则周期内后续上报数据均丢弃
            Map<String, Object> map = new HashMap<>();
            map.put(Consts.RTID, rtId);
            map.put(Consts.DATAID, dataId);
            map.put(Consts.MSGKEY, msgKey);
            map.put(Consts.MSGVALUE, msgVal);
            map.put(Consts.PARTITION, partition);
            map.put(Consts.OFFSET, offset);
            map.put(Consts.ERROR, error);
            map.put(Consts.TIMESTAMP, timestamp);
            badMsgStats.putIfAbsent(rtId, map);
        }
    }

    /**
     * 上报总线任务事件
     *
     * @param uid 上报事件的对象uid
     * @param type 事件类型
     * @param extra 附加信息
     * @param message 消息内容
     */
    public void reportEvent(String uid, String type, String extra, String message) {
        eventQueue.add(new EventBean(uid, type, extra, message));
    }

    /**
     * 上报总线任务错误日志
     *
     * @param uid 上报错误日志的对象uid
     * @param extra 附加信息
     * @param message 消息内容
     */
    public void reportErrorLog(String uid, String extra, String message) {
        errorLogQueue.add(new ErrorLogBean(uid, extra, message));
    }

    /**
     * 获取本机IP
     */
    public String getWorkerIp() {
        return Utils.getInnerIp();
    }

    /**
     * 拷贝打点数据,并重置打点数据对象。
     *
     * @return 打点数据的拷贝
     */
    private ConcurrentHashMap<String, ResultTableStat> cloneMetricStat() {
        ConcurrentHashMap<String, ResultTableStat> tmp = new ConcurrentHashMap<>();
        for (String connector : connStats.keySet()) {
            ResultTableStat stat = connStats.get(connector);
            // 如果此connector在上一个周期内有数据上报，则新建一个打点数据对象，否则删除此打点数据对象
            if (stat.getCount() > 0) {
                ResultTableStat newStat = new ResultTableStat(stat.getRtId(), stat.getConnector(), stat.getTopic(),
                        stat.getClusterType());
                tmp.put(connector, newStat);
            }
        }
        // 对调对象，将当前状态数据返回用于上报打点数据
        ConcurrentHashMap<String, ResultTableStat> clone = connStats;
        connStats = tmp;

        return clone;
    }

    /**
     * 构建打点数据,发送到kafka中
     *
     * @param metricStat 打点数据对象
     */
    private void storeMetricData(ConcurrentHashMap<String, ResultTableStat> metricStat) {
        // 改为unix的时间戳(秒)
        Long now = (System.currentTimeMillis() / 1000);

        long msgCount = 0;
        long msgSize = 0;
        long recordCount = 0;
        long totalByteSize = 0;
        // 首先尝试将数据写入到kafka中，如果失败，则记录到日志文件中
        List<String> allList = new ArrayList<>();
        for (ResultTableStat stat : metricStat.values()) {
            Map<String, Object> map = new LinkedHashMap<>(8);
            map.put(Consts.TIME, now); // 上报的时间戳
            map.put(Consts.VERSION, "2.0"); // version信息
            map.put(Consts.INFO, getInfo(stat)); // info
            map.put(Consts.METRICS, getMetrics(now, stat)); // metrics
            String statInfo = JsonUtils.toJsonWithoutException(map);
            totalByteSize += statInfo.getBytes(StandardCharsets.UTF_8).length;
            if (totalByteSize >= maxMessageSize) {
                String statInfoList = generateStatInfo(allList);
                producer.send(new ProducerRecord<>(metricTopic, statInfoList),
                        new MetricProducerCallback(metricTopic, "", statInfoList));
                allList.clear();
                LogUtils.info(log, statInfoList);
            }
            allList.add(statInfo);
            // 累加此次上报中的打点数据
            msgCount += stat.getCount();
            msgSize += stat.getMsgSize();
            recordCount += stat.getTotalCount();
        }
        if (!allList.isEmpty()) {
            String statInfoList = generateStatInfo(allList);
            producer.send(new ProducerRecord<>(metricTopic, statInfoList),
                    new MetricProducerCallback(metricTopic, "", statInfoList));
            LogUtils.info(log, statInfoList);
        }

        // 上报集群汇总数据
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        ClusterStatBean bean = new ClusterStatBean(msgCount, msgSize, recordCount, totalMemory, freeMemory, maxMemory);
        String clusterStat = JsonUtils.toJsonWithoutException(bean);
        producer.send(new ProducerRecord<>(clusterStatTopic, msgKeyPrefix, clusterStat),
                new MetricProducerCallback(clusterStatTopic, msgKeyPrefix, clusterStat));
    }

    private String generateStatInfo(List<String> statInfoList) {
        StringBuilder statInfoBuild = new StringBuilder("[");
        for (String statInfo : statInfoList) {
            statInfoBuild.append(statInfo).append(",");
        }
        statInfoBuild.deleteCharAt(statInfoBuild.length() - 1).append("]");
        return statInfoBuild.toString();
    }

    /**
     * 发送清洗失败的数据到kafka中
     */
    private void sendBadEtlMsg() {
        String msgKey = Consts.COLLECTOR_DS + "=" + (System.currentTimeMillis() / 1000);
        ConcurrentHashMap<String, Map<String, Object>> tmp = badMsgStats;
        badMsgStats = new ConcurrentHashMap<>();
        for (Map<String, Object> map : tmp.values()) {
            String msgVal = JsonUtils.toJsonWithoutException(map);
            // 当badmsg topic非默认值时，按照 badMsgTopic + "_" + dataId 的topic 格式发送清洗异常消息
            String topic = badMsgTopic.equals(Consts.DEFAULT_BAD_MSG_TOPIC) ? badMsgTopic
                    : String.format("%s_%s", badMsgTopic, map.get(Consts.DATAID));
            producer.send(new ProducerRecord<>(topic, msgKey, msgVal),
                    new MetricProducerCallback(topic, msgKey, msgVal));
        }
    }

    /**
     * 构建rt统计数据的info部分的数据
     *
     * @param stat rt统计数据
     * @return rt统计数据的info部分数据
     */
    private Map<String, Object> getInfo(ResultTableStat stat) {
        Map<String, Object> info = new LinkedHashMap<>(10);
        info.put(Consts.MODULE, module);
        info.put(Consts.COMPONENT, component);
        info.put(Consts.CLUSTER, cluster);
        info.put(Consts.STORAGE, getStorage(stat));
        info.put(Consts.LOGICAL_TAG, getLogicalTag(stat));
        info.put(Consts.PHYSICAL_TAG, getPhysicalTag(stat));
        info.put(Consts.CUSTOM_TAGS, getCustomTag(stat));

        return info;
    }

    /**
     * 构造{"tag": {"a": "v1", "b": "v2"}, "desc": "v1|v2"}的结构并返回
     *
     * @param tagDesc tag描述信息
     * @return 基于tagDesc构建的标签结构数据
     */
    private Map<String, Object> composeTag(Map<String, Object> tagDesc) {
        Map<String, Object> tag = new LinkedHashMap<>(4);
        tag.put(Consts.TAG, StringUtils.join(tagDesc.values(), "|"));
        tag.put(Consts.DESC, tagDesc);

        return tag;
    }

    /**
     * 构建rt统计数据中info里的logical_tag部分
     *
     * @param stat rt统计数据
     * @return info里的logical_tag部分
     */
    private Map<String, Object> getLogicalTag(ResultTableStat stat) {
        Map<String, Object> logicalTagDesc = new LinkedHashMap<>(4);
        logicalTagDesc.put(Consts.RESULT_TABLE_ID, stat.getRtId());

        return composeTag(logicalTagDesc);
    }

    /**
     * 构建rt统计数据中info里的physical_tag部分
     *
     * @param stat rt统计数据
     * @return info里的physical_tag部分
     */
    private Map<String, Object> getPhysicalTag(ResultTableStat stat) {
        Map<String, Object> physicalTagDesc = new LinkedHashMap<>(4);
        physicalTagDesc.put(Consts.CONNECTOR, stat.getConnector());
        physicalTagDesc.put(Consts.WORKER_IP, Utils.getInnerIp());

        return composeTag(physicalTagDesc);
    }

    /**
     * 构建rt统计数据中info里的custom_tag部分
     *
     * @param stat rt统计数据
     * @return info里的custom_tag部分
     */
    private Map<String, Object> getCustomTag(ResultTableStat stat) {
        Map<String, Object> customTagDesc = new LinkedHashMap<>(4);
        customTagDesc.put(Consts.WORKER_IP, Utils.getInnerIp());

        return customTagDesc;
    }

    /**
     * 构建rt统计数据中info里的storage部分
     *
     * @param stat rt统计数据
     * @return info里的storage部分
     */
    private Map<String, Object> getStorage(ResultTableStat stat) {
        Map<String, Object> storage = new LinkedHashMap<>(4);
        storage.put(Consts.CLUSTER_TYPE, stat.getClusterType());

        return storage;
    }


    /**
     * 构建rt统计数据中metrics部分数据
     *
     * @param time 当前时间戳
     * @param stat rt统计数据
     * @return rt统计数据的metrics部分
     */
    private Map<String, Object> getMetrics(long time, ResultTableStat stat) {
        Map<String, Object> metrics = new LinkedHashMap<>(8);
        metrics.put(Consts.DATA_MONITOR, getDataMonitor(stat));
        metrics.put(Consts.RESOURCE_MONITOR, getResouceMonitor(stat));
        metrics.put(Consts.CUSTOM_METRICS, getCustomMetrics(time, stat));

        return metrics;
    }

    /**
     * 构建rt统计数据中data_monitor里的数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的数据
     */
    private Map<String, Object> getDataMonitor(ResultTableStat stat) {
        Map<String, Object> dataMonitor = new LinkedHashMap<>(4);
        dataMonitor.put(Consts.DATA_LOSS, getDataLoss(stat));
        dataMonitor.put(Consts.DATA_DELAY, getDataDelay(stat));

        return dataMonitor;
    }

    /**
     * 构建rt统计数据中data_monitor里的data_loss部分数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的data_loss部分数据
     */
    private Map<String, Object> getDataLoss(ResultTableStat stat) {
        Map<String, Object> dataLoss = new LinkedHashMap<>(8);
        dataLoss.put(Consts.INPUT, getDataLossInput(stat));
        dataLoss.put(Consts.OUTPUT, getDataLossOutput(stat));
        dataLoss.put(Consts.DATA_DROP, getDataLossDataDrop(stat));

        return dataLoss;
    }

    /**
     * 构建rt统计数据中data_monitor里的data_loss->input部分数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的data_loss->input部分数据
     */
    private Map<String, Object> getDataLossInput(ResultTableStat stat) {
        Map<String, Object> dataLossInputTags = new LinkedHashMap<>();

        Map<String, AtomicInteger> inputTagTimes = stat.getInputTagsCount();
        long inputCnt = 0;
        for (Map.Entry<String, AtomicInteger> entry : inputTagTimes.entrySet()) {
            dataLossInputTags.put(entry.getKey(), entry.getValue());
            inputCnt += entry.getValue().longValue();
        }

        Map<String, Object> dataLossInput = new LinkedHashMap<>(8);
        if (dataLossInputTags.size() == 0) {
            // 若没有input, 则返回空
            return dataLossInput;
        }

        dataLossInput.put(Consts.TAGS, dataLossInputTags);
        dataLossInput.put(Consts.TOTAL_CNT, inputCnt);
        dataLossInput.put(Consts.TOTAL_CNT_INCREMENT, inputCnt);

        return dataLossInput;
    }

    /**
     * 构建rt统计数据中data_monitor里的data_loss->output部分数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的data_loss->output部分数据
     */
    private Map<String, Object> getDataLossOutput(ResultTableStat stat) {
        Map<String, Object> dataLossOutputTags = new LinkedHashMap<>();

        Map<String, AtomicInteger> tagTimes = stat.getOutputTagsCount();
        long outputCnt = 0;
        for (Map.Entry<String, AtomicInteger> entry : tagTimes.entrySet()) {
            dataLossOutputTags.put(entry.getKey(), entry.getValue());
            outputCnt += entry.getValue().longValue();
        }

        Map<String, Object> dataLossOutput = new LinkedHashMap<>(8);
        if (dataLossOutputTags.size() == 0) {
            // 若没有output, 则返回空
            return dataLossOutput;
        }

        dataLossOutput.put(Consts.TAGS, dataLossOutputTags);
        dataLossOutput.put(Consts.TOTAL_CNT, outputCnt);
        dataLossOutput.put(Consts.TOTAL_CNT_INCREMENT, outputCnt);

        return dataLossOutput;
    }

    /**
     * 构建rt统计数据中data_monitor里的data_loss->data_drop部分数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的data_loss->data_drop部分数据
     */
    private Map<String, Object> getDataLossDataDrop(ResultTableStat stat) {
        Map<String, Object> dataLossDrop = new LinkedHashMap<>();

        Map<String, AtomicInteger> errors = stat.getErrCounts();
        for (Map.Entry<String, AtomicInteger> entry : errors.entrySet()) {
            Map<String, Object> error = new HashMap<>();
            error.put(Consts.CNT, entry.getValue().intValue());
            error.put(Consts.REASON, entry.getKey());
            dataLossDrop.put(entry.getKey(), error);
        }

        return dataLossDrop;
    }

    /**
     * 构建rt统计数据中data_monitor里的data_delay部分数据
     *
     * @param stat rt统计数据
     * @return data_monitor里的data_delay部分数据
     */
    private Map<String, Object> getDataDelay(ResultTableStat stat) {
        long delayNow = stat.getDelayNow();
        long delayMinTime = stat.getDelayMinTime();
        long delayMaxTime = stat.getDelayMaxTime();

        Map<String, Object> dataDelay = new LinkedHashMap<>(8);
        if (delayMaxTime == 0) {
            return dataDelay; //如果没有就不要输出了，免得产生比较大的脏数据
        }

        Map<String, Long> minDelay = new HashMap<>();
        minDelay.put(Consts.OUTPUT_TIME, delayNow);
        minDelay.put(Consts.DATA_TIME, delayMinTime);
        minDelay.put(Consts.DELAY_TIME, delayNow - delayMinTime);

        Map<String, Long> maxDelay = new HashMap<>();
        maxDelay.put(Consts.OUTPUT_TIME, delayNow);
        maxDelay.put(Consts.DATA_TIME, delayMaxTime);
        maxDelay.put(Consts.DELAY_TIME, delayNow - delayMaxTime);

        dataDelay.put(Consts.MIN_DELAY, minDelay);
        dataDelay.put(Consts.MAX_DELAY, maxDelay);
        dataDelay.put(Consts.WINDOW_TIME, 0);
        dataDelay.put(Consts.WAITING_TIME, 0);

        return dataDelay;
    }

    /**
     * 构建rt统计数据中resource_monitor里的数据
     *
     * @param stat rt统计数据
     * @return resource_monitor里的数据
     */
    private Map<String, Object> getResouceMonitor(ResultTableStat stat) {
        return new HashMap<>();
    }

    /**
     * 构建rt统计数据中custom_metrics里的数据
     *
     * @param time 当前时间戳
     * @param stat rt统计数据
     * @return custom_metrics里的数据
     */
    private Map<String, Object> getCustomMetrics(long time, ResultTableStat stat) {
        Map<String, Object> tags = new LinkedHashMap<>(8);
        tags.put(Consts.RESULT_TABLE_ID, stat.getRtId());
        tags.put(Consts.WORKER_IP, Utils.getInnerIp());
        tags.put(Consts.CONNECTOR, stat.getConnector());

        Map<String, Object> processMetric = new LinkedHashMap<>(8);
        processMetric.put(Consts.RECORD_COUNT, stat.getTotalCount());
        processMetric.put(Consts.MSG_COUNT, stat.getCount());
        processMetric.put(Consts.MSG_SIZE, stat.getMsgSize());
        processMetric.put(Consts.TAGS, tags);

        Map<String, Object> customMetrics = new LinkedHashMap<>(4);
        customMetrics.put(Consts.TIME, time);
        customMetrics.put(Consts.DATABUS_PROCESS_METRIC, processMetric);

        return customMetrics;
    }


    private class QueueToKafka {

        private String keyPrefix;
        private String topic;
        private ArrayBlockingQueue<IdBean> msgs;

        /**
         * 构造函数
         *
         * @param keyPrefix kafka的msg key的前缀
         * @param topic kafka的topic名称
         * @param capacity 缓存队列容量
         */
        QueueToKafka(String keyPrefix, String topic, int capacity) {
            this.keyPrefix = keyPrefix;
            this.topic = topic;
            msgs = new ArrayBlockingQueue<>(capacity);
        }

        /**
         * 往队列中增加一条消息
         *
         * @param val 消息内容
         */
        void add(IdBean val) {
            if (!msgs.offer(val)) {
                // 添加失败，重试
                LogUtils.warn(log, "failed to add message {} to queue as it is full, will retry after flush", val);
                // 当队列满时，先将数据发送到kafka中，再尝试添加
                flush();
                add(val);
            }
        }

        /**
         * 将缓存队列中的数据发送到kafka中
         */
        void flush() {
            while (true) {
                IdBean val = msgs.poll();
                if (val == null) {
                    break;
                } else {
                    String key = keyPrefix + val.getUid();
                    String value = JsonUtils.toJsonWithoutException(val);
                    producer.send(new ProducerRecord<>(topic, key, value),
                            new MetricProducerCallback(topic, key, value));
                }
            }
        }
    }

}