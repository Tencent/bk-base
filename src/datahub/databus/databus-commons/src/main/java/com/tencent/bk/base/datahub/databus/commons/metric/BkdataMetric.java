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

package com.tencent.bk.base.datahub.databus.commons.metric;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ErrorLogBean;
import com.tencent.bk.base.datahub.databus.commons.bean.EventBean;
import com.tencent.bk.base.datahub.databus.commons.errors.KeyGenException;
import com.tencent.bk.base.datahub.databus.commons.utils.InfluxdbUtils;
import com.tencent.bk.base.datahub.databus.commons.metric.custom.MetricMap;
import com.tencent.bk.base.datahub.databus.commons.metric.custom.Metrics;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.KafkaUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.NamedThreadFactory;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.util.concurrent.TimeUnit;

public class BkdataMetric {

    private static final Logger log = LoggerFactory.getLogger(BkdataMetric.class);

    private ConcurrentHashMap<String, TsdbStat> tsdbStats = new ConcurrentHashMap<>();

    private QueueToKafka eventQueue;

    private QueueToKafka errorLogQueue;

    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("metric") {
        public Thread newThread(Runnable run) {
            Thread thread = Executors.defaultThreadFactory().newThread(run);
            // 需为守护线程，否则会卡主进程
            thread.setDaemon(true);
            return thread;
        }
    });

    private KafkaProducer<String, String> producer = null;
    // tsdb相关配置项，用于上报数据到tsdb中
    private String tsdbUrl = "";
    private String tsdbDbname = "";
    private String tsdbtbname = "";
    private String tsdbUserPass = "";

    // kafka topic相关配置
    private String eventTopic = "";
    private String errorLogTopic = "";

    /**
     * 构造函数
     */
    private BkdataMetric() {
        // 初始化所需要的资源，这里可能上报数据到kafka中，也可能上报到tsdb中，也可能上报到其他存储中
        init();

        // 启动定期上报数据线程
        LogUtils.debug(log, "start the reporting logic");
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 上报数据
                    sendStats();
                } catch (Exception e) {
                    LogUtils.warn(log, "failed to report bkdata metric data in one loop!!", e);
                }
            }
        }, 15, 60, TimeUnit.SECONDS);

        // 增加shutdown hook清理资源
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    exec.shutdown();
                    // 提交还未提交的打点信息
                    sendStats();
                    // 释放资源
                    close();
                } catch (Exception ignore) {
                    LogUtils.warn(log, "exception in shutdown hook for BkdataMetric: ", ignore);
                }
            }
        });
    }


    // 单例模式
    private static class MetricHolder {

        private static final BkdataMetric INSTANCE = new BkdataMetric();
    }

    /**
     * 获取Metric对象
     *
     * @return Metric对象
     */
    public static final BkdataMetric getInstance() {
        return MetricHolder.INSTANCE;
    }

    /**
     * 上报打点数据到tsdb中
     *
     * @param measurement 上报的指标名称
     * @param tagsStr 上报的tags
     * @param fields 上报的字段值
     */
    public void reportTsdbStat(String measurement, String tagsStr, Map<String, Number> fields) {
        reportTsdbStat(measurement, tagsStr, fields, "");
    }

    /**
     * 上报打点数据到tsdb中
     *
     * @param measurement 上报的指标名称
     * @param tagsStr 上报的tags
     * @param fields 上报的字段值
     * @param maxFields 需要统计一个周期内最大值的字段列表，逗号分隔
     */
    public void reportTsdbStat(String measurement, String tagsStr, Map<String, Number> fields, String maxFields) {
        // 生成tsdb数据打点对象
        String key = measurement + "," + tagsStr;
        TsdbStat stat = tsdbStats.get(key);
        if (stat == null) {
            // 可能存在并发访问
            tsdbStats.putIfAbsent(key, new TsdbStat(measurement, tagsStr, maxFields));
            stat = tsdbStats.get(key);
        }

        // 记录打点
        stat.accumulate(fields);
    }

    /**
     * 往队列中增加一事件消息
     */
    public void reportEvent(String clientId, String uid, String type, String extra, String message) {
        String key = String.format("%s|%s|%s", clientId, Utils.getInnerIp(), type);
        Object val = new EventBean(uid, type, extra, message);
        eventQueue.add(key, val);
    }

    /**
     * 上报总线任务错误日志
     *
     * @param uid 上报错误日志的对象uid
     * @param extra 附加信息
     * @param message 消息内容
     */
    public void reportErrorLog(String clientId, String uid, String extra, String message) {
        String key = String.format("%s|%s|%s", clientId, Utils.getInnerIp(), "");
        Object val = new ErrorLogBean(uid, extra, message);
        errorLogQueue.add(key, val);
    }

    /**
     * 初始化一些资源
     */
    private void init() {
        // 初始化需要的资源，从配置文件中获取所需配置项
        tsdbUrl = DatabusProps.getInstance().getOrDefault(Consts.METRIC_TSDB_URL, "");
        tsdbtbname = DatabusProps.getInstance().getOrDefault(Consts.METRIC_TSDB_TBNAME, "");
        tsdbDbname = DatabusProps.getInstance().getOrDefault(Consts.METRIC_TSDB_DBNAME, "");
        String tsdbUser = DatabusProps.getInstance().getOrDefault(Consts.METRIC_TSDB_USER, "");
        String tsdbPass = DatabusProps.getInstance().getOrDefault(Consts.METRIC_TSDB_PASS, "");

        if (StringUtils.isNoneBlank(tsdbUser) && StringUtils.isNoneBlank(tsdbPass)) {
            // 将tsdb的账户和密码用 : 符号连接成一个字符串
            String instanceKey = DatabusProps.getInstance().getOrDefault(Consts.INSTANCE_KEY, "");
            String rootKey = DatabusProps.getInstance().getOrDefault(Consts.ROOT_KEY, "");
            String keyIV = DatabusProps.getInstance().getOrDefault(Consts.KEY_IV, "");
            if (StringUtils.isNoneBlank(instanceKey)) {
                try {
                    tsdbPass = new String(KeyGen.decrypt(tsdbPass, rootKey, keyIV, instanceKey),
                            StandardCharsets.UTF_8);
                } catch (KeyGenException e) {
                    LogUtils.warn(log, "failed to decode tsdb pass!", e);
                }
            }
            tsdbUserPass = tsdbUser + ":" + tsdbPass;
        }

        eventTopic = DatabusProps.getInstance().getOrDefault(Consts.EVENT_TOPIC, Consts.EVENT_TOPIC_DEFAULT);
        errorLogTopic = DatabusProps.getInstance().getOrDefault(Consts.ERROR_LOG_TOPIC, Consts.ERROR_LOG_TOPIC_DEFAULT);
        eventQueue = new QueueToKafka(producer, eventTopic, 200);
        errorLogQueue = new QueueToKafka(producer, errorLogTopic, 200);
        Map<String, Object> result = DatabusProps.getInstance().originalsWithPrefix(Consts.PRODUCER_PREFIX);
        if (result.size() != 0) {
            producer = KafkaUtils.initProducer();
        }
    }

    /**
     * 关闭所有占用的资源
     */
    private void close() {
        // 释放一些占用的资源
        if (producer != null) {
            eventQueue.flush();
            errorLogQueue.flush();
            producer.close(10, TimeUnit.SECONDS);
            producer = null;
        }
    }

    /**
     * 上报各类打点数据到不同目的地
     */
    private void sendStats() {
        // 支持发送打点数据到不同的资源中
        sendCustomMetricStats();
        sendKafkaStats();
        sendTsdbStats();

    }

    /**
     * 实现打点数据上报到tsdb的逻辑
     */
    private void sendTsdbStats() {
        if (StringUtils.isNoneBlank(tsdbDbname) && StringUtils.isNoneBlank(tsdbUrl)) {
            // 当tsdb的地址和db两项配置均存在时，提交数据到tsdb中存储
            ConcurrentHashMap<String, TsdbStat> clone = cloneTsdbStat();
            List<String> records = new ArrayList<>(clone.size());
            // 这里toString获得的字符串是符合tsdb(influxdb)数据格式的一条记录
            clone.forEach((key, value) -> records.add(value.toString()));

            InfluxdbUtils.submitData(tsdbDbname, StringUtils.join(records, "\n"), tsdbUrl, tsdbUserPass);
        }
    }

    /**
     * 实现打点数据上报到kafka的逻辑
     */
    private void sendKafkaStats() {
        Map<String, Object> result = DatabusProps.getInstance().originalsWithPrefix(Consts.PRODUCER_PREFIX);
        try {
            if (producer != null) {
                eventQueue.flush();
                errorLogQueue.flush();
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to construct the producer for metric data. {}", result);
        }
    }

    /**
     * 发送用户自定义监控指标
     */
    private void sendCustomMetricStats() {

        if (StringUtils.isNoneBlank(tsdbDbname) && StringUtils.isNoneBlank(tsdbUrl)) {

            List<String> records = new ArrayList<>();
            MetricMap.concurrentMap().forEach((key, value) -> {
                Metrics metric = value.swap();
                TsdbStat stat = new TsdbStat(tsdbtbname, value.getTags(), "");
                stat.accumulate(metric.toMap());
                records.add(stat.toString());
            });

            InfluxdbUtils.submitData(tsdbDbname, StringUtils.join(records, "\n"), tsdbUrl, tsdbUserPass);
        }
    }

    /**
     * 拷贝tsdb打点数据，并重置打点数据对象，用于下一个周期的数据上报
     *
     * @return tsdb打点数据的拷贝
     */
    private ConcurrentHashMap<String, TsdbStat> cloneTsdbStat() {
        ConcurrentHashMap<String, TsdbStat> tmp = new ConcurrentHashMap<>();
        tsdbStats.forEach((key, value) -> {
            // 只复制在本周期内有数据上报的统计数据对象
            if (value.getReportCount() > 0) {
                tmp.put(key, new TsdbStat(value.getMeasurement(), value.getTags(), value.getMaxFields()));
            }
        });

        // 对调对象，用于下一个周期的数据上报
        ConcurrentHashMap<String, TsdbStat> clone = tsdbStats;
        tsdbStats = tmp;

        return clone;
    }

    private static class QueueToKafka {

        private String topic;
        private KafkaProducer<String, String> producer = null;
        private ArrayBlockingQueue<KafkaStat> msgs;

        /**
         * 构造函数
         *
         * @param topic kafka的topic名称
         * @param capacity 缓存队列容量
         */
        QueueToKafka(KafkaProducer<String, String> producer, String topic, int capacity) {
            this.producer = producer;
            this.topic = topic;
            msgs = new ArrayBlockingQueue<>(capacity);
        }

        /**
         * 往队列中增加一条消息
         *
         * @param val 消息内容
         */
        void add(String key, Object val) {
            KafkaStat stat = new KafkaStat(key, val);
            while (!msgs.offer(stat)) {
                // 添加失败，重试
                LogUtils.warn(log, "failed to add event {} to queue as it is full, will retry after flush", stat);
                // 当队列满时，先将数据发送到kafka中，再尝试添加
                flush();
            }
        }

        /**
         * 将缓存队列中的数据发送到kafka中
         */
        void flush() {
            while (true) {
                KafkaStat stat = msgs.poll();
                if (stat == null) {
                    break;
                } else {
                    String key = stat.getKey();
                    String value = JsonUtils.toJsonWithoutException(stat.getObject());
                    try {
                        producer.send(new ProducerRecord<>(topic, key, value));
                    } catch (Exception e) {
                        LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL, log,
                                String.format("Error when sending message to %s with key: %s, value: %s", topic, key,
                                        value), e);
                    }
                }
            }
        }
    }

    private static class KafkaStat {

        private String key;
        private Object object;

        /**
         * 构造函数
         *
         * @param key kafka的msg key
         * @param object msg
         */
        KafkaStat(String key, Object object) {
            this.key = key;
            this.object = object;
        }

        String getKey() {
            return key;
        }

        Object getObject() {
            return object;
        }

    }
}


