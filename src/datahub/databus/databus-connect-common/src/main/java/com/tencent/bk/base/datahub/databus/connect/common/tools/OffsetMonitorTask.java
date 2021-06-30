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

package com.tencent.bk.base.datahub.databus.connect.common.tools;


import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;

import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.KeyGenException;
import com.tencent.bk.base.datahub.databus.commons.monitor.ConsumerOffsetStat;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.InfluxdbUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JmxUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

public class OffsetMonitorTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(OffsetMonitorTask.class);

    private static final String EMPTY_GROUP = "EMPTY_GROUP";
    private static final String MONITOR_TABLE = "bkdata_kafka_metrics";
    private static final String MONITOR_DB;
    private static final String TSDB_URL;
    private static final String TSDB_USER_PASS;
    private static int TSDB_BATCH_SIZE = 1000;
    private static int LOOP = 0;

    // kafka中的offset相关数据协议定义，参考kafka源码
    private static Field GROUP_FIELD = new Field("group", STRING);
    private static Field TOPIC_FIELD = new Field("topic", STRING);
    private static Field PARTITION_FIELD = new Field("partition", INT32);

    // offset key schema定义
    private static Schema OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(GROUP_FIELD, TOPIC_FIELD, PARTITION_FIELD);

    private static Field OFFSET_FIELD = new Field("offset", INT64);
    private static Field METADATA_FIELD = new Field("metadata", STRING, "Associated metadata.", "");
    private static Field TIMESTAMP_FIELD = new Field("timestamp", INT64);
    private static Field EXPIRE_TIMESTAMP_FIELD = new Field("expire_timestamp", INT64);

    // offset value schema 定义
    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(OFFSET_FIELD, METADATA_FIELD, TIMESTAMP_FIELD);
    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(OFFSET_FIELD, METADATA_FIELD, TIMESTAMP_FIELD,
            EXPIRE_TIMESTAMP_FIELD);

    private static Schema[] VALUE_SCHEMA_ARR = {OFFSET_COMMIT_VALUE_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V1};

    // 用于统计、打点、定期上报数据的对象
    private static final ConcurrentHashMap<String, ConsumerOffsetStat> OFFSET_STAT = new ConcurrentHashMap<>(1000);
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);
    private static final ConcurrentHashMap<String, Long> LOG_END_OFFSET = new ConcurrentHashMap<>(1000);
    private static final ConcurrentHashMap<String, Long> LOG_START_OFFSET = new ConcurrentHashMap<>(1000);

    private OffsetMonitorConfig config;
    private Consumer<byte[], byte[]> consumer;
    private String superUser = "";
    private String superPass = "";
    private int count = 0;
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
    private ConcurrentHashMap<String, Set<ObjectName>> jmxHostToTps;
    private ExecutorService jmxExecPool = Executors.newFixedThreadPool(10);


    static {
        // 获取tsdb的配置信息
        Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
        TSDB_URL = clusterProps.getOrDefault("tsdb.url", "");
        TSDB_USER_PASS = clusterProps.getOrDefault("tsdb.user", "") + ":" + clusterProps.getOrDefault("tsdb.pass", "");
        MONITOR_DB = clusterProps.getOrDefault("tsdb.dbname", "monitor_custom_metrics");
        String batchSize = clusterProps.getOrDefault("tsdb.batch.size", "10");
        try {
            int size = Integer.parseInt(batchSize);
            if (size > 0) {
                TSDB_BATCH_SIZE = size;
            }
        } catch (NumberFormatException ignore) {
            LogUtils.warn(log, "parse int failed. tsdb.batch.size: " + batchSize, ignore);
        }

        // 上报采集的信息
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (StringUtils.isNoneBlank(TSDB_URL)) {
                        commitToTsdb();
                    }
                    if (++LOOP % 60 == 0) {
                        // 清空缓存的数据，避免consumer group已没工作，但一直上报数据
                        OFFSET_STAT.clear();
                        LOOP = 0;
                    }
                } catch (Exception ignore) {
                    LogUtils.warn(log, "exception during submit data to TSDB! ", ignore);
                }
                //LogUtils.info(log, "{}", OFFSET_STAT.keySet().toString());
            }
        }, 30, 60, TimeUnit.SECONDS);
    }

    /**
     * 启动task,初始化资源和属性
     */
    @Override
    public void startTask() {
        rtId = OffsetMonitorConfig.RT_ID_DEFAULT;
        config = new OffsetMonitorConfig(configProperties);

        // 初始化kafka的consumer，指定消费 __consumer_offsets 里的内容
        initConsumer();

        // 初始化通过JMX拉取topic的LogEndOffset数据
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (count++ % 120 == 0) { // 每两个小时
                        initTpToJmxMapping();
                    }
                    collectTpLogOffset();
                } catch (Exception ignore) {
                    LogUtils.warn(log, "exception during submit data to TSDB! ", ignore);
                }
            }
        }, 0, 1, TimeUnit.MINUTES);
    }


    /**
     * 拉取kafka中consumer offset相关数据，对数据进行拆分，发送到目的地的中。
     *
     * @return 处理的结果
     */
    @Override
    public List<SourceRecord> pollData() {
        // 启动任务
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(5000L);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                int count = 0;

                // 当数据的key/value都非空时，继续处理逻辑
                if (record.key() != null && record.value() != null) {
                    ByteBuffer key = ByteBuffer.wrap(record.key());
                    ByteBuffer val = ByteBuffer.wrap(record.value());
                    short keyVer = key.getShort();
                    short valVer = val.getShort();

                    // 检查协议版本，key和value都只支持0、1两个版本号，其他版本的并非offset相关数据
                    if (keyVer >= 0 && keyVer <= 1 && valVer >= 0 && valVer <= 1) {
                        // 读取消费组、topic、分区信息
                        Struct keyStruct = OFFSET_COMMIT_KEY_SCHEMA_V0.read(key);
                        String group = keyStruct.getString(GROUP_FIELD.name);
                        if (StringUtils.isBlank(group)) {
                            group = EMPTY_GROUP;
                        }
                        String topic = keyStruct.getString(TOPIC_FIELD.name);
                        int partition = keyStruct.getInt(PARTITION_FIELD.name);
                        String statKey = config.kafkaServers + "|" + group + "|" + topic + "|" + partition;

                        // 跳过 __consumer_offsets 此topic的offsets相关信息，此topic默认有50个partition
                        if (!topic.equals(Consts.CONSUMER_OFFSET_TOPIC)) {
                            // 读取offset和timestamp信息
                            Struct valStruct = VALUE_SCHEMA_ARR[valVer].read(val);
                            long offset = valStruct.getLong(OFFSET_FIELD.name);
                            long timestamp = valStruct.getLong(TIMESTAMP_FIELD.name);

                            // 更新统计数据
                            ConsumerOffsetStat stat = OFFSET_STAT.get(statKey);
                            if (stat != null) {
                                stat.updateOffsetAndTs(offset, timestamp);
                            } else {
                                stat = new ConsumerOffsetStat(config.kafkaServers, group, topic, partition, offset,
                                        timestamp);
                                OFFSET_STAT.put(statKey, stat);
                            }
                            count = 1; // 已处理
                        }
                    } else {
                        // 不支持的协议版本
                        LogUtils.warn(log, "{}-{}-{} key version {} and value version {} not supported!",
                                config.kafkaServers, record.partition(), record.offset(), keyVer, valVer);
                    }
                }

                Metric.getInstance()
                        .updateStat(config.kafkaServers, config.connector, Consts.CONSUMER_OFFSET_TOPIC, "kafka", count,
                                0, "0", "0");
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, "error during pull consumer offsets data", ignore);
        }

        return null;
    }


    /**
     * 停止任务
     */
    @Override
    public void stopTask() {
        exec.shutdown();
        try {
            exec.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LogUtils.warn(log, "get interrupted exception!", e);
        }
    }

    /**
     * 初始化kafka消费者
     */
    private void initConsumer() {
        // 设置consumer的属性
        Properties props = new Properties();
        props.putAll(BasicProps.getInstance().getConsumerProps());

        props.put("group.id", "kafka.metric");
        props.put("bootstrap.servers", config.kafkaServers);
        props.put("exclude.internal.topics", "false"); // 设置后方可以读取__consumer_offsets里的内容
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        // 设置sasl验证
        if (config.useSaslAuth) {
            Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
            if (clusterProps.containsKey(Consts.SASL_USER)) {
                superUser = clusterProps.get(Consts.SASL_USER);
            }
            if (clusterProps.containsKey(Consts.SASL_PASS)) {
                String encodedPass = clusterProps.get(Consts.SASL_PASS);
                String instanceKey = clusterProps.get(Consts.INSTANCE_KEY);
                String rootKey = clusterProps.getOrDefault(Consts.ROOT_KEY, "");
                String keyIV = clusterProps.getOrDefault(Consts.KEY_IV, "");
                try {
                    superPass = new String(KeyGen.decrypt(encodedPass, rootKey, keyIV, instanceKey));
                } catch (KeyGenException ex) {
                    LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR,
                            rtId + " failed to decode pass. key: " + instanceKey + " pass: " + encodedPass, ex);
                }
            }
            // 设置sasl账号信息
            if (StringUtils.isNoneBlank(superUser) && StringUtils.isNoneBlank(superPass)) {
                props.put("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + superUser
                                + "\" password=\"" + superPass + "\";");
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "SCRAM-SHA-512");
            } else {
                LogUtils.warn(log, "bad SASL settings in config. user: {}, password: {}", superUser, superPass);
            }
        }

        // 构建kafka的消费者，订阅默认的__consumer_offsets队列
        consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe(Arrays.asList(Consts.CONSUMER_OFFSET_TOPIC));
    }

    /**
     * 通过jmx获取数据，初始化kafka中topic-partition到kafka中broker的jmx的映射
     */
    private void initTpToJmxMapping() {
        List<String> kafkaHosts = HttpUtils.getKafkaHosts(config.kafkaServers);
        LogUtils.info(log, "{} hosts are {}", config.kafkaServers, kafkaHosts);

        jmxHostToTps = new ConcurrentHashMap<>(6);
        for (String host : kafkaHosts) {
            // 通过jmx获取服务器上的和log相关的列表，参考：kafka.log:type=Log,name=LogEndOffset,partition=0,topic=table_xxx_xx
            JMXConnector jmxConnector = JmxUtils.getJmxConnector(host, config.jmxPort);
            try {
                // 获取LogEndOffset和LogStartOffset的mbean集合
                MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
                Set<ObjectName> endOffsetBeans = mbeanConn
                        .queryNames(new ObjectName("kafka.log:type=Log,name=LogEndOffset,*"), null);
                Set<ObjectName> startOffsetBeans = mbeanConn
                        .queryNames(new ObjectName("kafka.log:type=Log,name=LogStartOffset,*"), null);
                endOffsetBeans.addAll(startOffsetBeans);

                LogUtils.info(log, "offset beans: {}:{}:{} {}", config.kafkaServers, host, config.jmxPort,
                        endOffsetBeans);
                JmxUtils.closeJmxConnector(jmxConnector);
                jmxHostToTps.put(host, endOffsetBeans);
            } catch (Exception ignore) {
                LogUtils.warn(log,
                        String.format("unable to connect %s jmx by %s:%s! ", config.kafkaServers, host, config.jmxPort),
                        ignore);
            }
        }
    }

    /**
     * 通过调用jmx获取指定的topic-partition当前的LogEndOffset
     */
    private void collectTpLogOffset() {
        long start = System.currentTimeMillis();
        List<Future> tasks = new ArrayList<>(jmxHostToTps.size());
        for (String host : jmxHostToTps.keySet()) {
            Future future = jmxExecPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        JMXConnector jmxConnector = JmxUtils.getJmxConnector(host, config.jmxPort);
                        MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
                        Set<ObjectName> beanSet = jmxHostToTps.getOrDefault(host, new HashSet<>());
                        for (ObjectName bean : beanSet) {
                            String kafkaTopicKey = config.kafkaServers + "|" + bean.getKeyProperty("topic") + "|" + bean
                                    .getKeyProperty("partition");
                            Object value = mbeanConn.getAttribute(bean, "Value");
                            if (bean.getKeyProperty("name").equals("LogEndOffset")) {
                                LOG_END_OFFSET.put(kafkaTopicKey, (Long) value);
                            } else {
                                LOG_START_OFFSET.put(kafkaTopicKey, (Long) value);
                            }
                        }
                        JmxUtils.closeJmxConnector(jmxConnector);
                    } catch (Exception ignore) {
                        LogUtils.warn(log,
                                String.format("failed to get LogEndOffset by jmx %s:%s", host, config.jmxPort), ignore);
                    }
                }
            });

            tasks.add(future);
        }

        // 等待任务执行完毕
        for (Future future : tasks) {
            try {
                future.get();
            } catch (Exception ignore) {
                LogUtils.warn(log, String.format("%s failed to get log offset by jmx", config.kafkaServers), ignore);
            }
        }
        LogUtils.info(log, "getting LogOffset for {} on {} hosts, starts {}, takes {}", config.kafkaServers,
                jmxHostToTps.keySet().size(), start, System.currentTimeMillis() - start);
    }

    /**
     * 提交数据到tsdb中
     */
    private static void commitToTsdb() {
        long start = System.currentTimeMillis();
        int count = 0;
        StringBuilder sb = new StringBuilder();
        for (ConsumerOffsetStat stat : OFFSET_STAT.values()) {
            // 构建tag部分
            sb.append(MONITOR_TABLE).append(",kafka=").append(stat.getKafka()).append(",topic=")
                    .append(stat.getTopic());
            sb.append(",partition=").append(stat.getPartition()).append(",group=").append(stat.getGroup()).append(" ");

            // 获取topic-partition的当前logEndOffset，计算Lag值。
            long offset = stat.getOffset();
            String kafkaTopicKey = stat.getKafka() + "|" + stat.getTopic() + "|" + stat.getPartition();
            long logEndOffset = LOG_END_OFFSET.getOrDefault(kafkaTopicKey, offset);
            logEndOffset = logEndOffset > offset ? logEndOffset : offset;
            long logStartOffset = LOG_START_OFFSET.getOrDefault(kafkaTopicKey, 0L);

            // 构建field部分
            sb.append("offset=").append(offset).append("i,").append("start=").append(logStartOffset).append("i,");
            sb.append("end=").append(logEndOffset).append("i,lag=").append(logEndOffset - offset).append("i ");

            // 补全时间戳为纳秒
            sb.append(stat.getTimestamp()).append("000000").append("\n");

            count++;
            if (count == TSDB_BATCH_SIZE) {
                sb.deleteCharAt(sb.length() - 1); // 删除最后一个多余的换行符
                InfluxdbUtils.submitData(MONITOR_DB, sb.toString(), TSDB_URL, TSDB_USER_PASS);
                sb = new StringBuilder();
                count = 0;
            }
        }

        // 可能还有记录未提交
        if (count > 0) {
            sb.deleteCharAt(sb.length() - 1); // 删除最后一个多余的换行符
            InfluxdbUtils.submitData(MONITOR_DB, sb.toString(), TSDB_URL, TSDB_USER_PASS);
        }

        LogUtils.info(log, "commit {} records to TSDB starts {}, takes {}", OFFSET_STAT.values().size(), start,
                System.currentTimeMillis() - start);
    }
}
