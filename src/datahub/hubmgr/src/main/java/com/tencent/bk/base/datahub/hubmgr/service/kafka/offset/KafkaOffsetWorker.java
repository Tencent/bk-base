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

package com.tencent.bk.base.datahub.hubmgr.service.kafka.offset;

import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.STRING;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.LoadingCache;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.errors.KeyGenException;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.service.Service;
import com.tencent.bk.base.datahub.hubmgr.utils.JmxUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOffsetWorker implements Service {

    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetWorker.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String API_SERVICE_STATUS = "/v1/catalog/service/";
    private static final String EMPTY_GROUP = "EMPTY_GROUP";
    private static final String MONITOR_TABLE = "bkdata_kafka_metrics";
    private static final String LOGIN_MODULE =
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
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
    private final long startTime = System.currentTimeMillis();
    private final long offsetTimeout = DatabusProps.getInstance()
            .getOrDefault(MgrConsts.KAFKA_OFFSET_COMMIT_TIMEOUT, MgrConsts.KAFKA_OFFSET_COMMIT_TIMEOUT_DEFAULT) * 1000;
    // 用于统计、打点、定期上报数据的对象
    private final ConcurrentHashMap<String, ConsumerOffsetStat> offsetStat = new ConcurrentHashMap<>(1000);
    private final ConcurrentHashMap<String, Long> logEndOffset = new ConcurrentHashMap<>(1000);
    private final ConcurrentHashMap<String, Long> logStartOffset = new ConcurrentHashMap<>(1000);
    private AtomicBoolean isStop = new AtomicBoolean(false);
    private long timeout = 2000L;
    private Consumer<byte[], byte[]> consumer;
    private String bootstrapServer;
    private boolean useSaslAuth;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private String jmxPort = DatabusProps.getInstance().getOrDefault(MgrConsts.KAFKA_JMX_PORT, "8080");
    private int count = 0;
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(5);
    private ConcurrentHashMap<String, Set<ObjectName>> jmxHostToTps;
    private ExecutorService jmxExecPool = Executors.newFixedThreadPool(10);

    private LoadingCache<String, GroupLag> cache;
    private long configLag;
    private int configPercent;

    /**
     * 构造函数
     */
    public KafkaOffsetWorker(String bootstrapServer, LoadingCache cache) {
        this.bootstrapServer = bootstrapServer;

        DatabusProps props = DatabusProps.getInstance();
        jmxHostToTps = new ConcurrentHashMap<>(6);

        useSaslAuth = false;
        String[] clusters = props.getOrDefault(MgrConsts.KAFKA_SASL_CLUSTER, "").split(",");
        for (String cluster : clusters) {
            if (cluster.equals(bootstrapServer)) {
                useSaslAuth = true;
            }
        }

        // 用于记录消费组下延迟过大的消费信息，当超过2分钟没有被访问或者更新会被刷新（消费组已经不再延迟）
        this.cache = cache;

        configLag = props.getOrDefault(MgrConsts.LAG_TO_WARN, 10000L);
        configPercent = props.getOrDefault(MgrConsts.PERCENT_TO_WARN, 10);
    }

    @Override
    public String getServiceName() {
        return "KafkaOffsetWorker: " + bootstrapServer;
    }

    @Override
    public void start() {
        // 初始化kafka的consumer，指定消费 __consumer_offsets 里的内容
        initConsumer();

        // 初始化通过JMX拉取topic的LogEndOffset数据
        exec.scheduleAtFixedRate(() -> {
            try {
                if (count++ % 120 == 0 || jmxHostToTps.isEmpty()) {
                    // 每两个小时重新初始化一次jmx
                    initTpToJmxMapping();
                }
                collectTpLogOffset();
            } catch (Exception e) {
                LogUtils.warn(log, "exception during submit data to TSDB! ", e);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // 创建子线程消费并处理数据
        executorService.submit(() -> {
            LogUtils.info(log, "starting poll-handler loop for {}", MgrConsts.CONSUMER_OFFSET_TOPIC);
            while (!isStop.get() && DatabusMgr.isRunning()) {
                try {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                    if (!records.isEmpty()) {
                        processData(records);
                        consumer.commitSync();
                    }
                } catch (Exception e) {
                    LogUtils.warn(log, "failed to poll-handler data in a loop!", e);
                }
            }
            consumer.close(5, TimeUnit.SECONDS);
            LogUtils.info(log, "finish processing msg in {}", MgrConsts.CONSUMER_OFFSET_TOPIC);
        });

        // 上报采集的信息
        exec.scheduleAtFixedRate(() -> {
            try {
                commitToTsdb();
            } catch (Exception e) {
                LogUtils.warn(log, "exception during submit data to TSDB! ", e);
            }
            //LogUtils.info(log, "{}", offsetStat.keySet().toString());
        }, 30, 60, TimeUnit.SECONDS);
    }

    private void processData(ConsumerRecords<byte[], byte[]> records) {
        try {
            for (ConsumerRecord<byte[], byte[]> record : records) {
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
                        String statKey = bootstrapServer + "|" + group + "|" + topic + "|" + partition;

                        // 跳过 __consumer_offsets 此topic的offsets相关信息，此topic默认有50个partition
                        if (!topic.equals(MgrConsts.CONSUMER_OFFSET_TOPIC)) {
                            reportOffset(valVer, val, statKey, group, topic, partition);
                        }
                    } else {
                        // 不支持的协议版本
                        LogUtils.warn(log, "{}-{}-{} key version {} and value version {} not supported!",
                                bootstrapServer, record.partition(), record.offset(), keyVer, valVer);
                    }
                }
            }
        } catch (Exception e) {
            LogUtils.warn(log, "error during process consumer offsets data", e);
        }
    }

    /**
     * 上报offset信息
     */
    private void reportOffset(short valVer, ByteBuffer val, String statKey, String group, String topic, int partition) {
        // 读取offset和timestamp信息
        Struct valStruct = VALUE_SCHEMA_ARR[valVer].read(val);
        long offset = valStruct.getLong(OFFSET_FIELD.name);
        long timestamp = valStruct.getLong(TIMESTAMP_FIELD.name);

        // 更新统计数据
        ConsumerOffsetStat stat = offsetStat.get(statKey);
        if (stat != null) {
            stat.updateOffsetAndTs(offset, timestamp);
        } else {
            stat = new ConsumerOffsetStat(bootstrapServer, group, topic, partition, offset, timestamp);
            offsetStat.put(statKey, stat);

            if (timestamp - startTime >= offsetTimeout) {
                // TODO: 上报新增事件
                TsdbWriter.getInstance().reportData(MgrConsts.DATABUSMGR_OFFSET_COMMIT_TOPIC_DEFAULT,
                        "kafka=" + bootstrapServer + ",topic=" + topic + ",partition="
                                + partition + ",group=" + group, "uid=\"new_committed\"", timestamp / 1000);
            }
        }
    }

    @Override
    public void stop() {
        LogUtils.info(log, "going to stop {} ", getServiceName());
        // 首先标记状态，然后触发executor的shutdown
        isStop.set(true);
        executorService.shutdown();

        // 等待executor结束执行，并关闭consumer
        try {
            if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
                LogUtils.warn(log, "failed to shutdown executor in timeout seconds");
            }
            TsdbWriter.getInstance().flush();
        } catch (InterruptedException e) {
            // ignore
        }

        exec.shutdown();
        try {
            exec.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LogUtils.warn(log, "get interrupted exception!", e);
        }
    }

    /**
     * 提交数据到tsdb中
     */
    private void commitToTsdb() {
        long start = System.currentTimeMillis();
        int count = 0;
        StringBuilder sb = new StringBuilder();
        long currentTimeMillis = System.currentTimeMillis();
        for (Map.Entry<String, ConsumerOffsetStat> entry : offsetStat.entrySet()) {
            ConsumerOffsetStat stat = entry.getValue();
            if (currentTimeMillis - stat.getTimestamp() >= offsetTimeout) {
                // TODO： 上报中断事件
                offsetStat.remove(entry.getKey());
                TsdbWriter.getInstance().reportData(MgrConsts.DATABUSMGR_OFFSET_COMMIT_TOPIC_DEFAULT,
                        "kafka=" + stat.getKafka() + ",topic=" + stat.getTopic() + ",partition=" + stat.getPartition()
                                + ",group=" + stat.getGroup(), "uid=\"stop_committed\"", stat.getTimestamp() / 1000);

            }

            // 构建tag部分
            sb.append(MONITOR_TABLE).append(",kafka=").append(stat.getKafka()).append(",topic=").append(stat.getTopic())
                    .append(",partition=").append(stat.getPartition())
                    .append(",group=").append(stat.getGroup()).append(" ");

            // 获取topic-partition的当前logEndOffset，计算Lag值。
            long offset = stat.getOffset();
            String kafkaTopicKey = stat.getKafka() + "|" + stat.getTopic() + "|" + stat.getPartition();
            long logEndOffset = this.logEndOffset.getOrDefault(kafkaTopicKey, offset);
            logEndOffset = logEndOffset > offset ? logEndOffset : offset;
            long logStartOffset = this.logStartOffset.getOrDefault(kafkaTopicKey, 0L);
            long lag = logEndOffset - offset;

            // 构建field部分
            sb.append("offset=").append(offset).append("i,").append("start=").append(logStartOffset).append("i,");
            sb.append("end=").append(logEndOffset).append("i,lag=").append(lag).append("i ");

            // 补全时间戳为纳秒
            sb.append(stat.getTimestamp()).append("000000").append("\n");

            if (lag > configLag && logEndOffset != logStartOffset) {
                double p = (lag * 100.0) / (logEndOffset - logStartOffset);
                if (p >= configPercent) {
                    try {
                        GroupLag groupLag = cache.get(stat.getGroup());
                        groupLag.addLag(stat, lag, p);
                    } catch (ExecutionException e) {
                        LogUtils.warn(log, "fail to regist lag info");
                    }
                }
            }

            count++;
            if (count == MgrConsts.TSDB_BATCH_SIZE) {
                sb.deleteCharAt(sb.length() - 1); // 删除最后一个多余的换行符
                TsdbWriter.getInstance().reportData(sb.toString());
                sb = new StringBuilder();
                count = 0;
            }
        }

        // 可能还有记录未提交
        if (count > 0) {
            sb.deleteCharAt(sb.length() - 1); // 删除最后一个多余的换行符
            TsdbWriter.getInstance().reportData(sb.toString());
        }

        LogUtils.info(log, "commit {} records to TSDB starts {}, takes {}, cluster {}",
                offsetStat.values().size(), start, System.currentTimeMillis() - start, bootstrapServer);
    }

    /**
     * 初始化kafka消费者
     */
    private void initConsumer() {
        // 设置consumer的属性
        Properties props = new Properties();
        props.putAll(DatabusProps.getInstance().originalsWithPrefix("consumer."));

        props.put("group.id", "kafka.metric");
        props.put("bootstrap.servers", bootstrapServer);
        props.put("exclude.internal.topics", "false"); // 设置后方可以读取__consumer_offsets里的内容
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        // 设置sasl验证
        if (useSaslAuth) {
            DatabusProps databusProps = DatabusProps.getInstance();
            String superUser = databusProps.getOrDefault(MgrConsts.KAFKA_SASL_USER, "");
            String encodedPass = databusProps.getOrDefault(MgrConsts.KAFKA_SASL_PASS, "");
            String key = databusProps.getOrDefault(MgrConsts.INSTANCE_KEY, "");
            String rootKey = databusProps.getOrDefault(Consts.ROOT_KEY, "");
            String keyIV = databusProps.getOrDefault(Consts.KEY_IV, "");

            String superPass = "";
            try {
                superPass = new String(KeyGen.decrypt(encodedPass, rootKey, keyIV, key), StandardCharsets.UTF_8);
            } catch (KeyGenException ex) {
                LogUtils.error(MgrConsts.ERRCODE_DECODE_PASS_FAIL, log,
                        "failed to decode pass. key: " + key + " pass: " + encodedPass, ex);
            }
            // 设置sasl账号信息
            if (StringUtils.isNoneBlank(superUser) && StringUtils.isNoneBlank(superPass)) {
                String jaas = String.format(LOGIN_MODULE, superUser, superPass);
                props.put("sasl.jaas.config", jaas);
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "SCRAM-SHA-512");
            } else {
                LogUtils.warn(log, "bad SASL settings in config. user: {}, password: {}", superUser, superPass);
            }
        }

        // 构建kafka的消费者，订阅默认的__consumer_offsets队列
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(MgrConsts.CONSUMER_OFFSET_TOPIC));
    }

    /**
     * 获取consul服务状态的url地址
     *
     * @return consul服务状态的url地址
     */
    private String getConsulServiceUrl() {
        String dns = DatabusProps.getInstance().getProperty(MgrConsts.CONSUL_DNS);
        return "http://" + dns + API_SERVICE_STATUS;
    }

    /**
     * 通过接口获取kafka集群对应的机器IP列表
     *
     * @param kafkaBsServers kafka集群的地址
     * @return 集群机器的IP列表
     */
    private List<String> getKafkaHosts(String kafkaBsServers) {
        List<String> hosts = new ArrayList<>();
        try {
            String[] arr = StringUtils.split(kafkaBsServers, ".");
            String url = getConsulServiceUrl();
            LogUtils.info(log, "get url={}, arr={}", url, arr);
            if (arr.length == 4) {
                url += arr[0] + "?dc=" + arr[2];
            } else if (arr.length == 5) {
                url += arr[1] + "?dc=" + arr[3] + "&tag=" + arr[0];
            } else {
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(),
                        DatabusProps.getInstance().getOrDefault(MgrConsts.DATABUS_ADMIN, ""),
                        "未识别kafka域名：" + kafkaBsServers);
                return null;
            }
            String result = HttpUtils.get(url);
            LogUtils.warn(log, "url {}, result {}", url, result);

            ArrayNode jsonArr = (ArrayNode) MAPPER.readTree(result);
            for (JsonNode node : jsonArr) {
                hosts.add(node.get("Address").asText());
            }

        } catch (IOException | RuntimeException ignore) {
            LogUtils.warn(log, "failed to get kafka hosts for {}", kafkaBsServers);
            return null;
        }

        return hosts;
    }

    /**
     * 通过jmx获取数据，初始化kafka中topic-partition到kafka中broker的jmx的映射
     */
    private void initTpToJmxMapping() {
        List<String> kafkaHosts = getKafkaHosts(bootstrapServer);
        LogUtils.info(log, "{} hosts are {}", bootstrapServer, kafkaHosts);

        if (kafkaHosts == null) {
            LogUtils.warn(log, "failed to get kafka host!");
            return;
        }

        jmxHostToTps = new ConcurrentHashMap<>(6);
        for (String host : kafkaHosts) {
            try {
                // 通过jmx获取服务器上的和log相关的列表，参考：kafka.log:type=Log,name=LogEndOffset,partition=0,topic=table_xxx_xx
                JMXConnector jmxConnector = JmxUtils.getJmxConnector(host, jmxPort);
                if (jmxConnector == null) {
                    throw new IllegalStateException("jmx connector failed!");
                }
                // 获取LogEndOffset和LogStartOffset的mbean集合
                MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
                ObjectName end = new ObjectName("kafka.log:type=Log,name=LogEndOffset,*");
                Set<ObjectName> endOffsetBeans = mbeanConn.queryNames(end, null);
                ObjectName start = new ObjectName("kafka.log:type=Log,name=LogStartOffset,*");
                Set<ObjectName> startOffsetBeans = mbeanConn.queryNames(start, null);
                endOffsetBeans.addAll(startOffsetBeans);

                LogUtils.info(log, "offset beans: {}:{}:{} {}", bootstrapServer, host, jmxPort, endOffsetBeans);
                JmxUtils.closeJmxConnector(jmxConnector);
                jmxHostToTps.put(host, endOffsetBeans);
            } catch (Exception e) {
                LogUtils.warn(log, String.format("unable to connect %s jmx by %s:%s! ",
                        bootstrapServer, host, jmxPort), e);
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
            Future future = jmxExecPool.submit(() -> {
                try {
                    JMXConnector jmxConnector = JmxUtils.getJmxConnector(host, jmxPort);
                    if (jmxConnector == null) {
                        throw new IllegalStateException("jmx connector failed!");
                    }
                    MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
                    Set<ObjectName> beanSet = jmxHostToTps.getOrDefault(host, new HashSet<>());
                    for (ObjectName bean : beanSet) {
                        String kafkaTopicKey =
                                bootstrapServer + "|" + bean.getKeyProperty("topic") + "|" + bean
                                        .getKeyProperty("partition");
                        Object value = mbeanConn.getAttribute(bean, "Value");
                        if (bean.getKeyProperty("name").equals("LogEndOffset")) {
                            logEndOffset.put(kafkaTopicKey, (Long) value);
                        } else {
                            logStartOffset.put(kafkaTopicKey, (Long) value);
                        }
                    }
                    JmxUtils.closeJmxConnector(jmxConnector);
                } catch (Exception e) {
                    LogUtils.warn(log, String.format("failed to get LogEndOffset by jmx %s:%s", host, jmxPort), e);
                }
            });

            tasks.add(future);
        }

        // 等待任务执行完毕
        for (Future future : tasks) {
            try {
                future.get();
            } catch (Exception e) {
                LogUtils.warn(log, String.format("%s failed to get log offset by jmx", bootstrapServer), e);
            }
        }
        LogUtils.info(log, "getting LogOffset for {} on {} hosts, starts {}, takes {}",
                bootstrapServer, jmxHostToTps.keySet().size(), start, System.currentTimeMillis() - start);
    }
}
