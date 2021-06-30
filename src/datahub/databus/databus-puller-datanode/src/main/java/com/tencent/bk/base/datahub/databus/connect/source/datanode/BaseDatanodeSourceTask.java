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

package com.tencent.bk.base.datahub.databus.connect.source.datanode;

import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.INodeTransform;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.TransformRecord;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.TransformResult;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.exceptons.TransformException;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseDatanodeSourceTask extends BkSourceTask implements TaskContextChangeCallback {

    private static final Logger log = LoggerFactory.getLogger(BaseDatanodeSourceTask.class);
    private static final String SOURCE_RESULT_TABLE_IDS = "source_result_table_ids";
    private static final String CONFIG = "config";
    private static final String NODE_TYPE = "node_type";
    private static final String MERGE_TYPE = "merge";
    private static final String SPLIT_TYPE = "split";
    private static final int RETRY_TIMES = 3;
    private static final AtomicLong recordCountTotal = new AtomicLong(0);
    private static final AtomicLong recordSizeTotal = new AtomicLong(0);

    /**
     * 数据转换对象，在子类中赋值，用于对原始数据进行简单的处理，
     */
    protected INodeTransform transform;
    protected DatanodeSourceConfig config;
    private String[] sourceRtArr;
    private String datanodeConfig;
    private boolean isTaskContextChanged = false;
    //Map<RT, ConsumerRecordPuller>
    private Map<String, ConsumerRecordPuller> rtPuller = new HashMap<>();
    private Producer<byte[], byte[]> producer;
    private String destTopic;
    private long lastCheckTime;
    private long lastLogCount = 0;
    private int maxPollCount = 500;


    /**
     * 启动task,初始化资源和属性
     */
    @Override
    public void startTask() {
        config = new DatanodeSourceConfig(configProperties);
        Map<String, String> configMap = HttpUtils.getDatanodeConfig(rtId);
        sourceRtArr = configMap.getOrDefault(SOURCE_RESULT_TABLE_IDS, "").split(",");
        datanodeConfig = configMap.getOrDefault(CONFIG, "");
        Map<String, String> consumerConfProps = BasicProps.getInstance().getConsumerProps();
        String maxPollRecords = consumerConfProps.getOrDefault("max.poll.records", "500");
        try {
            maxPollCount = Integer.parseInt(maxPollRecords);
        } catch (Exception ignore) {
            // 配置异常，使用默认值，忽略错误
        }
        // 检查固化节点配置，如果检查失败，抛出异常终止任务
        validateOrFail();
        // 初始化kafka producer
        destTopic = "table_" + rtId;
        String producerBsServer = ctx.getKafkaBsServer();
        LogUtils.info(log, "{} going to init producer for {} for topic {}", rtId, producerBsServer, destTopic);
        producer = createProducer(producerBsServer);

        // 初始化转换对象, 在子类中赋值
        transform = getNodeTransformer(config.connector, datanodeConfig, converter, ctx);
        transform.configure();
        lastCheckTime = System.currentTimeMillis();
        refreshPuller();
        // 注册回调函数
        ZkUtils.registerCtxChangeCallback(rtId, this);
    }

    @Override
    public void markBkTaskCtxChanged() {
        LogUtils.info(log, "{} task rt config is changed, going to reload rt config!", rtId);
        isTaskContextChanged = true;
    }

    /**
     * 从consumer中批量获取数据，经过数据的转换，发送到目的地中
     *
     * @return 处理的结果
     */
    @Override
    public List<SourceRecord> pollData() {
        while (!isStop.get()) {
            if (isTaskContextChanged) {
                Map<String, String> configMap = HttpUtils.getDatanodeConfig(rtId);
                LogUtils.info(log, "get datanode config: {}", configMap);
                String type = configMap.get(NODE_TYPE);
                if (MERGE_TYPE.equals(type)) {
                    sourceRtArr = configMap.getOrDefault("source_result_table_ids", "").split(",");
                    // 检查固化节点配置，如果检查失败，抛出异常终止任务
                    validateOrFail();
                    // 初始化资源，获取源rt的kafka地址，创建对应的consumer
                    refreshPuller();
                } else if (SPLIT_TYPE.equals(type)) {
                    datanodeConfig = configMap.get("config");
                    transform.refreshConfig(datanodeConfig);
                }
                isTaskContextChanged = false;
            }

            // 定期对比源rt的schema，如果schema不同，则中断任务执行，抛出异常
            if (System.currentTimeMillis() - lastCheckTime >= 900000) {
                LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", recordCountTotal, recordSizeTotal,
                        rtId);
                validateOrFail();
                lastCheckTime = System.currentTimeMillis();
            }

            // 每消费一定数量的数据，输出日志记录下
            if (recordCountTotal.get() - lastLogCount >= 10000) {
                LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", recordCountTotal, recordSizeTotal,
                        rtId);
                lastLogCount = recordCountTotal.get();
            }

            sleepQuietly(TimeUnit.SECONDS.toMillis(30));
        }
        // 无需记录offset相关信息
        return null;
    }


    /**
     * 处理转换后的结果，发送到目标topic
     *
     * @param resultRecord 转换后的结果
     */
    private void handleTransformResult(TransformResult resultRecord) {
        if (resultRecord != null) {
            //多rt处理逻辑
            for (TransformRecord recordEntry : resultRecord) {
                String tmpBid = recordEntry.getBid();
                String destTopicTemp =
                        StringUtils.isNotBlank(tmpBid) ? "table_" + tmpBid + "_" + rtId.split("_", 2)[1] : destTopic;
                ConsumerRecord<byte[], byte[]> tmpRecord = recordEntry.getRecord();
                LogUtils.debug(log, "begin to send kafka info topic:{}, tmpRecord:{}", destTopicTemp, tmpRecord);
                producer.send(new ProducerRecord<>(destTopicTemp, tmpRecord.key(), tmpRecord.value()),
                        new ProducerCallback(destTopicTemp, tmpRecord.partition(), tmpRecord.key(), tmpRecord.value()));
            }
        }
    }


    /**
     * 停止task
     */
    @Override
    protected void stopTask() {
        for (ConsumerRecordPuller puller : rtPuller.values()) {
            puller.stop();
        }
        try {
            Thread.sleep(1000);
            producer.flush();
            producer.close(10, TimeUnit.SECONDS);
        } catch (Exception ignor) {
            LogUtils.warn(log, "failed to close producer!");
        }
    }


    /**
     * 获取固化节点数据转换器
     *
     * @param config connector节点配置
     * @param converter 数据格式转换器
     * @param ctx task上下文
     * @return INodeTransform 转换器
     */
    protected abstract INodeTransform getNodeTransformer(String connector, String config,
            Converter converter, TaskContext ctx);

    /**
     * 休眠一段时间
     *
     * @param millis 秒数
     */
    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            // just ignore
        }
    }

    /**
     * 检查固化节点的配置，检查未通过时，抛出异常中断任务执行
     */
    private void validateOrFail() {
        if (!validateSourceRtSchema()) {
            this.stopTask();
            String msg = String.format("%s found different schema in %s, going to stop task", rtId,
                    StringUtils.join(sourceRtArr, ","));
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.DATANODE_SCHEMA_MISMATCH, log, msg);
            throw new ConnectException(msg);
        }
    }

    /**
     * 检查固化节点的schema，如果schema和父节点的schema不同，则返回false
     *
     * @return true/false，是否固化节点的schema和所有父节点相同
     */
    private boolean validateSourceRtSchema() {
        Map<String, String> rtProps = getRtInfoWithRetry(rtId);
        String columnsInfo = rtProps.get(Consts.COLUMNS);
        for (String rt : sourceRtArr) {
            rtProps = getRtInfoWithRetry(rt);
            String sourceColumnInfo = rtProps.get(Consts.COLUMNS);
            if (!columnsInfo.equals(sourceColumnInfo)) {
                LogUtils.warn(log, "{}'s schema {} is different with source rt {}'s schema {}", rtId, columnsInfo, rt,
                        sourceColumnInfo);
                return false;
            }
        }
        // 所有的source rt和当前rt的schema均相同
        return true;
    }

    private Map<String, String> getRtInfoWithRetry(String rtId) {
        int retryCount = 0;
        Map<String, String> rtProps = null;
        do {
            retryCount++;
            try {
                rtProps = HttpUtils.getRtInfo(rtId);
            } catch (Exception e) {
                LogUtils.warn(log, "{} execute get rtInfo failed! error:{}", rtId, e.getMessage());
                if (retryCount <= RETRY_TIMES) {
                    sleepQuietly(TimeUnit.SECONDS.toMillis(3));
                }
            }
        } while (null == rtProps && retryCount <= RETRY_TIMES);

        if (rtProps == null) {
            throw new ConnectException("Retry 3 time to get rtInfo failed!");
        }
        return rtProps;
    }


    /**
     * 刷新
     */
    private void refreshPuller() {
        // 初始化资源，获取源rt的kafka地址，创建对应的consumer
        Set<String> sourceRtSet = new HashSet<>();
        for (String sourceRt : sourceRtArr) {
            Map<String, String> rtInfo = getRtInfoWithRetry(sourceRt);
            String bsServer = rtInfo.get(Consts.BOOTSTRAP_SERVERS);
            String topic = "table_" + sourceRt;
            LogUtils.info(log, "{} going to init consumers for {}", rtId, topic);
            initConsumerRecordPuller(topic, bsServer);
            sourceRtSet.add(topic);
        }
        List<String> remove = new ArrayList<>();
        for (Entry<String, ConsumerRecordPuller> entry : rtPuller.entrySet()) {
            if (!sourceRtSet.contains(entry.getKey())) {
                remove.add(entry.getKey());
            }
        }
        remove.forEach(k -> rtPuller.remove(k).stop());
    }

    /**
     * 初始化kafka消息记录puller
     *
     * @param topic kafka 的topic
     * @param bsServer kafka 地址
     */
    private void initConsumerRecordPuller(String topic, String bsServer) {
        // 初始化consumer，订阅topic
        try {
            ConsumerRecordPuller puller = rtPuller.get(topic);
            if (puller == null) {
                Consumer<byte[], byte[]> consumer = createConsumer(bsServer, config.connector + "_" + topic);
                consumer.subscribe(Collections.singleton(topic));
                puller = new ConsumerRecordPuller(consumer, maxPollCount, transform, this::handleTransformResult,
                        config.rtId, config.connector);
                rtPuller.put(topic, puller);
                puller.start();
                LogUtils.info(log, "subcribe topic: {} from :{}", topic, bsServer);
            }
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "initConsumer: failed to initialize kafka consumer", e);
            throw new ConnectException(e);
        }

    }


    /**
     * 初始化kafka Consumer
     *
     * @param bootstrapServer kafka的bs地址
     * @param groupName 消費者的群组名称
     * @return kafka Consumer
     */
    private Consumer<byte[], byte[]> createConsumer(String bootstrapServer, String groupName) {
        Map<String, Object> consumerProps = new HashMap<>(BasicProps.getInstance().getConsumerProps());
        consumerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        // 序列化配置,发送数据的value为字节数组
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // 指定consumer group和其他的consumer属性
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * 初始化kafka producer
     *
     * @param bootstrapServer kafka的bs地址
     * @return kafka producer
     */
    private Producer<byte[], byte[]> createProducer(String bootstrapServer) {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
            // 序列化配置,发送数据的value为字节数组
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
            LogUtils.info(log, "initProducer: kafka_producer: {}", producerProps);
            return new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "initProducer: failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 负责从从source的一个kafka topic中拉取数据写入到目标RT kafka topic
     */
    private static class ConsumerRecordPuller implements Runnable {

        private final Consumer<byte[], byte[]> consumer;
        private final Set<String> subscription;
        private final INodeTransform transform;
        private final java.util.function.Consumer<TransformResult> transformResultHandler;
        private final AtomicBoolean isStop = new AtomicBoolean(false);
        private final String rtId;
        private final String connector;
        private final int maxPollCount;
        private int lastPollCount = 0;
        private Thread thread;


        protected ConsumerRecordPuller(Consumer<byte[], byte[]> consumer, int maxPollCount, INodeTransform transform,
                java.util.function.Consumer<TransformResult> transformResultHandler, String rtId, String connector) {
            this.consumer = consumer;
            this.subscription = consumer.subscription();
            this.maxPollCount = maxPollCount;
            this.transform = transform;
            this.transformResultHandler = transformResultHandler;
            this.rtId = rtId;
            this.connector = connector;
            this.thread = new Thread(this);
            this.thread.setUncaughtExceptionHandler(
                    (t, e) -> LogUtils.error("", log, "[{}] Error while consuming records", t.getName(), e));
            this.thread.setName("ConsumerRecordPuller-Thread#" + subscription);
        }

        protected void stop() {
            LogUtils.info(log, "{} stop consumer record puller from {}", rtId, subscription);
            this.isStop.set(true);
        }

        protected void start() {
            this.isStop.set(false);
            this.thread.start();
            LogUtils.info(log, "{} start consumer record puller from {}", rtId, subscription);
        }


        @Override
        public void run() {
            while (!isStop.get()) {
                try {
                    // 按照分钟取秒
                    long tagTime = System.currentTimeMillis() / 60 * 60;
                    // poll等待时间从50ms到500ms
                    long pollTimeout = calcPollTimeForKafka();
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
                    // 遍历结果数据，转换后发送到目的地中
                    if (records != null) {
                        for (ConsumerRecord<byte[], byte[]> record : records) {
                            TransformResult resultRecord = transform.transform(record);
                            transformResultHandler.accept(resultRecord);
                            // 上报打点数据
                            try {
                                int size = record.value().length;
                                Metric.getInstance()
                                        .updateStat(this.rtId, this.connector, record.topic(), "kafka", 1, size,
                                                tagTime + "", this.connector + "|" + tagTime);
                                recordCountTotal.incrementAndGet();
                                recordSizeTotal.addAndGet(size);
                            } catch (Exception ignore) {
                                LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                                        ignore.getMessage());
                            }
                        }
                        lastPollCount = records.count();
                        LogUtils.debug(log, "{} processed {} records from {}", rtId, lastPollCount, subscription);
                    } else {
                        sleepQuietly(500);
                    }
                } catch (WakeupException e) {
                    // 等待下一个周期从kafka中读取数据
                    LogUtils.warn(log, rtId + " found Wakeup Exception during poll records from kafka! ", e);
                } catch (TransformException e) {
                    LogUtils.reportExceptionLog(log, "", "merge datanode transform failure!", e);
                } catch (Throwable e) {
                    LogUtils.reportExceptionLog(log, "", "merge datanode processing failure!", e);
                }
            }
            LogUtils.info(log, "{} ending poll records from {}", rtId, subscription);
            try {
                consumer.close(3, TimeUnit.SECONDS);
            } catch (Exception ignor) {
                LogUtils.warn(log, "failed to close consumer!");
            }
        }

        /**
         * 根据上次拉取的记录条数计算本次拉取kafka的最大等待时间，最低50ms，最高500ms
         *
         * @return 拉取kafka最大等待的时间
         */
        private long calcPollTimeForKafka() {
            return 450 * lastPollCount / maxPollCount + 50L;
        }
    }
}
