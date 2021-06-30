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


package com.tencent.bk.base.datahub.databus.connect.source.kafka;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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


public class KafkaSourceTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceTask.class);
    // Settings
    private String destTopic;

    // Consumer
    private KafkaConsumer<byte[], byte[]> consumer;
    private Producer<String, byte[]> producer;


    // 记录打印数量统计日志时间
    private long lastCheckTime;
    private long msgCountTotal = 0L;
    private long lastLogCount = 0;
    private long msgSizeTotal = 0L;

    private KafkaSourceConnectorConfig config;

    /**
     * 启动task,初始化资源和属性, 初始化consumer
     */
    @Override
    public void startTask() {
        LogUtils.info(log, "{}: task is starting.", name);
        config = new KafkaSourceConnectorConfig(configProperties);

        destTopic = config.topic;
        producer = initProducer();

        // Setup Kafka consumer
        consumer = new KafkaConsumer<>(config.getKafkaConsumerProperties());
        consumer.subscribe(Collections.singleton(config.srcTopic));
    }

    /**
     * 拉取数据, 转发到目标kafka
     *
     * @return null
     */
    @Override
    public List<SourceRecord> pollData() {

        if (msgCountTotal - lastLogCount >= 5000 || System.currentTimeMillis() - lastCheckTime > 900000) {
            LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", msgCountTotal, msgSizeTotal, rtId);
            lastLogCount = msgCountTotal;
            lastCheckTime = System.currentTimeMillis();
        }
        if (!isStop.get()) {
            long tagTime = System.currentTimeMillis() / 10000 * 10;
            String outputTag = getMetricTag(dataId, tagTime);
            try {
                ConsumerRecords<byte[], byte[]> krecords = consumer.poll(config.pollTimeout);
                LogUtils.debug(log, "{}: Got {} records from source.", name, krecords.count());
                int msgSize = 0;
                for (ConsumerRecord<byte[], byte[]> krecord : krecords) {
                    String sourceTopic = krecord.topic();
                    byte[] recordValue = krecord.value();
                    msgSize += recordValue.length;
                    LogUtils.trace(log,
                            "Task: sourceTopic:{} sourcePartition:{} sourceOffSet:{} destinationTopic:{}, valueSize:{}",
                            sourceTopic, krecord.partition(), krecord.offset(), destTopic,
                            krecord.serializedValueSize());

                    // key: ds=155324530&tag=xxx&po=xxx
                    String msgKey = String.format("ds=%d&tag=%s", tagTime, outputTag);
                    producer.send(new ProducerRecord<>(destTopic, msgKey, recordValue),
                            new ProducerCallback(destTopic, 0, msgKey.getBytes(StandardCharsets.UTF_8), recordValue));
                }
                msgCountTotal += krecords.count();
                msgSizeTotal += msgSize;
                Metric.getInstance()
                        .updateStat(dataId, destTopic, destTopic, "kafka", krecords.count(), msgSize, "", outputTag);
            } catch (WakeupException e) {
                LogUtils.info(log, "{}: Caught WakeupException. Probably shutting down.", name);
            }
        }

        return null;
    }

    /**
     * 停止任务
     */
    @Override
    public void stopTask() {
        LogUtils.info(log, "{}: stop() called. Waking up consumer and shutting down", name);
        if (null != consumer) {
            consumer.wakeup();
            LogUtils.info(log, "{}: Shutting down consumer.", name);
            consumer.close(Math.max(0, config.maxShutdownWait), TimeUnit.MILLISECONDS);
        }

        if (null != producer) {
            producer.flush();
            producer.close(5, TimeUnit.SECONDS);
        }
        LogUtils.info(log, "{}: task has been stopped", name);
    }

    /**
     * 初始化kafka producer,用于发送结果数据
     */
    private KafkaProducer<String, byte[]> initProducer() {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, config.destKafkaBs);

            // 序列化配置,发送数据的value为字节数组
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
            LogUtils.debug(log, "_initProducer_: kafka_producer: {}", producerProps);

            return new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "_initProducer_: failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param dataId dataid
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    private String getMetricTag(String dataId, long tagTime) {
        // tag=dataId|timestamp|ip
        return String.format("%s|%d|%s", dataId, tagTime, Metric.getInstance().getWorkerIp());
    }

}