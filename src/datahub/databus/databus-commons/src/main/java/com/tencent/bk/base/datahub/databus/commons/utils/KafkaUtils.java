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
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaUtils {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);
    private static final long DefaultTimeout = 10000L; // 10s

    /**
     * 构建kafka消费者
     *
     * @param groupId 消费组ID
     * @param kafkaAddr kafka地址
     * @return kafka consumer
     */
    public static KafkaConsumer<String, String> initStringConsumer(String groupId, String kafkaAddr) {
        Properties props = new Properties();
        props.putAll(BasicProps.getInstance().getConsumerProps());

        props.put(Consts.GROUP_ID, groupId);
        props.put(Consts.BOOTSTRAP_SERVERS, kafkaAddr);
        props.put(Consts.KEY_DESER, StringDeserializer.class.getName());
        props.put(Consts.VALUE_DESER, StringDeserializer.class.getName());

        return new KafkaConsumer<>(props);
    }

    /**
     * 构建kafka生产者
     *
     * @return kafka consumer
     */
    public static KafkaProducer<String, String> initProducer() {
        Properties props = new Properties();
        Map<String, Object> result = DatabusProps.getInstance().originalsWithPrefix(Consts.PRODUCER_PREFIX);
        if (StringUtils.isBlank((String) result.get(Consts.KEY_DESER))) {
            result.put(Consts.KEY_DESER, StringDeserializer.class.getName());
        }
        if (StringUtils.isBlank((String) result.get(Consts.VALUE_DESER))) {
            result.put(Consts.VALUE_DESER, StringDeserializer.class.getName());
        }
        props.putAll(result);
        try {
            return new KafkaProducer<String, String>(props);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to construct the producer for metric data. {}", props);
            throw e;
        }
    }

    /**
     * 获取topic每个partition最后count条数据
     * 先会调用一次poll(0), topic不存在则会创建, 如果支持自动创建的话
     *
     * @param configs kafka配置
     * @param topic topic
     * @param groupId group id
     * @param count 数据条数
     * @param timeout 消费超时时间
     * @return consumer data
     */
    public static ConsumerRecords<String, String> getLastMessages(Map<String, String> configs, String topic,
            String groupId, int count, long timeout) {
        Properties props = new Properties();
        props.putAll(configs);
        if (groupId == null) {
            // generate random group id, bkdata-consumer-xxx-xxx
            groupId = String.format("bkdata-consumer-%s", UUID.randomUUID());
        }
        props.put(Consts.GROUP_ID, groupId);
        LogUtils.info(log, "consumer props={}", props.toString());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));
        consumer.poll(0);
        consumer.commitSync();

        ConsumerRecords<String, String> records = ConsumerRecords.empty();

        // 获取offset, 然后计算偏移量
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions.size() == 0) {
            return records;
        }

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo p : partitions) {
            topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
        }

        boolean empty = true;
        Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitions);
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            long offset = entry.getValue();
            if (offset <= 0) {
                continue;
            }
            empty = false;
            long newOffset = offset > count ? offset - count : 0;
            consumer.seek(entry.getKey(), newOffset);
        }

        if (!empty) {
            // 消费数据
            records = consumer.poll(timeout);
            consumer.commitSync();
        }
        consumer.close();
        return records;
    }

    /**
     * 查询每个分区的最后1条数据
     *
     * @param configs kafka配置
     * @param topic topic
     * @param group group
     * @return 每个分区的最后1条数据
     */
    public static ConsumerRecords<String, String> getLastMessage(Map<String, String> configs, String topic,
            String group) {
        return getLastMessages(configs, topic, group, 1, DefaultTimeout);
    }

    /**
     * 查询每个分区的最后1条数据
     *
     * @param kafkaAddr kafka地址, 例如127.0.0.1:9092
     * @param topic topic
     * @param group group
     * @return 每个分区的最后1条数据
     */
    public static ConsumerRecords<String, String> getLastMessage(String kafkaAddr, String topic, String group) {
        Map<String, String> configs = new HashMap<>();
        configs.put(Consts.BOOTSTRAP_SERVERS, kafkaAddr);
        configs.put(Consts.KEY_DESER, StringDeserializer.class.getName());
        configs.put(Consts.VALUE_DESER, StringDeserializer.class.getName());
        return getLastMessages(configs, topic, group, 1, DefaultTimeout);
    }

    /**
     * 从指定kafka,topic中获取partition
     *
     * @param bootStrapServer kafka地址
     * @param topic topic
     * @return 返回partition信息
     */
    public static List<PartitionInfo> getPartitions(String bootStrapServer, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServer);
        props.put("group.id", "lastMsgFetcher");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        try {
            consumer.close();
        } catch (Exception e) {
            LogUtils.warn(log, "failed to close kafka consumer {}", e.getMessage());
        }

        return partitions;
    }

    /**
     * 从指定kafka中获取partition
     *
     * @param props Kafka配置
     * @param topic topic
     * @return 返回partition信息
     */
    public static List<PartitionInfo> getPartitions(Properties props, String topic) {
        props.put("group.id", "lastMsgFetcher");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        try {
            consumer.close(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to close kafka consumer {}", e.getMessage());
        }

        return partitions;
    }


}
