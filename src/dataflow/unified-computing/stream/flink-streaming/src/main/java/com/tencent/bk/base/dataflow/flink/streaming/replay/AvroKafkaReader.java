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

package com.tencent.bk.base.dataflow.flink.streaming.replay;

import com.tencent.bk.base.dataflow.core.common.Tools;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroKafkaReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroKafkaReader.class);

    private KafkaConsumer<String, byte[]> consumer;

    public AvroKafkaReader(String kafkaHosts) {
        initKafkaConsumer(kafkaHosts);
    }

    /**
     * 初始化kafka consumer
     *
     * @param kafkaHosts ip:port,ip:port
     */
    private void initKafkaConsumer(String kafkaHosts) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHosts);
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "flink-seek");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        if (null != consumer) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOGGER.error("failed to close producer", e);
            }
        }
        consumer = new KafkaConsumer<>(props);
    }

    /**
     * 获取topic的partition数
     *
     * @param topic topic
     * @return partition count
     */
    public int getPartitionCount(String topic) {
        return consumer.partitionsFor(topic).size();
    }

    /**
     * 获取topic的range
     * {
     * "0": {
     * "head": 11676960,
     * "length": 935641,
     * "tail": 12612599
     * }
     * }
     *
     * @param topic topic
     * @return range
     */
    public Map<String, Map<String, Number>> getTopicRange(String topic) {
        Map<String, Map<String, Number>> partitionOffsetRange = new HashMap<>();

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.subscribe(Arrays.asList(topic));
        consumer.poll(1000);
        for (TopicPartition topicPartition : partitions) {
            for (int i = 0; i < 3; i++) {
                try {
                    consumer.seekToBeginning(Collections.singletonList(topicPartition));
                    long headOffset = consumer.position(topicPartition);
                    consumer.seekToEnd(Collections.singletonList(topicPartition));
                    long tailOffset = consumer.position(topicPartition);

                    partitionOffsetRange.put(String.valueOf(topicPartition.partition()), new HashMap<String, Number>() {
                        {
                            put("head", headOffset);
                            put("tail", tailOffset);
                        }
                    });
                    break;
                } catch (Exception e) {
                    LOGGER.error(String.format("failed to init kafka consumer and retry, topic: %s, partition: %d",
                            topic, topicPartition.partition()));
                    if (i == 2) {
                        throw new RuntimeException();
                    }
                    Tools.pause(i);
                }
            }
        }
        consumer.unsubscribe();
        return partitionOffsetRange;
    }

    /**
     * 根据offset获取 message
     *
     * @param topic topic
     * @param partition partition
     * @param offset offset
     * @return message
     */
    public byte[] getMessageAsByte(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<TopicPartition> topics = Arrays.asList(tp);
        // 指定 topic 及 partition
        consumer.assign(topics);
        consumer.seek(tp, offset);
        byte[] message = null;
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            if (records.isEmpty()) {
                message = null;
            } else {
                for (ConsumerRecord<String, byte[]> record : records) {
                    message = record.value();
                    break;
                }
            }
            break;
        }
        consumer.unsubscribe();
        return message;
    }

    /**
     * close kafka consumer
     */
    public void close() {
        try {
            if (null != consumer) {
                consumer.close();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close kafka consumer.", e);
        }
    }
}
