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

package com.tencent.bk.base.dataflow.core.storage;

import com.tencent.bk.base.dataflow.core.common.Tools;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStorage implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStorage.class);
    // 存储各个单例对象
    private static Map<String, KafkaStorage> registryMap = new HashMap<>();
    private String kafkaHost;
    private transient Producer<String, String> producer;

    private KafkaStorage(String kafkaHost) {
        this.kafkaHost = kafkaHost;
        initKafkaProducer();
    }

    public static KafkaStorage getInstance(String kafkaHost) {
        if (!registryMap.containsKey(kafkaHost)) {
            synchronized (KafkaStorage.class) {
                if (!registryMap.containsKey(kafkaHost)) {
                    KafkaStorage instance = new KafkaStorage(kafkaHost);
                    registryMap.put(kafkaHost, instance);
                    return instance;
                }
            }
        }
        return registryMap.get(kafkaHost);
    }

    /**
     * 初始化kafka producer
     */
    private void initKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.kafkaHost);
        properties.put("acks", "1");
        properties.put("retries", 5);
        properties.put("max.in.flight.requests.per.connection", 5);
        properties.put("batch.size", 1048576);
        properties.put("linger.ms", 500);
        properties.put("buffer.memory", 52428800);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (null != producer) {
            try {
                producer.close();
            } catch (Exception e) {
                LOGGER.error("failed to close producer", e);
            }
        }
        producer = new KafkaProducer<String, String>(properties);
    }

    /**
     * 保存数据至kafka 包含key
     *
     * @param topic topic
     * @param key key
     * @param data data
     */
    public void saveDataWithKey(String topic, String key, String data) {
        for (int i = 0; i < 3; i++) {
            try {
                producer.send(new ProducerRecord<String, String>(topic, key, data));
                return;
            } catch (Exception e) {
                LOGGER.error("failed to save data into kafka, kafka host is " + kafkaHost, e);
                Tools.pause(i);
                initKafkaProducer();
            }
        }
    }

    public void saveString2Kafka(String topic, String message) {
        long timestamp = System.currentTimeMillis();
        producer.send(new ProducerRecord<String, String>(topic, String.valueOf(timestamp), message));
    }
}
