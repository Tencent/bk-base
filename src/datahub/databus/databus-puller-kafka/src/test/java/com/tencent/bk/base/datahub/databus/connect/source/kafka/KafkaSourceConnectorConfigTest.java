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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;

public class KafkaSourceConnectorConfigTest {

    // Minimum config
    private static final Map<String, String> defaultMap;

    static {
        Map<String, String> map = new HashMap<>();
        map.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, "localhost:6000");
        map.put(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG, "test-consumer-group");
        defaultMap = Collections.unmodifiableMap(map);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testMinimalConfigDoesNotThrow() {
        Map<String, String> configMap = new HashMap<>(defaultMap);
        new KafkaSourceConnectorConfig(configMap);
    }

    @Test(expected = ConfigException.class)
    public void testThrowsWithoutTopic() {
        Map<String, String> configMap = new HashMap<>(defaultMap);
        new KafkaSourceConnectorConfig(configMap);
    }

    @Test(expected = ConfigException.class)
    public void testThrowsWithoutBootstrapServers() {
        Map<String, String> configMap = new HashMap<>(defaultMap);
        configMap.remove(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        new KafkaSourceConnectorConfig(configMap);
    }

    @Test(expected = ConfigException.class)
    public void testThrowsWithoutConsumerGroup() {
        Map<String, String> configMap = new HashMap<>(defaultMap);
        configMap.remove(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG);
        new KafkaSourceConnectorConfig(configMap);
    }

    @Test()
    public void testSetParametersWithSourcePrefix() {
        String configKey = "max.poll.records";
        Map<String, String> configMap = new HashMap<>(defaultMap);
        String sourceMaxPollRecordsString = KafkaSourceConnectorConfig.SOURCE_PREFIX.concat(configKey);
        String maxPollRecords = "123456";
        configMap.put(sourceMaxPollRecordsString, maxPollRecords);
        Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
        assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
    }

    @Test()
    public void testSetParametersWithConsumerPrefix() {
        String configKey = "max.poll.records";
        Map<String, String> configMap = new HashMap<>(defaultMap);
        String consumerMaxPollRecordsString = KafkaSourceConnectorConfig.CONSUMER_PREFIX.concat(configKey);
        String maxPollRecords = "123456";
        configMap.put(consumerMaxPollRecordsString, maxPollRecords);
        Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
        assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
    }

    @Test()
    public void testOverridesSourcePrefix() {
        String configKey = "max.poll.records";
        Map<String, String> configMap = new HashMap<>(defaultMap);
        String sourceMaxPollRecordsString = KafkaSourceConnectorConfig.SOURCE_PREFIX.concat(configKey);
        String consumerMaxPollRecordsString = KafkaSourceConnectorConfig.CONSUMER_PREFIX.concat(configKey);
        String maxPollRecords = "123456";
        configMap.put(sourceMaxPollRecordsString, "0");
        configMap.put(consumerMaxPollRecordsString, maxPollRecords);
        Properties consumerProps = new KafkaSourceConnectorConfig(configMap).getKafkaConsumerProperties();
        assertEquals(maxPollRecords, String.valueOf(consumerProps.get(configKey)));
    }


}
