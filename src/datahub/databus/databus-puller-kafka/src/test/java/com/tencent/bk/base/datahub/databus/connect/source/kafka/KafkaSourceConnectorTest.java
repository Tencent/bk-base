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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.easymock.PowerMock;
import org.easymock.EasyMockSupport;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaSourceConnector.class})
@PowerMockIgnore("javax.management.*")
public class KafkaSourceConnectorTest extends EasyMockSupport {

    private static final String SOURCE_TOPICS_VALUE = "test.topic";
    private static final String SOURCE_BOOTSTRAP_SERVERS_CONFIG = "localhost:6000";
    private static final String POLL_LOOP_TIMEOUT_MS_VALUE = "2000";
    private static final String TOPIC_LIST_TIMEOUT_MS_VALUE = "5000";
    private static final String CONSUMER_GROUP_ID_VALUE = "test-consumer-group";

    private ConnectorContext connectorContextMock;
    private Set<LeaderTopicPartition> stubLeaderTopicPartitions;
    private KafkaSourceConnector connector;
    private Map<String, String> sourceProperties;

    @Before
    public void setUp() throws Exception {
        connector = new KafkaSourceConnector();
        connectorContextMock = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(connectorContextMock);

        // Default test settings
        sourceProperties = new HashMap<>();
        sourceProperties
                .put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        sourceProperties.put(KafkaSourceConnectorConfig.POLL_LOOP_TIMEOUT_MS_CONFIG, POLL_LOOP_TIMEOUT_MS_VALUE);
        sourceProperties.put(KafkaSourceConnectorConfig.TOPIC_LIST_TIMEOUT_MS_CONFIG, TOPIC_LIST_TIMEOUT_MS_VALUE);
        sourceProperties.put(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG, CONSUMER_GROUP_ID_VALUE);

        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition);
    }

    @After
    public void tearDown() {
    }

    @Test(expected = ConfigException.class)
    public void testStartMissingBootstrapServers() {
        PowerMock.replayAll();

        sourceProperties.remove(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testStartBlankBootstrapServers() {
        PowerMock.replayAll();

        sourceProperties.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, "");
        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testStartTopicWhitelistMissing() {
        replayAll();

        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test
    public void testStartCorrectConfig() throws Exception {

        PowerMock.expectLastCall().andVoid();
        PowerMock.replayAll();

        connector.start(sourceProperties);

        verifyAll();
    }

    @Test
    public void testTaskOnOneTopicPartition() throws Exception {
        PowerMock.expectLastCall().andVoid();
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        assertEquals(1, taskConfigs.size());
        assertEquals("0:test.topic:0", taskConfigs.get(0).get("task.leader.topic.partitions"));
        assertEquals(SOURCE_BOOTSTRAP_SERVERS_CONFIG,
                taskConfigs.get(0).get(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG));

        verifyAll();
    }

    @Test
    public void testTaskOnTwoTopicPartitions() throws Exception {
        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition1 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition1);
        LeaderTopicPartition leaderTopicPartition2 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 1);
        stubLeaderTopicPartitions.add(leaderTopicPartition2);

        PowerMock.expectLastCall().andVoid();
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertEquals(1, taskConfigs.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testTasksOnTwoTopicPartitions() throws Exception {
        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition1 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition1);
        LeaderTopicPartition leaderTopicPartition2 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 1);
        stubLeaderTopicPartitions.add(leaderTopicPartition2);


        PowerMock.expectLastCall().andVoid();
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        assertEquals(2, taskConfigs.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        replayAll();

        assertEquals(KafkaSourceTask.class, connector.taskClass());

        verifyAll();
    }

}
