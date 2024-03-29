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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HdfsSinkConnectorTestBase {

    protected Configuration conf;
    protected String url;
    protected Map<String, String> connectorProps;
    protected BizHdfsSinkConfig connectorConfig;
    protected String topicsDir;
    protected String logsDir;
//    protected AvroData avroData;

    protected MockSinkTaskContext context;
    protected static final String TOPIC = "topic";
    protected static final int PARTITION = 12;
    protected static final int PARTITION2 = 13;
    protected static final int PARTITION3 = 14;
    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
    protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);
    protected static Set<TopicPartition> assignment;


    protected Map<String, String> createProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BizHdfsSinkConfig.HDFS_URL_CONFIG, url);
        props.put(BizHdfsSinkConfig.FLUSH_SIZE_CONFIG, "3");
        props.put(BizHdfsSinkConfig.BIZ_ID, "0");
        return props;
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct().name("record").version(1)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .build();
    }

    protected Struct createRecord(Schema schema) {
        return new Struct(schema)
                .put("boolean", true)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2);
    }

    protected Schema createNewSchema() {
        return SchemaBuilder.struct().name("record").version(2)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", SchemaBuilder.string().defaultValue("abc").build())
                .build();
    }

    protected Struct createNewRecord(Schema newSchema) {
        return new Struct(newSchema)
                .put("boolean", true)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2)
                .put("string", "def");
    }

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        url = "memory://";
        connectorProps = createProps();
        // Configure immediately in setup for common case of just using this default. Subclasses can
        // re-call this safely.
        configureConnector();
        assignment = new HashSet<>();
        assignment.add(TOPIC_PARTITION);
        assignment.add(TOPIC_PARTITION2);
        context = new MockSinkTaskContext();
    }

    @After
    public void tearDown() throws Exception {
        if (assignment != null) {
            assignment.clear();
        }
    }

    protected void configureConnector() {
        connectorConfig = new BizHdfsSinkConfig(connectorProps);
        topicsDir = connectorConfig.getString(BizHdfsSinkConfig.TOPICS_DIR_CONFIG);
        logsDir = connectorConfig.getString(BizHdfsSinkConfig.LOGS_DIR_CONFIG);
//        int schemaCacheSize = connectorConfig.getInt(BizHdfsSinkConfig.SCHEMA_CACHE_SIZE_CONFIG);
//        avroData = new AvroData(schemaCacheSize);
    }

    protected static class MockSinkTaskContext implements SinkTaskContext {

        private Map<TopicPartition, Long> offsets;
        private long timeoutMs;

        public MockSinkTaskContext() {
            this.offsets = new HashMap<>();
            this.timeoutMs = -1L;
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        /**
         * Get offsets that the SinkTask has submitted to be reset. Used by the Copycat framework.
         *
         * @return the map of offsets
         */
        public Map<TopicPartition, Long> offsets() {
            return offsets;
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         *
         * @return the backoff timeout in milliseconds.
         */
        public long timeout() {
            return timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         *
         * @return the backoff timeout in milliseconds.
         */

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
            return;
        }

        @Override
        public void resume(TopicPartition... partitions) {
            return;
        }

        @Override
        public void requestCommit() {

        }
    }
}

