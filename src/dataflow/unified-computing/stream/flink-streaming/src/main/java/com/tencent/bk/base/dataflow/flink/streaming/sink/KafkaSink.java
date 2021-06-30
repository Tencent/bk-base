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

package com.tencent.bk.base.dataflow.flink.streaming.sink;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_ACKS_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_BATCH_SIZE_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_BUFFER_MEMORY_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_LINGER_MS_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_MAX_REQUEST_SIZE_DEFAULT_VALUE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.SINK_KAFKA_RETRIES_DEFAULT_VALUE;

import com.tencent.bk.base.dataflow.core.configuration.UCConfiguration;
import com.tencent.bk.base.dataflow.core.sink.AbstractSink;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class KafkaSink extends AbstractSink {

    private SinkNode node;
    private Map<String, DataStream<Row>> dataStreams;
    private AbstractFlinkStreamingCheckpointManager checkpointManager;
    private Properties properties;

    private FlinkStreamingTopology topology;

    public KafkaSink(SinkNode node, Map<String, DataStream<Row>> dataStreams, FlinkStreamingRuntime runtime) {
        this.node = node;
        this.dataStreams = dataStreams;
        this.checkpointManager = runtime.getCheckpointManager();
        this.properties = new Properties();
        this.topology = runtime.getTopology();
        initProducerProperties(properties, node);
    }

    @Override
    public void createNode() {

        BucketingFlinkKafkaProducer010<Row> customProducer = new BucketingFlinkKafkaProducer010<Row>(
                NodeUtils.generateKafkaTopic(node.getNodeId()),
                new DummyKeyedSerializationSchema(),
                new SQLAbstractAvroKeyedSerializationSchema(topology, node),
                properties,
                node,
                topology,
                checkpointManager);

        dataStreams.get(node.getNodeId()).addSink(customProducer).name(node.getNodeId());
    }

    private void initProducerProperties(Properties properties, SinkNode node) {
        // 默认参数及配置
        UCConfiguration outputConf = node.getOutput().getConf();
        // acks: "0", "1" or "all"
        outputConf.setProperty("acks", outputConf.getString("acks", SINK_KAFKA_ACKS_DEFAULT_VALUE));
        outputConf.setProperty("retries", outputConf.getInt("retries", SINK_KAFKA_RETRIES_DEFAULT_VALUE));
        outputConf.setProperty(
                "max.in.flight.requests.per.connection",
                outputConf.getInt("max.in.flight.requests.per.connection",
                        SINK_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT_VALUE));
        outputConf.setProperty("batch.size", outputConf.getLong("batch.size", SINK_KAFKA_BATCH_SIZE_DEFAULT_VALUE));
        outputConf.setProperty("linger.ms", outputConf.getLong("linger.ms", SINK_KAFKA_LINGER_MS_DEFAULT_VALUE));
        outputConf.setProperty("buffer.memory",
                outputConf.getLong("buffer.memory", SINK_KAFKA_BUFFER_MEMORY_DEFAULT_VALUE));
        outputConf.setProperty("max.request.size",
                outputConf.getLong("max.request.size", SINK_KAFKA_MAX_REQUEST_SIZE_DEFAULT_VALUE));
        Iterator<String> keys = outputConf.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            properties.put(key, outputConf.getString(key));
        }
        properties.put("bootstrap.servers", node.getOutput().getOutputInfo().toString());
    }
}
