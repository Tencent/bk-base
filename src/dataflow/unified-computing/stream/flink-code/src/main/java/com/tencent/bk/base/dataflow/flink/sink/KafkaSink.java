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

package com.tencent.bk.base.dataflow.flink.sink;

import com.tencent.bk.base.dataflow.core.sink.AbstractSink;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.flink.runtime.FlinkCodeRuntime;
import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology;
import com.tencent.bk.base.dataflow.kafka.producer.BucketingFlinkKafkaProducer010;
import com.tencent.bk.base.dataflow.kafka.producer.util.DummyKeyedSerializationSchema;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.types.Row;

public class KafkaSink extends AbstractSink {

    private SinkNode node;
    private Map<String, DataStream<Row>> dataStreams;
    private Properties properties;

    private FlinkCodeTopology topology;

    public KafkaSink(SinkNode node, Map<String, DataStream<Row>> dataStreams, FlinkCodeRuntime runtime) {
        this.node = node;
        this.dataStreams = dataStreams;
        this.properties = new Properties();
        this.topology = runtime.getTopology();
        initProducerProperties(properties, node);
    }

    @Override
    public void createNode() {
        FlinkKafkaProducer09 producer;
        producer = new BucketingFlinkKafkaProducer010<Row>(
                NodeUtils.generateKafkaTopic(node.getNodeId()),
                new DummyKeyedSerializationSchema(),
                new CodeAvroKeyedSerializationSchema(topology, node),
                properties,
                node,
                topology);
        // 调试打印输出
//    dataStreams.get(node.getNodeId()).print();
        dataStreams.get(node.getNodeId()).addSink(producer);
    }

    private void initProducerProperties(Properties properties, SinkNode node) {
        properties.put("bootstrap.servers", node.getOutput().getOutputInfo().toString());
        properties.put("acks", "all");
        properties.put("retries", 5);
        properties.put("max.in.flight.requests.per.connection", 5);
        properties.put("batch.size", 1048576);
        properties.put("linger.ms", 1000);
        properties.put("buffer.memory", 52428800);
        properties.put("max.request.size", 3145728);
    }
}
