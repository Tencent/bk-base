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

package com.tencent.bk.base.dataflow.flink.source;

import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.types.AvroMessage;
import com.tencent.bk.base.dataflow.flink.checkpoint.FlinkCodeCheckpointManager;
import com.tencent.bk.base.dataflow.kafka.consumer.IConsumer;
import com.tencent.bk.base.dataflow.kafka.consumer.utils.ByteOffsetSourceKafka;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkKafkaConsumer implements IConsumer<FlinkKafkaConsumer010<AvroMessage>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaConsumer.class);
    private String topic;
    private Properties properties;
    // flink source struct
    private FlinkKafkaConsumer010<AvroMessage> consumer;
    private String startPosition;

    /**
     * source的构造方法
     *
     * @param node rt
     * @throws IOException exception
     */
    public FlinkKafkaConsumer(SourceNode node, FlinkCodeCheckpointManager checkpointManager, Topology topology) {
        this.topic = String.format("table_%s", node.getNodeId());
        this.properties = new Properties();
        this.startPosition = checkpointManager.getStartPosition();
        String inputInfo = node.getInput().getInputInfo().toString();
        initProperties(properties, inputInfo, topology);
    }

    /**
     * 获取 kafka consumer
     * TODO: 根据 checkpointManager 配置处理对应的逻辑
     *
     * @return kafka consumer
     */
    @Override
    public FlinkKafkaConsumer010 getConsumer() {
        consumer = new FlinkKafkaConsumer010<AvroMessage>(
                topic,
                new ByteOffsetSourceKafka(),
                properties);

        // 判断是否从最新位置开始消费数据
        if ("from_tail".equalsIgnoreCase(this.startPosition)) {
            consumer.setStartFromLatest();
            LOGGER.info("read latest data from kafka!!!!");
        } else if ("continue".equalsIgnoreCase(this.startPosition)) {
            // 继续消费
            consumer.setStartFromGroupOffsets();
            LOGGER.info("continue to read data.");
        } else {
            consumer.setStartFromEarliest();
            LOGGER.info("read earliest data from kafka!!!");
        }
        return consumer;
    }

    private void initProperties(Properties properties, String inputInfo, Topology topology) {
        properties.setProperty("bootstrap.servers", inputInfo);
        String groupId;
        if ("product".equals(topology.getRunMode())) {
            groupId = String.format("%s-%s-%s", "calculate", "flink", topology.getJobId());
        } else {
            groupId = String.format("%s-%s", "calculate", "flink-debug");
        }

        properties.setProperty("group.id", groupId);
    }

}
