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

package com.tencent.bk.base.dataflow.flink.streaming.source;

import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.types.AvroMessage;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.replay.AvroKafkaReader;
import com.tencent.bk.base.dataflow.flink.streaming.replay.KafkaOffsetSeeker;
import com.tencent.bk.base.dataflow.flink.streaming.source.kafka.BkFlinkKafkaConsumer010;
import com.tencent.bk.base.dataflow.flink.streaming.table.RegisterTable;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.kafka.consumer.parser.AvroParser;
import com.tencent.bk.base.dataflow.metric.SourceMetricMapperWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;
    private SourceNode node;
    private String topic;
    private Properties properties;
    private TypeInformation<?>[] fieldsTypes;
    // flink source struct
    private FlinkKafkaConsumer010<AvroMessage> consumer;
    private DataStream<Row> sourceStream;
    private RowTypeInfo typeInfo;
    private Boolean fromTail = false;
    private AvroKafkaReader seekConsumer;
    private KafkaOffsetSeeker kafkaOffsetSeeker;
    private FlinkStreamingTopology topology;

    /**
     * source的构造方法
     *
     * @param node
     * @param runtime
     * @throws IOException
     */
    public KafkaSource(SourceNode node, FlinkStreamingRuntime runtime) throws IOException {
        this.env = runtime.getEnv();
        this.tableEnv = runtime.getTableEnv();
        this.node = node;
        this.topic = String.format("table_%s", node.getNodeId());
        this.properties = new Properties();
        if (runtime.getCheckpointManager().getStartPosition().equals("from_tail")) {
            this.fromTail = true;
        }
        String inputInfo = node.getInput().getInputInfo().toString();
        initProperties(properties, inputInfo, runtime.getTopology());
        SchemaFactory schemaFactory = new SchemaFactory();
        // 构造schema
        fieldsTypes = schemaFactory.getFieldsTypes(node);
        typeInfo = new RowTypeInfo(fieldsTypes);
        seekConsumer = new AvroKafkaReader(inputInfo);
        kafkaOffsetSeeker = new KafkaOffsetSeeker(runtime.getTopology(), node, runtime.getCheckpointManager());
        this.topology = runtime.getTopology();
    }

    /**
     * 创建节点 包含注册表的逻辑在内 即有多少个子表就会注册对应的sql表
     */
    @Override
    public DataStream<Row> createNode() {
        consumer = new BkFlinkKafkaConsumer010<AvroMessage>(
                topic,
                new ByteOffsetSourceKafka(),
                properties);
        int partitionCount = seekConsumer.getPartitionCount(topic);
        // 判断是否从最新位置开始消费数据
        if (fromTail) {
            consumer.setStartFromLatest();
            LOGGER.info("read latest data from kafka!!!!");
            // replay机制
        } else {
            LOGGER.info("Begin to seek offsets.");
            Map<Integer, Long> initalOffsets = kafkaOffsetSeeker
                    .seekInitialOffsets(topic, partitionCount, seekConsumer);
            LOGGER.info("the target offset is " + initalOffsets.toString());
            boolean isHead = true;
            // 获取是否从head开始消费数据
            for (Long value : initalOffsets.values()) {
                if (value != 0) {
                    isHead = false;
                    break;
                }
            }
            if (isHead) {
                consumer.setStartFromEarliest();
                LOGGER.info("because init offset all is zero, read earliest data from kafka!!!");
            } else {
                Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
                for (Map.Entry<Integer, Long> entry : initalOffsets.entrySet()) {
                    specificStartOffsets.put(new KafkaTopicPartition(topic, entry.getKey()), entry.getValue());
                }
                LOGGER.info("Let's read data from " + specificStartOffsets.toString());
                System.out.println("Let's read data from " + specificStartOffsets.toString());
                consumer.setStartFromSpecificOffsets(specificStartOffsets);
            }
        }
        close();
        DataStream<Tuple2<MessageHead, Row>> tempMessageSstream = env.addSource(consumer).name(topic)
                .setParallelism(partitionCount)
                .flatMap(new AvroParser(node, topology)).name("AvroParser");

        sourceStream = tempMessageSstream.map(new ToRowDataStreamMap()).name("RowDataMap").returns(typeInfo);

        // metric
        tempMessageSstream.map(new SourceMetricMapperWrapper(node, topology)).name("Metric");
        // 注册表
        RegisterTable.registerFlinkTable(node, sourceStream, tableEnv);

        return sourceStream;
    }

    /**
     * source 读取rt对应的kafka config 消费组命名规范 calculate-flink-jobid 通过消费组命名来获取积压量的信息
     *
     * @param properties kafka配置
     * @param inputInfo input kafka info
     * @param topology topo
     */
    private void initProperties(Properties properties, String inputInfo, Topology topology) {
        String autoOffsetReset = this.fromTail ? "latest" : "earliest";
        properties.setProperty("auto.offset.reset", autoOffsetReset);
        properties.setProperty("bootstrap.servers", inputInfo);
        String groupId;
        if ("product".equals(topology.getRunMode())) {
            groupId = String.format("%s-%s-%s", "calculate", "flink", topology.getJobId());
        } else {
            groupId = String.format("%s-%s", "calculate", "flink-debug");
        }

        properties.setProperty("group.id", groupId);
    }

    private void close() {
        if (seekConsumer != null) {
            seekConsumer.close();
        }
        if (kafkaOffsetSeeker != null) {
            kafkaOffsetSeeker.close();
        }
    }
}
