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

import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.types.MessageHead;
import com.tencent.bk.base.dataflow.flink.checkpoint.FlinkCodeCheckpointManager;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.util.ToRowDataStreamMap;
import com.tencent.bk.base.dataflow.kafka.consumer.IConsumer;
import com.tencent.bk.base.dataflow.kafka.consumer.parser.AvroParser;
import com.tencent.bk.base.dataflow.metric.SourceMetricMapperWrapper;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSource extends AbstractSource {

    private StreamExecutionEnvironment env;
    private SourceNode node;
    private Properties properties;
    private TypeInformation<?>[] fieldsTypes;
    private SchemaFactory schemaFactory;
    // flink source struct
    private DataStream<Row> sourceStream;
    private RowTypeInfo typeInfo;
    private StreamTopology topology;

    private IConsumer consumer;

    /**
     * source的构造方法
     *
     * @param env flink的env
     * @param node rt
     * @throws IOException exception
     */
    public KafkaSource(
            StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, SourceNode node,
            FlinkCodeCheckpointManager checkpointManager, StreamTopology topology) throws IOException {
        this.env = env;
        this.node = node;
        this.properties = new Properties();
        String inputInfo = node.getInput().getInputInfo().toString();
        initProperties(properties, inputInfo, topology);
        schemaFactory = new SchemaFactory();
        // 构造schema
        fieldsTypes = schemaFactory.getFieldsTypes(node);
        // datastream schema 拥有类型和字段名称信息
        typeInfo = new RowTypeInfo(fieldsTypes, node.getFieldNames());
        this.topology = topology;

        this.consumer = new FlinkKafkaConsumer(node, checkpointManager, topology);
    }

    /**
     * 创建节点
     * 包含注册表的逻辑在内 即有多少个子表就会注册对应的sql表
     */
    @Override
    public DataStream<Row> createNode() {

        DataStream<Tuple2<MessageHead, Row>> tempMessageStream = env.addSource(
                (SourceFunction) this.consumer.getConsumer()).flatMap(new AvroParser(node, topology));
        // metric 打点
        tempMessageStream.map(new SourceMetricMapperWrapper(this.node, this.topology));
        sourceStream = tempMessageStream.map(new ToRowDataStreamMap()).returns(typeInfo);

        return sourceStream;
    }

    /**
     * source 读取rt对应的kafka config
     * 消费组命名规范 calculate-flink-jobid
     * 通过消费组命名来获取积压量的信息
     *
     * @param properties kafka配置
     * @param inputInfo input kafka info
     * @param topology topo
     */
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
