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

package com.tencent.bk.base.dataflow.flink.pipeline;

import com.tencent.bk.base.dataflow.common.api.AbstractFlinkBasicTransform;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.debug.DebugResultDataStorage;
import com.tencent.bk.base.dataflow.flink.exception.ResultTableFieldMatchException;
import com.tencent.bk.base.dataflow.flink.runtime.FlinkCodeRuntime;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.source.KafkaSource;
import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology;
import com.tencent.bk.base.dataflow.metric.MetricMapper;
import com.tencent.bk.base.dataflow.flink.sink.KafkaSink;
import com.tencent.bk.base.dataflow.flink.sink.SortFields;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.util.StringUtils;

public class FlinkCodePipeline extends AbstractFlinkPipeline<FlinkCodeTopology, FlinkCodeRuntime> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkCodePipeline.class);
    protected Map<String, DataStream<Row>> dataStreams = new HashMap<>();

    public FlinkCodePipeline(FlinkCodeTopology topology) {
        super(topology);
    }

    @Override
    public void source() {
        // source
        for (Map.Entry<String, SourceNode> entry : this.getTopology().getSourceNodes().entrySet()) {
            SourceNode sourceNode = entry.getValue();
            try {
                this.dataStreams.put(sourceNode.getNodeId(),
                        new KafkaSource(
                                this.getRuntime().getEnv(), this.getRuntime().getTableEnv(), sourceNode,
                                this.getRuntime().getCheckpointManager(),
                                this.getTopology()).createNode());
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Failed to create source node with " + sourceNode.getNodeId());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void transform() {
        FlinkCodeTopology topology = this.getTopology();
        String transformClassStr = topology.getUserMainClass();
        List<String> userArgs = topology.getUserArgs();
        AbstractFlinkBasicTransform dataTransformer;
        try {
            // 构造用户代码实例
            Class transformClass = Class.forName(transformClassStr);
            Constructor constructor = transformClass.getConstructor();
            Object transform = transformClass.newInstance();
            if (transform instanceof AbstractFlinkBasicTransform) {
                dataTransformer = (AbstractFlinkBasicTransform) constructor.newInstance();
            } else {
                throw new RuntimeException("Type mismatch Error for constructing "
                        + "AbstractFlinkBasicTransform instance.");
            }
            // 设置 Flink env
            Method setEnvMethod = AbstractFlinkBasicTransform.class
                    .getDeclaredMethod("setEnv", StreamExecutionEnvironment.class);
            setEnvMethod.setAccessible(true);
            setEnvMethod.invoke(dataTransformer, this.getRuntime().getEnv());
            // 设置 Flink tableEnv
            Method setTableEnvMethod = AbstractFlinkBasicTransform.class
                    .getDeclaredMethod("setTableEnv", StreamTableEnvironment.class);
            setTableEnvMethod.setAccessible(true);
            setTableEnvMethod.invoke(dataTransformer, this.getRuntime().getTableEnv());
            // 设置用户参数
            Method setArgsMethod = AbstractFlinkBasicTransform.class.getSuperclass()
                    .getDeclaredMethod("setArgs", List.class);
            setArgsMethod.setAccessible(true);
            setArgsMethod.invoke(dataTransformer, userArgs);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(String.format("The class %s is not found. %s", transformClassStr, e));
        }
        Map<String, DataStream<Row>> transformedDataStreams = dataTransformer.transform(dataStreams);
        this.validate(transformedDataStreams);
        this.addExtraRecordField(transformedDataStreams);
        Map<String, TransformNode> transformNodes = constructTransformNodes();
        topology.setTransformNodes(transformNodes);

        this.dataStreams.putAll(transformedDataStreams);
    }

    @Override
    public void sink() {
        // metric 打点
        this.getTopology().getSinkNodes().forEach((resultTableId, sinkNode) ->
                dataStreams.get(resultTableId).map(new MetricMapper(sinkNode, this.getTopology())));

        // sink
        if (this.getTopology().isDebug()) {
            this.getTopology().getSinkNodes().forEach((resultTableId, sinkNode) ->
                    dataStreams.get(resultTableId).map(new DebugResultDataStorage(sinkNode, this.getTopology())));
        } else {
            this.getTopology().getSinkNodes().forEach((resultTableId, sinkNode) -> {
                try {
                    new KafkaSink(sinkNode, dataStreams, this.getRuntime()).createNode();
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Sink node {} failed.", sinkNode.getNodeId());
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void submit() throws Exception {
        // source
        this.source();

        // transform
        this.transform();

        // sink
        this.sink();

        this.getRuntime().execute();
    }

    /**
     * 在节点字段列表最后增加 dtEventTimeStamp 字段
     *
     * @param dataStream
     * @param eventTimeFieldIndex -1 表示增加本地时间实际为数据时间
     */
    private DataStream<Row> appendEventTimeField(DataStream<Row> dataStream, int eventTimeFieldIndex) {
        TableSchema tableSchema = this.getRuntime().getTableEnv().fromDataStream(dataStream).getSchema();

        // append long type
        TypeInformation<?>[] fieldTypes = tableSchema.getFieldTypes();
        TypeInformation<?>[] appendFieldTypes = Arrays.copyOf(fieldTypes, fieldTypes.length + 1);
        appendFieldTypes[appendFieldTypes.length - 1] = BasicTypeInfo.LONG_TYPE_INFO;
        // add field_name of long type
        String[] fieldNames = tableSchema.getFieldNames();
        String[] appendFieldNames = Arrays.copyOf(fieldNames, fieldNames.length + 1);
        appendFieldNames[appendFieldNames.length - 1] = ConstantVar.EVENT_TIMESTAMP;

        return dataStream.map(new MapFunction<Row, Row>() {
            /**
             * The mapping method. Takes an element from the input data set and transforms
             * it into exactly one element.
             *
             * @param oldRow The input value.
             * @return The transformed value
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
             *                   to fail and may trigger recovery.
             */
            @Override
            public Row map(Row oldRow) throws Exception {
                Row outRow = new Row(oldRow.getArity() + 1);
                for (int i = 0; i < oldRow.getArity(); i++) {
                    outRow.setField(i, oldRow.getField(i));
                }
                if (eventTimeFieldIndex < 0) {
                    outRow.setField(outRow.getArity() - 1, System.currentTimeMillis());
                } else {
                    outRow.setField(outRow.getArity() - 1, oldRow.getField(eventTimeFieldIndex));
                }
                return outRow;
            }
        }).returns(new RowTypeInfo(appendFieldTypes, appendFieldNames));
    }

    /**
     * 增加额外的字段信息，当前需要增加 dtEventTimeStamp
     *
     * @param dataStreams
     */
    private void addExtraRecordField(Map<String, DataStream<Row>> dataStreams) {
        Map<String, SinkNode> sinkNodes = this.getTopology().getSinkNodes();

        for (Map.Entry<String, SinkNode> entry : sinkNodes.entrySet()) {
            SinkNode sinkNode = entry.getValue();
            int eventTimeFieldIndex = sinkNode.getEventTimeFieldIndex();
            if (eventTimeFieldIndex != -1) {
                LOGGER.info(String.format("增加用户自定义时间字段: %s", sinkNode.getFields().get(eventTimeFieldIndex)));
            } else {
                LOGGER.info("增加系统当前时间作为时间字段");
            }
            // 在节点字段列表最后增加 dtEventTimeStamp 字段
            // 在 AvroKeyedSerializationSchema 中取最后一位为 msg key
            NodeField nodeField = new NodeField();
            nodeField.setField(ConstantVar.EVENT_TIMESTAMP);
            nodeField.setType("long");
            nodeField.setOrigin("");
            nodeField.setEventTime(false);
            nodeField.setDescription(ConstantVar.EVENT_TIMESTAMP);
            List<NodeField> nodeFields = sinkNode.getFields();
            nodeFields.add(nodeField);
            sinkNode.setFields(nodeFields);
            String transformedResultTableId = entry.getKey();
            dataStreams.put(transformedResultTableId,
                    this.appendEventTimeField(dataStreams.get(transformedResultTableId), eventTimeFieldIndex));
        }
    }

    /**
     * 根据 sinkNode 构造 transformNode, 用于后续可串接关联关系(当前仅用于当打点监控的 input 信息补齐)
     *
     * @return
     */
    private Map<String, TransformNode> constructTransformNodes() {
        Map<String, TransformNode> transformNodes = new HashMap<>();
        Collection<SourceNode> sourceNodes = this.getTopology().getSourceNodes().values();
        for (Map.Entry<String, SinkNode> entry : this.getTopology().getSinkNodes().entrySet()) {
            SinkNode sinkNode = entry.getValue();
            TransformNode transformNode = new TransformNode();
            transformNode.setNodeId(sinkNode.getNodeId());
            transformNode.setNodeName(sinkNode.getNodeName());
            transformNode.setDescription(sinkNode.getDescription());
            transformNode.setFields(sinkNode.getFields());

            // 设置依赖关系
            transformNode.setParents(new ArrayList<>(sourceNodes));
            String resultTableId = entry.getKey();
            transformNodes.put(resultTableId, transformNode);
        }
        // 设置依赖关系
        for (SourceNode sourceNode : sourceNodes) {
            sourceNode.getChildren().addAll(transformNodes.values());
        }
        return transformNodes;
    }

    /**
     * 输出 dataStream 表结构校验
     *
     * @param dataStreams
     */
    private void validate(Map<String, DataStream<Row>> dataStreams) {
        Map<String, SinkNode> sinkNodes = this.getTopology().getSinkNodes();
        if (dataStreams.keySet().isEmpty()) {
            throw new RuntimeException("There is no output result table for the transformation.");
        }

        SchemaFactory schemaFactory = new SchemaFactory();

        for (Map.Entry<String, SinkNode> entry : sinkNodes.entrySet()) {
            String transformedResultTableId = entry.getKey();
            DataStream<Row> dataStream = dataStreams.get(transformedResultTableId);
            if (null == dataStream) {
                throw new RuntimeException(String.format(
                        "The result table(%s) from the output of transformation is not exist in node configuration.",
                        transformedResultTableId));
            }
            SinkNode sinkNode = entry.getValue();
            TableSchema tableSchema = this.getRuntime().getTableEnv().fromDataStream(dataStream).getSchema();
            // 根据用户输出 dataStream 的 fieldName 重排 sinkNode 字段顺序(原顺序是通过 API 获取)
            String[] transformRTFieldNames = tableSchema.getFieldNames();
            TypeInformation<?>[] transformedRTFieldTypes = tableSchema.getFieldTypes();
            List<NodeField> unSortedSinkNodeFields = sinkNode.getFields();
            try {
                // 1. 判断字段长度是否匹配
                if (transformRTFieldNames.length != unSortedSinkNodeFields.size()) {
                    throw new ResultTableFieldMatchException(
                            String.format("Fields length match error for %s", transformedResultTableId));
                }
                // 2. 重新初始化 sinkNode fields，判断字段名称-类型是否匹配
                List<NodeField> sortedSinkNodeFields = new SortFields(
                        unSortedSinkNodeFields, transformRTFieldNames).getSortedNodeFieldsByName();
                sinkNode.setFields(sortedSinkNodeFields);
                TypeInformation<?>[] sinkRTFieldTypes = schemaFactory.getFieldsTypes(sinkNode);
                for (int i = 0; i < tableSchema.getFieldCount(); i++) {
                    if (!sortedSinkNodeFields.get(i).getField().equals(transformRTFieldNames[i])
                            || !sinkRTFieldTypes[i].equals(transformedRTFieldTypes[i])) {
                        throw new ResultTableFieldMatchException(
                                String.format(
                                        "The output field of result table(%s) are not match: "
                                                + "%n==> Expected: (%s:%s)%n==> Actual: (%s:%s).",
                                        transformedResultTableId,
                                        sortedSinkNodeFields.get(i).getField(), sinkRTFieldTypes[i],
                                        transformRTFieldNames[i], transformedRTFieldTypes[i])
                        );
                    }
                }
            } catch (ResultTableFieldMatchException exception) {
                LOGGER.error(exception.getMessage());
                List<String> sinkFields = new ArrayList<>();
                for (NodeField field : sinkNodes.get(transformedResultTableId).getFields()) {
                    sinkFields.add(String
                            .format("(%s:%s)", field.getField(), schemaFactory.callTypeInfo(field.getType())));
                }
                List<String> outputFields = new ArrayList<>();
                for (int i = 0; i < transformedRTFieldTypes.length; i++) {
                    outputFields.add(String.format("(%s:%s)", transformRTFieldNames[i], transformedRTFieldTypes[i]));
                }
                throw new RuntimeException(String.format(
                        "The output field of result table(%s) are not match: %n==> Expected: %s%n==> Actual: %s.",
                        transformedResultTableId,
                        StringUtils.join(sinkFields, ", "),
                        StringUtils.join(outputFields, ", "))
                );
            }
        }
    }

    @Override
    public FlinkCodeRuntime createRuntime() {
        return new FlinkCodeRuntime(this.getTopology());
    }
}
