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

package com.tencent.bk.base.dataflow.flink.streaming.pipeline;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.pipeline.AbstractFlinkPipeline;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.FlinkFunctionFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.transform.TransformStage;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.sink.KafkaSink;
import com.tencent.bk.base.dataflow.flink.streaming.source.SourceStage;
import com.tencent.bk.base.dataflow.udf.UdfRegister;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkStreamingPipeline extends AbstractFlinkPipeline<FlinkStreamingTopology, FlinkStreamingRuntime> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStreamingPipeline.class);
    private Map<String, DataStream<Row>> dataStreams = new HashMap<>();

    public FlinkStreamingPipeline(Topology topology) {
        super(topology);
    }

    /**
     * 获取flink的runtime
     *
     * @return flink-streaming runtime
     */
    @Override
    public FlinkStreamingRuntime createRuntime() {
        return new FlinkStreamingRuntime(this.getTopology());
    }

    @Override
    public void submit() throws Exception {
        this.prepare();
        this.source();
        this.transform();
        this.sink();
        this.getRuntime().execute();
    }

    @Override
    public void prepare() {
        // register internal function
        new FlinkFunctionFactory(this.getRuntime()).registerAll();
        // register user defined function
        registerUserDefinedFunction(this.getRuntime());
    }

    @Override
    public void source() {
        // source
        for (Map.Entry<String, SourceNode> entry : getTopology().getSourceNodes().entrySet()) {
            SourceNode sourceNode = entry.getValue();
            try {
                this.dataStreams.put(sourceNode.getNodeId(),
                        new SourceStage(sourceNode, this.getRuntime()).source());
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Failed to create source node with " + sourceNode.getNodeId());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void transform() {
        // transform
        for (Map.Entry<String, TransformNode> entry : getTopology().getTransformNodes().entrySet()) {
            TransformNode transformNode = entry.getValue();
            try {
                new TransformStage(transformNode, this.dataStreams, this.getRuntime()).transform();
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Transform node {} run failed. {}", transformNode.getNodeId(), e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sink() {
        // sink
        for (Map.Entry<String, SinkNode> entry : getTopology().getSinkNodes().entrySet()) {
            SinkNode sinkNode = entry.getValue();
            try {
                new KafkaSink(sinkNode, this.dataStreams, this.getRuntime()).createNode();
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Sink node {} failed.", sinkNode.getNodeId());
                throw new RuntimeException(e);
            }
        }
    }

    private void registerUserDefinedFunction(FlinkStreamingRuntime runtime) {
        this.getTopology().getUserDefinedFunctions().getFunctions()
                .forEach(functionConfig -> {
                    Object udfInstance = UdfRegister.getUdfInstance(
                            ConstantVar.Role.stream.toString(),
                            functionConfig.getName(),
                            functionConfig.getLanguage());
                    if (udfInstance instanceof ScalarFunction) {
                        runtime.getTableEnv().registerFunction(functionConfig.getName(), (ScalarFunction) udfInstance);
                    } else if (udfInstance instanceof TableFunction) {
                        runtime.getTableEnv().registerFunction(functionConfig.getName(), (TableFunction) udfInstance);
                    } else if (udfInstance instanceof AggregateFunction) {
                        runtime.getTableEnv()
                                .registerFunction(functionConfig.getName(), (AggregateFunction) udfInstance);
                    } else {
                        throw new RuntimeException("Failed to register udf " + functionConfig.getName());
                    }
                });
    }
}
