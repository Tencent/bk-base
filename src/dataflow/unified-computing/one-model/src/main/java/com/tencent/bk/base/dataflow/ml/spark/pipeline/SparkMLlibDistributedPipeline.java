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

package com.tencent.bk.base.dataflow.ml.spark.pipeline;

import com.tencent.bk.base.dataflow.core.pipeline.AbstractDefaultPipeline;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.ml.spark.sink.MLSinkStage;
import com.tencent.bk.base.dataflow.ml.spark.source.MLSourceStage;
import com.tencent.bk.base.dataflow.ml.metric.DebugConstant;
import com.tencent.bk.base.dataflow.ml.spark.transform.MLTransformStage;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMLlibDistributedPipeline extends AbstractDefaultPipeline<ModelTopology, SparkMLlibRuntime> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkMLlibDistributedPipeline.class);

    private MLSourceStage sourceStage;
    private MLTransformStage transformStage;
    private MLSinkStage sinkStage;

    public SparkMLlibDistributedPipeline(Topology modelTopology) {
        super(modelTopology);
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        SparkSession sparkSession = getRuntime().getEnv();
        DebugConstant.initRunMode(sparkSession, this.getTopology());
        this.sourceStage = new MLSourceStage(models, dataSets, sparkSession);
        this.transformStage = new MLTransformStage(models, dataSets, sparkSession);
        this.sinkStage = new MLSinkStage(models, dataSets, sparkSession);
    }

    @Override
    public SparkMLlibRuntime createRuntime() {
        return new SparkMLlibRuntime(this.getTopology());
    }

    @Override
    public void source() {
        this.getTopology().getSourceNodes()
                .forEach((nodeName, sourceNode) -> sourceStage.source(sourceNode));
    }


    @Override
    public void transform() {
        // todo 支持spark sql
        this.getTopology().getTransformNodes()
                .forEach((nodeName, transformNode) -> transformStage.transform(transformNode));
    }

    @Override
    public void sink() {
        this.getTopology().getSinkNodes()
                .forEach((nodeName, sinkNode) -> sinkStage.sink(sinkNode));
    }


    /**
     * 执行根据 {@link ModelTopology} 构建的 {@link SparkMLlibDistributedPipeline}
     *
     * <p>执行过程包含三个阶段 {@link MLSourceStage} {@link MLTransformStage} {@link MLSinkStage}</p>
     */
    @Override
    public void submit() {
        try {
            source();
            transform();
            sink();
        } finally {
            this.getRuntime().close();
        }
    }

}
