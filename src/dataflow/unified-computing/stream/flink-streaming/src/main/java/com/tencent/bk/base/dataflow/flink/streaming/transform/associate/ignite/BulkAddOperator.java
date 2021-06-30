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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.STATIC_JOIN_DEFAULT_BATCH_SIZE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS;

import com.google.common.collect.Lists;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.metric.MetricManger;
import java.util.List;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkAddOperator extends AbstractStreamOperator<List<Row>>
        implements OneInputStreamOperator<Row, List<Row>>, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkAddOperator.class);
    private TransformNode node;
    private FlinkStreamingTopology topology;
    private Integer interval;
    private long lastGetTime;
    private List<Row> inputs;
    private int batchSize;
    private transient StreamRecord<Row> currentInput;
    private transient MetricManger metricManger;
    private transient ListState<Row> checkpointedState;
    private transient ProcessingTimeService processingTimeService;

    public BulkAddOperator(TransformNode node, FlinkStreamingTopology topology) {
        this.node = node;
        this.topology = topology;
        this.interval = RunMode.product == ConstantVar.RunMode.valueOf(topology.getRunMode()) ? 60 : 6;
        this.inputs = Lists.newArrayList();
        this.batchSize = STATIC_JOIN_DEFAULT_BATCH_SIZE;
    }

    @Override
    public void open() {
        initMetricManager();
        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        this.lastGetTime = currentProcessingTime;
        processingTimeService.registerTimer(currentProcessingTime + STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS, this);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<Row> descriptor =
                new ListStateDescriptor<>(
                        "batch-state",
                        TypeInformation.of(new TypeHint<Row>() {
                        }));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            checkpointedState.get().forEach(row -> inputs.add(row));
            checkpointedState.clear();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpointedState.clear();
        checkpointedState.addAll(inputs);
    }

    @Override
    public void processElement(StreamRecord<Row> input) {
        this.currentInput = input;
        inputs.add(input.getValue());
        if (shouldGetData()) {
            flushData();
        }
    }

    @Override
    public void onProcessingTime(long l) {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        if (lastGetTime < currentProcessingTime - STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS && inputs.size() > 0) {
            flushData();
            refreshMetric();
        }
        processingTimeService.registerTimer(currentProcessingTime + STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS, this);
    }

    private void flushData() {
        lastGetTime = processingTimeService.getCurrentProcessingTime();
        if (currentInput == null) {
            return;
        }
        StreamRecord<Row> copyCurrentInput = currentInput.copy(currentInput.getValue());
        output.collect(copyCurrentInput.replace(inputs));
        inputs = Lists.newArrayList();
    }

    private boolean shouldGetData() {
        boolean shouldGetDataFromIgnite = false;
        if (inputs.size() >= getBatchSize()) {
            shouldGetDataFromIgnite = true;
        }
        return shouldGetDataFromIgnite;
    }

    private void initMetricManager() {
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        this.metricManger = new MetricManger();
        this.metricManger.registerMetric(taskId, node, topology, node.getNodeId());
    }

    private void refreshMetric() {
        try {
            if (this.metricManger.isTick(interval)) {
                // save and init metric info
                this.metricManger.saveAndInitNodesMetric(topology);
            }
        } catch (Exception e) {
            // 打点异常不影响正常流程
            LOGGER.warn("BulkAddMetric error.", e);
        }
    }

    @VisibleForTesting
    protected int getBatchSize() {
        return batchSize;
    }
}
