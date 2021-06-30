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

package com.tencent.bk.base.dataflow.metric;

import com.tencent.bk.base.dataflow.topology.StreamTopology;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.Node;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricMapper extends RichMapFunction<Row, Row> implements Serializable, ProcessingTimeCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricMapper.class);

    private static final long serialVersionUID = 1L;

    private Node node;
    private String nodeAndTaskId;
    private StreamTopology topology;
    private Integer interval;
    private int taskId;
    // 变更为非静态类
    private transient MetricManger metricManger;
    private transient ProcessingTimeService processingTimeService;

    public MetricMapper(Node node, StreamTopology topology) {
        this.node = node;
        this.topology = topology;
        this.interval = RunMode.product == ConstantVar.RunMode.valueOf(topology.getRunMode()) ? 60 : 6;
    }

    @Override
    public void open(Configuration config) {
        this.realOpen(config, getRuntimeContext());
    }

    @Override
    public Row map(Row value) throws Exception {
        return this.realMap(value, null);
    }

    /**
     * metric mapper初始化工作
     *
     * @param config config
     * @param runtimeContext context
     */
    public void realOpen(Configuration config, RuntimeContext runtimeContext) {
        this.taskId = runtimeContext.getIndexOfThisSubtask();
        this.nodeAndTaskId = MetricManger.getNodeMetricId(node, taskId);
        this.metricManger = new MetricManger();
        this.metricManger.registerMetric(taskId, node, topology, node.getNodeId());
        this.processingTimeService = ((StreamingRuntimeContext) runtimeContext).getProcessingTimeService();
        // 注册处理时间
        this.registerProcessingTime(this.interval);
    }

    /**
     * 根据数据的map操作进行metric统计
     *
     * @param value 行
     * @param tag metric标签
     * @return row
     */
    public Row realMap(Row value, String tag) {
        try {
            // tag count
            this.metricManger.getMetricObject(nodeAndTaskId).recordCounter(value, tag);
        } catch (Exception e) {
            // 打点异常不影响正常流程
            LOGGER.warn("MetricMapper error.", e);
        }
        return value;
    }

    /**
     * 定时触发上报打点数据
     *
     * @param l 时间.
     * @throws Exception 异常.
     */
    @Override
    public void onProcessingTime(long l) throws Exception {
        try {
            // TODO:后续isTick检测可以去掉
            if (this.metricManger.isTick(interval)) {
                // save and init metric info
                this.metricManger.saveAndInitNodesMetric(topology);
            }
        } catch (Exception e) {
            // 打点异常不影响正常流程
            LOGGER.warn("MetricMapper error.", e);
        }
        this.registerProcessingTime(interval);
    }

    /**
     * 注册定时触发.
     */
    public void registerProcessingTime(Integer interval) {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        long inactiveBucketCheckInterval = interval * 1000L;
        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
    }

}

