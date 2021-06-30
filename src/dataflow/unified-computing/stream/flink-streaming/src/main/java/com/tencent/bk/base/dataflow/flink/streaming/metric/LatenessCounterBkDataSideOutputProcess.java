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

package com.tencent.bk.base.dataflow.flink.streaming.metric;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.metric.MetricManger;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.api.BkDataSideOutputProcess;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatenessCounterBkDataSideOutputProcess implements BkDataSideOutputProcess {

    private Node node;
    private FlinkStreamingTopology topology;

    public LatenessCounterBkDataSideOutputProcess(Node node, FlinkStreamingTopology topology) {
        this.node = node;
        this.topology = topology;
    }

    @Override
    public void process(DataStream<CRow> dataStream, RowSchema inputSchema) {
        // 统计延迟数据
        dataStream.map(new LatenessMetricMapper(node, topology));
    }

    /**
     * 延迟数据打点统计
     */
    private static class LatenessMetricMapper extends RichMapFunction<CRow, CRow> implements Serializable,
            ProcessingTimeCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(LatenessMetricMapper.class);

        private static final long serialVersionUID = 1L;

        private Node node;
        private String nodeAndTaskId;
        private FlinkStreamingTopology topology;
        private Integer interval;
        private int taskId;
        // 变更为非静态类
        private transient MetricManger metricManger;
        private transient ProcessingTimeService processingTimeService;

        LatenessMetricMapper(Node node, FlinkStreamingTopology topology) {
            this.node = node;
            this.topology = topology;
            this.interval = RunMode.product == ConstantVar.RunMode.valueOf(topology.getRunMode()) ? 60 : 6;
        }

        /**
         * 延迟数据打点初始化工作
         *
         * @param config config
         */
        @Override
        public void open(Configuration config) {
            this.taskId = getRuntimeContext().getIndexOfThisSubtask();
            this.nodeAndTaskId = MetricManger.getNodeMetricId(node, taskId);
            this.metricManger = new MetricManger();
            this.metricManger.registerMetric(taskId, node, topology, node.getNodeId());
            this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
            // 注册处理时间
            this.registerProcessingTime(this.interval);
        }

        /**
         * 延迟数据丢弃数统计
         *
         * @param value 数据
         * @return row
         */
        @Override
        public CRow map(CRow value) {
            try {
                this.metricManger.getMetricObject(nodeAndTaskId).recordDrop("window_delay_drop", null);
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
}
