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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.metric.MetricDataStatics;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricManger {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricManger.class);
    private Map<String, AbstractNodeMetric> nodeMetricMap = new HashMap<>();
    private long nowTime = System.currentTimeMillis() / 1000;
    private long initTime = System.currentTimeMillis() / 1000;
    private Lock tickLock = new ReentrantLock();

    /**
     * 注册rt的metric
     *
     * @param taskId task id
     * @param node rt
     */
    public void registerMetric(int taskId, Node node, StreamTopology topology, String parentNodeId) {
        String nodeAndTaskId = MetricManger.getNodeMetricId(node, taskId);
        if (!nodeMetricMap.containsKey(nodeAndTaskId)) {
            switch (ConstantVar.RunMode.valueOf(topology.getRunMode())) {
                case product:
                    nodeMetricMap.put(nodeAndTaskId, new MonitorMetric(taskId, node, topology, parentNodeId));
                    break;
                case udf_debug:
                case debug:
                    nodeMetricMap.put(nodeAndTaskId, new DebugMetric(taskId, node, topology, parentNodeId));
                    break;
                default:
                    nodeMetricMap.put(nodeAndTaskId, new DummyMetric(taskId, node, topology, parentNodeId));
            }
        }
    }

    /**
     * 当前各节点只统计各个节点 task 的 output，对非数据源节点，将当前节点对应 taskId (下游无则创建) 的 output 赋给所有子节点对应 input
     * 创建metric info并发送至kafka
     */
    public void saveAndInitNodesMetric(StreamTopology topology) throws ParseException {
        for (Map.Entry<String, AbstractNodeMetric> entry : nodeMetricMap.entrySet()) {
            entry.getValue().createMetricObject();
        }

        Map<String, AbstractNodeMetric> tmpNnodeMetricMap = new HashMap<>();
        tmpNnodeMetricMap.putAll(nodeMetricMap);

        for (Map.Entry<String, AbstractNodeMetric> entry : tmpNnodeMetricMap.entrySet()) {
            // 存在两个相同的Nodeid
            Node currentNode = entry.getValue().getNode();
            int taskId = entry.getValue().getTaskId();
            if (!(currentNode instanceof SinkNode)) {
                // Transform 当前只在 output 的 stream 进行了统计 metric，若 group-by（output 和 input 不在同一个 subtask 中）
                // 该逻辑只是保证节点间 input 和 output 整体能对上
                String currentNodeAndTaskId = MetricManger.getNodeMetricId(currentNode, taskId);
                Long currentOutputCount = 0L;
                Long currentOutputCountIncrement = 0L;
                Map<String, Long> outTags = new HashMap<>();
                DelayMetricSample outputDelayMetric = null;
                if (nodeMetricMap.containsKey(currentNodeAndTaskId)) {
                    // output还可能在继续变化中
                    MetricDataStatics output = nodeMetricMap.get(currentNodeAndTaskId).getMetricObject().getDataLoss()
                            .getOutput();
                    currentOutputCount = output.getTotalCnt();
                    currentOutputCountIncrement = output.getTotalCntIncrement();
                    outTags = output.getTags();
                    outputDelayMetric = nodeMetricMap.get(currentNodeAndTaskId).getOutputDelayMetric();
                }
                // 只有输出节点，才需要找子节点
                if (currentOutputCount > 0) {
                    for (Node child : currentNode.getChildren()) {
                        String childMetricId = MetricManger.getNodeMetricId(child, taskId);
                        if (!nodeMetricMap.containsKey(childMetricId)) {
                            // 创建的子node
                            registerMetric(taskId, child, topology, currentNode.getNodeId());
                            AbstractNodeMetric nodeMetric = nodeMetricMap.get(childMetricId);
                            nodeMetric.createMetricObject();
                        }
                        AbstractNodeMetric childNodeMetric = nodeMetricMap.get(childMetricId);
                        // 输入统计
                        MetricDataStatics input = childNodeMetric.getMetricObject().getDataLoss().getInput();
                        input.setTotalCnt(input.getTotalCnt() + currentOutputCount);
                        input.setTotalCntIncrement(input.getTotalCntIncrement() + currentOutputCountIncrement);
                        input.getTags().putAll(outTags);
                        // 输入延迟
                        if (null != outputDelayMetric) {
                            childNodeMetric.getInputDelayMetric().merge(outputDelayMetric);
                            childNodeMetric.updateMetricDataDelay();
                        }

                    }
                }
            }
        }
        for (Map.Entry<String, AbstractNodeMetric> entry : nodeMetricMap.entrySet()) {
            Node currentNode = topology.queryNode(entry.getKey().split(ConstantVar.METRIC_ID_SEPARATOR)[0]);
            if (!(currentNode instanceof SourceNode)) {
                entry.getValue().save();
            }
        }
        initNodesMetric();
    }

    /**
     * 所有node的metric同时初始化
     */
    public void initNodesMetric() {
        for (AbstractNodeMetric oneMetric : nodeMetricMap.values()) {
            oneMetric.init();
        }
    }

    /**
     * 获取metric对象
     *
     * @return
     */
    public AbstractNodeMetric getMetricObject(String nodeAndTaskId) {
        return nodeMetricMap.get(nodeAndTaskId);
    }


    /**
     * metric 定时器
     *
     * @return
     */
    public Boolean isTick(Integer interval) {
        tickLock.lock();
        try {
            nowTime = System.currentTimeMillis() / 1000;
            if ((nowTime - initTime) >= interval) {
                initTime = System.currentTimeMillis() / 1000;
                return true;
            }
            return false;
        } finally {
            tickLock.unlock();
        }
    }

    /**
     * 获取接 Metric 的ID
     *
     * @param node 节点
     * @param taskId 任务ID
     * @return
     */
    public static String getNodeMetricId(Node node, int taskId) {
        return node.getNodeId() + ConstantVar.METRIC_ID_SEPARATOR + taskId + ConstantVar.METRIC_ID_SEPARATOR + NodeUtils
                .getNodeType(node);
    }
}
