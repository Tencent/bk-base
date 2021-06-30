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
import com.tencent.bk.base.dataflow.core.metric.MetricDataDelay;
import com.tencent.bk.base.dataflow.core.metric.MetricDataLoss;
import com.tencent.bk.base.dataflow.core.metric.MetricDataStatics;
import com.tencent.bk.base.dataflow.core.metric.MetricDataStructure;
import com.tencent.bk.base.dataflow.core.metric.MetricObject;
import com.tencent.bk.base.dataflow.core.metric.MetricStream;
import com.tencent.bk.base.dataflow.core.metric.MetricTag;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNodeMetric extends AbstractBasicMetric {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeMetric.class);
    // 创建消息数类监控消息的时候需要锁定
    private static Lock createMetricsLock = new ReentrantLock();
    protected AtomicLong outputTotalCount = new AtomicLong(0);
    protected StreamTopology topology;
    private String module;
    private String component;
    private String cluster;
    private int taskId;
    private String topologyId;
    private Node node;
    private String tag;
    private String timeZone;
    private String metricNodeid;
    private long nowTime;
    private AtomicLong outputTotalCountIncrement = new AtomicLong(0);
    private AtomicLong inputTotalCount = new AtomicLong(0);
    private AtomicLong inputTotalCountIncrement = new AtomicLong(0);
    private Map<String, Long> inputTags = new HashMap<>();
    private Map<String, Long> tags = new HashMap<>();
    private Map<String, Long> dropMap = new HashMap<>();
    private Map<String, Long> malformedMetricMap = new HashMap<>();
    private DelayMetricSample inputDelayMetric = new DelayMetricSample();
    private DelayMetricSample outputDelayMetricSample = new DelayMetricSample();
    private DelayMetricSample outputDelayMetric;
    private MetricObject metricObject;
    private MetricTag physicalTag;
    private MetricTag logicalTag;
    private List<MetricStream> upstream;
    private List<MetricStream> downstream;
    private Runtime runtime;
    private long totalMem;
    private long usedMem;
    private String processNodeId;

    public AbstractNodeMetric(int taskId, Node node, StreamTopology topology, String processNodeId) {
        this.taskId = taskId;
        this.node = node;
        this.topology = topology;
        this.module = topology.getModule();
        // 打点日志增加处理节点ID
        this.processNodeId = processNodeId;
        //this.cluster = topology.getParams().get("cluster_name").toString();
        this.cluster = "default";

        this.component = topology.getComponent();
        this.topologyId = topology.getJobName();
        this.metricNodeid = node.getNodeId();
        timeZone = topology.getTimeZone();
        init();
    }


    /**
     * 周期性初始化
     */
    public void init() {

        physicalTag = new MetricTag();
        logicalTag = new MetricTag();

        upstream = new ArrayList<>();
        downstream = new ArrayList<>();

        // tags = new HashMap<>();
        // outputTotalCountIncrement.set(0);
        // dropMap = new HashMap<>();
        // inputTags = new HashMap<>();
        // inputTotalCountIncrement.set(0);

        // 这里为初始化待发送给监控的对象
        metricObject = new MetricObject();

        nowTime = System.currentTimeMillis() / 1000;

        runtime = Runtime.getRuntime();
        totalMem = runtime.totalMemory();
        usedMem = totalMem - runtime.freeMemory();

    }

    /**
     * metric 数据计数器 包括 tag计数 增量计数 全量计数
     */
    public void recordCounter(Row value, String outputTag) {
        // 增量计数
        outputTotalCountIncrement.incrementAndGet();
        // 全量计数
        outputTotalCount.incrementAndGet();
        // tag 计数
        // String inputTag = String.format("%s|%s|%s_%s", topologyId, metricNodeid, taskId, nowTime);
        if (null == outputTag) {
            outputTag = String.format("%s|%s|%s_%s", topologyId, metricNodeid, taskId, nowTime);
        }
        if (tags.containsKey(outputTag)) {
            tags.put(outputTag, tags.get(outputTag) + 1);
        } else {
            tags.put(outputTag, 1L);
        }
        // 延迟采样
        if (outputTotalCountIncrement.get() < 100) {
            try {
                if (null == outputDelayMetricSample) {
                    outputDelayMetricSample = new DelayMetricSample();
                }
                outputDelayMetricSample.setDelayTimeMetric(value.getField(0).toString());
            } catch (Exception e) {
                // 采样不输出异常
            }
        }
    }

    /**
     * 统计丢弃记录
     *
     * @param reason 丢弃原因
     * @param value 丢弃的记录
     */
    public void recordDrop(String reason, Row value) {
        Long dropCnt = dropMap.get(reason);
        if (null == dropCnt) {
            dropCnt = 0L;
        }
        dropMap.put(reason, dropCnt + 1);
    }

    /**
     * 统计数据格式错误
     *
     * @param malformedType 格式错误类型
     * @param value 记录
     */
    public void recordDataMalformed(String malformedType, Object value) {
        Long dataMalformedCnt = malformedMetricMap.get(malformedType);
        if (null == dataMalformedCnt) {
            dataMalformedCnt = 0L;
        }
        malformedMetricMap.put(malformedType, dataMalformedCnt + 1);
    }


    public AtomicLong getInputTotalCount() {
        return inputTotalCount;
    }

    public DelayMetricSample getInputDelayMetric() {
        return inputDelayMetric;
    }

    public DelayMetricSample getOutputDelayMetric() {
        return outputDelayMetric;
    }

    public int getTaskId() {
        return taskId;
    }

    public Node getNode() {
        return node;
    }

    /**
     * 构造databus的upstream
     *
     * @param topic upstream的topic
     * @return 构造的upstream信息
     */
    private MetricStream setDatabusUpStream(final String topic) {
        MetricStream metricStream = new MetricStream();
        metricStream.setModule("databus");
        metricStream.setComponent("kafka-inner");

        List<MetricTag> tags = new ArrayList<>();
        MetricTag tag = new MetricTag();
        tag.setTag(topic);
        tag.setDesc(new HashMap<String, Object>() {
            {
                put("topic", topic);
            }
        });
        tags.add(tag);
        metricStream.setLogicalTag(tags);
        return metricStream;
    }

    /**
     * 构造databus的downstream
     *
     * @param nodeId 对应的node id
     * @param topic 对应的topic
     * @return 返回downstream的信息
     */
    private MetricStream setDatabusDownStream(final String nodeId, final String topic) {
        MetricStream metricStream = new MetricStream();
        metricStream.setModule("databus");
        metricStream.setComponent("kafka-inner");

        List<MetricTag> tags = new ArrayList<>();
        MetricTag tag = new MetricTag();
        tag.setTag(topic + "|" + nodeId);
        tag.setDesc(new HashMap<String, Object>() {
            {
                put("topic", topic);
                put("result_table_id", nodeId);
            }
        });
        tags.add(tag);
        metricStream.setLogicalTag(tags);
        return metricStream;
    }

    /**
     * 设置以node为上下游的信息
     *
     * @param nodeId 对应的node id
     * @return 返回上下游信息
     */
    private MetricStream setNodeStream(final String nodeId) {
        MetricStream metricStream = new MetricStream();
        metricStream.setModule(module);
        metricStream.setComponent(component);

        List<MetricTag> tags = new ArrayList<>();
        MetricTag tag = new MetricTag();
        tag.setTag(nodeId);
        tag.setDesc(new HashMap<String, Object>() {
            {
                put("result_table_id", nodeId);
            }
        });
        tags.add(tag);
        metricStream.setLogicalTag(tags);
        return metricStream;
    }

    private void setAllStream(Node node) {
        // 构造upstream 上游为node或databus
        if (!(node instanceof SourceNode)) {
            for (final Node upNode : node.getParents()) {
                if (upNode instanceof SourceNode) {
                    upstream.add(setDatabusUpStream(NodeUtils.generateKafkaTopic(upNode.getNodeId())));
                } else {
                    upstream.add(setNodeStream(node.getNodeId()));
                }
            }
        }
        // 构造downstream 下游node/databus
        if (!node.getChildren().isEmpty()) {
            for (final Node downNode : node.getChildren()) {
                downstream.add(setNodeStream(downNode.getNodeId()));
            }
        }
        if (topology.getSinkNodes().containsKey(node.getNodeId())) {
            downstream.add(setDatabusDownStream(metricNodeid, NodeUtils.generateKafkaTopic(node.getNodeId())));
        }
    }

    private void createInfo() {
        // module
        metricObject.setModule(module);

        // component
        metricObject.setComponent(component);

        // cluster
        metricObject.setCluster(cluster);

        // storage
        if (node instanceof SinkNode) {
            SinkNode sinkNode = (SinkNode) node;
            String servers = sinkNode.getOutput().getOutputInfo().toString();
            String server = servers.split(",")[0];
            metricObject.setStorage(new HashMap<String, Object>() {
                {
                    put("host", server.split(":")[0]);
                    put("port", Integer.parseInt(server.split(":")[1]));
                }
            });
        } else {
            metricObject.setStorage(new HashMap<String, Object>() {
                {
                    put("storage_type", null);
                    put("storage_id", null);
                }
            });
        }

        // physical tag
        tag = String.format("%s|%s|%d", topologyId, metricNodeid, taskId);
        physicalTag.setTag(tag);
        physicalTag.setDesc(new HashMap<String, Object>() {
            {
                put("topology_id", topologyId);
                put("result_table_id", metricNodeid);
                put("task_id", taskId);
            }
        });
        metricObject.setPhysicalTag(physicalTag);

        // logical tag
        logicalTag.setTag(metricNodeid);
        logicalTag.setDesc(new HashMap<String, Object>() {
            {
                put("result_table_id", metricNodeid);
            }
        });
        metricObject.setLogicalTag(logicalTag);

        // custom tag
        metricObject.setCustomTags(new HashMap<String, Object>() {
            {
                put("ip", "");
                put("port", "");
                put("task", taskId);
                // 处理节点ID的tag
                put("process_node", processNodeId);
            }
        });
    }

    /**
     * 在数据流中的位置信息 location 包括 upstream 和 downstream
     */
    private void createLocation() {
        setAllStream(node);
        metricObject.setUpstream(upstream);
        metricObject.setDownStream(downstream);
    }

    /**
     * 构造metric中的metrics
     */
    private void createMetrics() {
        // data loss
        createMetricsLock.lock();
        try {

            MetricDataStatics input = new MetricDataStatics();
            input.setTotalCnt(inputTotalCount.get());
            input.setTotalCntIncrement(inputTotalCountIncrement.get());
            input.setTags(inputTags);
            MetricDataLoss dataLoss = new MetricDataLoss();
            dataLoss.setInput(input);

            MetricDataStatics output = new MetricDataStatics();
            output.setTotalCnt(outputTotalCount.get());
            output.setTotalCntIncrement(outputTotalCountIncrement.get());
            output.setTags(tags);
            //checkpoint阶段有存储计数
            dataLoss.setOutput(output);
            dataLoss.setDataDrop(dropMap);
            metricObject.setDataLoss(dataLoss);

            // 数据格式错误打点，并非每个 RT 都需要打点
            if (malformedMetricMap.size() > 0) {
                MetricDataStructure dataStructure = new MetricDataStructure();
                dataStructure.setMalformedMap(malformedMetricMap);
                metricObject.setDataStructure(dataStructure);
                malformedMetricMap = new HashMap<>();
            }

            // 重置计数器
            tags = new HashMap<>();
            outputTotalCountIncrement.set(0);
            dropMap = new HashMap<>();
            inputTags = new HashMap<>();
            inputTotalCountIncrement.set(0);

            // 重置延迟指标
            if (null == inputDelayMetric) {
                inputDelayMetric = new DelayMetricSample();
            }
            outputDelayMetric = outputDelayMetricSample;
            // 重建采样
            outputDelayMetricSample = new DelayMetricSample();

            inputDelayMetric.reset();
            outputDelayMetricSample.reset();

        } finally {
            createMetricsLock.unlock();
        }

        // resource monitor
        metricObject.setResourceMonitor(new HashMap<String, Object>() {
            {
                put("stream_flink_mem", new HashMap<String, Object>() {
                    {
                        put("total", totalMem);
                        put("used", usedMem);
                        put("tags", tag);
                    }
                });
            }
        });
    }

    /**
     * 延续信息是从上级节点的输出采样信息中获取 需更新
     */
    public void updateMetricDataDelay() {
        // data delay
        if (null != inputDelayMetric && null != inputDelayMetric.getMinDelayDelayTime()) {
            MetricDataDelay dataDelay = new MetricDataDelay();
            dataDelay.setWindowTime(NodeUtils.getCountFreq(node));
            dataDelay.setWaitingTime(NodeUtils.getWaitingTime(node));
            dataDelay.setMinDelayOutputTime(inputDelayMetric.getMinDelayOutputTime());
            dataDelay.setMinDelayDataTime(inputDelayMetric.getMinDelayDataTime());
            dataDelay.setMinDelayDelayTime(inputDelayMetric.getMinDelayDelayTime());
            dataDelay.setMaxDelayOutputTime(inputDelayMetric.getMaxDelayOutputTime());
            dataDelay.setMaxDelayDataTime(inputDelayMetric.getMaxDelayDataTime());
            dataDelay.setMaxDelayDelayTime(inputDelayMetric.getMaxDelayDelayTime());
            metricObject.setDataDelay(dataDelay);
        }
    }

    /**
     * 汇总metric打点的信息
     */
    public void createMetricObject() {
        createInfo();
        createLocation();
        createMetrics();
        metricObject.setTimeZone(timeZone);
    }

    public String getMetricObjectInfo() {
        return this.metricObject.collectMetricMessage();
    }

    public MetricObject getMetricObject() {
        return this.metricObject;
    }

    @Override
    public abstract void save();
}
