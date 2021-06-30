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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.common.HttpUtils;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugMetric extends AbstractNodeMetric implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugMetric.class);
    private String metricMessage;
    private Node node;
    private ObjectMapper objectMapper = new ObjectMapper();
    private StreamTopology topology;

    public DebugMetric(int taskId, Node node, StreamTopology topology, String processNodeId) {
        super(taskId, node, topology, processNodeId);
        this.node = node;
        this.topology = topology;
    }

    @Override
    public void save() {
        metricMessage = super.getMetricObjectInfo();
        if (outputTotalCount.get() != 0L) {
            reportMetricInfoByRestAPi(topology.getDebugConfig().getDebugNodeMetricApi(), node.getNodeId(),
                    super.getInputTotalCount().get(), outputTotalCount.get(), topology.getJobId(),
                    topology.getDebugConfig().getDebugId());
        }
        LOGGER.debug(String.format("The node %s metric is %s", node.getNodeId(), metricMessage));
    }

    private void reportMetricInfoByRestAPi(String url, String nodeId, long inputTotalCnt, long outputTotalCnt,
            String jobId, String debugId) {
        try {
            // 构造api请求体
            Map<String, Object> urlParameters = new HashMap<>();
            urlParameters.put("input_total_count", inputTotalCnt);
            urlParameters.put("output_total_count", outputTotalCnt);
            urlParameters.put("filter_discard_count", 0);
            urlParameters.put("transformer_discard_count", 0);
            urlParameters.put("aggregator_discard_count", 0);
            urlParameters.put("job_id", jobId);
            urlParameters.put("result_table_id", nodeId);

            LOGGER.info("Begin to reportMetricInfo metric data to mysql by {} and paramerters is {} and url is {}",
                    nodeId, urlParameters.toString(), MessageFormat.format(url, debugId));
            String result = HttpUtils.post(MessageFormat.format(url, debugId), urlParameters);
            JsonNode tree = objectMapper.readTree(result);
            if (!tree.get("result").asBoolean()) {
                throw new RuntimeException("the set metric data api return value is false.");
            }
        } catch (Exception e) {
            LOGGER.error("failed to reportMetricInfo metric data to mysql", e);
        } finally {
            LOGGER.info(String.format("End of reportMetricInfo metric data to mysql by rt[%s].", nodeId));
        }
    }
}
