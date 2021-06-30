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

package com.tencent.bk.base.dataflow.ml.metric;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.ChannelType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.ml.util.ModelNodeUtil;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import com.tencent.bk.base.dataflow.metrics.DataPlatformMetric;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNodeMetric implements BasicMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeMetric.class);
    protected ModelTopology topology;
    protected String component;
    protected String cluster;
    protected String module;
    protected Node node;
    protected String topologyId;
    protected String metricNodeid;
    protected Dataset<Row> inputDataset;
    protected Dataset<Row> outputDataset;

    protected DataPlatformMetric.Builder builder;

    public AbstractNodeMetric(ModelTopology topology) {
        this.topology = topology;
        this.component = topology.getJobType();
        this.cluster = topology.getClusterGroup();
        this.topologyId = topology.getJobId();
        this.module = ConstantVar.Role.modeling.toString();
    }

    public void setMetricInfo(Node node, Dataset<Row> inputDataset, Dataset<Row> outputDataset) {
        this.node = node;
        this.inputDataset = inputDataset;
        this.outputDataset = outputDataset;
        this.metricNodeid = node.getNodeId();
    }

    public abstract void initBuilder();

    public abstract void reportMetric() throws Exception;

    /**
     * 生成用于上报的DataPlatformMetric对象
     */
    public DataPlatformMetric buildMetric() {
        String ip = null;
        try {
            ip = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            ip = topologyId;
        }
        Map<String, Object> physicalTagDesc = new HashMap<String, Object>();
        physicalTagDesc.put("hostname", ip);
        physicalTagDesc.put("result_table_id", metricNodeid);

        Map<String, Object> logicalTagDesc = new HashMap<>();
        logicalTagDesc.put("result_table_id", metricNodeid);

        DataPlatformMetric dataPlatformMetric = builder
                .module(this.module)
                .component(this.component)
                .cluster(this.cluster)
                .storageType(ChannelType.hdfs.toString())
                .storageId(0)
                .physicalTag("tag", String.format("%s|%s", ip, metricNodeid))
                .physicalTag("desc", physicalTagDesc)
                .logicalTag("tag", metricNodeid)
                .logicalTag("desc", logicalTagDesc)
                .customTag("execute_id", "")
                .storageHost(this.cluster).build();
        long inputCount = inputDataset.count();
        dataPlatformMetric.getInputTotalCntCounter().inc(inputCount);
        dataPlatformMetric.getInputTotalCntIncrementCounter().inc(inputCount);
        dataPlatformMetric.getInputTagsCounter(ModelNodeUtil.getSingleParentDataNode(node).getNodeId(), inputCount);

        long outputCount = 0;
        if (outputDataset != null) {
            outputCount = outputDataset.count();
        }
        dataPlatformMetric.getOutputTotalCntCounter().inc(outputCount);
        dataPlatformMetric.getOutputTotalCntIncrementCounter().inc(outputCount);
        dataPlatformMetric.getOutputTagsCounter(node.getNodeId()).inc(outputCount);
        return dataPlatformMetric;
    }

    public void save() {
        try {
            //调用打点url完成上报
            this.reportMetric();
        } catch (Throwable e) {
            LOGGER.warn("report metric error", e);
        }
    }
}
