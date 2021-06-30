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

package com.tencent.bk.base.dataflow.core.metric;

import com.tencent.bk.base.dataflow.core.common.Tools;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricObject.class);
    //info
    private String module;
    private String component;
    private String cluster;

    //info -- physical_tag
    private MetricTag physicalTag;

    //info -- logical_tag
    private MetricTag logicalTag;

    //info -- custom_tags
    private Map<String, Object> customTags;

    // storage
    private Map<String, Object> storage;

    //location -- upstream
    private List<MetricStream> upstream;

    //location -- downStream
    private List<MetricStream> downStream;

    //metrics
    private MetricDataLoss dataLoss;
    private MetricDataDelay dataDelay;

    private Map<String, Object> resourceMonitor;
    private Map<String, Object> customMetrics;
    // 格式不规范的数据打点
    private MetricDataStructure dataStructure;
    private String timeZone = "Asia/Shanghai";

    public String collectMetricMessage() {
        Map<String, Object> object = collectMetric();
        return Tools.writeValueAsString(object);
    }

    /**
     * metric信息汇总
     *
     * @return metric信息
     */
    public Map<String, Object> collectMetric() {
        Map<String, Object> metric = new HashMap<>();
        metric.put("time", System.currentTimeMillis() / 1000);
        metric.put("version", "2.0");
        //info
        Map<String, Object> info = new HashMap<>();
        info.put("module", module);
        info.put("component", component);
        info.put("cluster", cluster);
        info.put("storage", storage);
        info.put("physical_tag", physicalTag.toMap());
        info.put("logical_tag", logicalTag.toMap());
        if (customTags != null) {
            info.put("custom_tags", customTags);
        }
        metric.put("info", info);
        // metric.put("location", location);
        //metrics
        Map<String, Object> metrics = new HashMap<>();
        Map<String, Object> dataMonitor = new HashMap<>();
        dataMonitor.put("data_loss", dataLoss.toMap());
        if (dataDelay != null) {
            dataMonitor.put("data_delay", dataDelay.toMap());
        }
        metrics.put("data_monitor", dataMonitor);

        if (dataStructure != null) {
            metrics.put("data_profiling", new HashMap<String, Object>() {
                {
                    put("data_structure", dataStructure.toMap());
                }
            });
        }
        // V2.0 目前先不用打
        if (resourceMonitor != null) {
            metrics.put("resource_monitor", resourceMonitor);
        }
        if (customMetrics != null) {
            metrics.put("custom_metrics", customMetrics);
        }
        metric.put("metrics", metrics);
        return metric;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setPhysicalTag(MetricTag physicalTag) {
        this.physicalTag = physicalTag;
    }

    public void setLogicalTag(MetricTag logicalTag) {
        this.logicalTag = logicalTag;
    }

    public Map<String, Object> getCustomTags() {
        return customTags;
    }

    public void setCustomTags(Map<String, Object> customTags) {
        this.customTags = customTags;
    }

    public void setUpstream(List<MetricStream> upstream) {
        this.upstream = upstream;
    }

    public List<MetricStream> getDownStream() {
        return downStream;
    }

    public void setDownStream(List<MetricStream> downStream) {
        this.downStream = downStream;
    }

    public MetricDataLoss getDataLoss() {
        return dataLoss;
    }

    public void setDataLoss(MetricDataLoss dataLoss) {
        this.dataLoss = dataLoss;
    }

    public MetricDataDelay getDataDelay() {
        return dataDelay;
    }

    public void setDataDelay(MetricDataDelay dataDelay) {
        this.dataDelay = dataDelay;
    }

    public MetricDataStructure getDataStructure() {
        return dataStructure;
    }

    public void setDataStructure(MetricDataStructure dataStructure) {
        this.dataStructure = dataStructure;
    }

    public void setResourceMonitor(Map<String, Object> resourceMonitor) {
        this.resourceMonitor = resourceMonitor;
    }

    public Map<String, Object> getCustomMetrics() {
        return customMetrics;
    }

    public void setCustomMetrics(Map<String, Object> customMetrics) {
        this.customMetrics = customMetrics;
    }

    public void setStorage(Map<String, Object> storage) {
        this.storage = storage;
    }
}
