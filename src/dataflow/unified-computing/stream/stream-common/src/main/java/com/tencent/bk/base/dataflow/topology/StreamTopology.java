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

package com.tencent.bk.base.dataflow.topology;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.debug.StreamDebugConfig;
import java.util.Map;
import java.util.Objects;

public class StreamTopology<CheckpointConfigT> extends Topology {

    private String timeZone;
    private boolean isDebug;
    private boolean isDisableChain;
    private StreamMetricConfig metricConfig;
    private StreamDebugConfig debugConfig;
    private String module;
    private String component;

    private CheckpointConfigT checkpointConfig;

    public StreamTopology(AbstractStreamBuilder builder) {
        super(builder);
        this.timeZone = builder.getTimeZone();
        this.metricConfig = builder.metricConfig;
        // 调试参数
        this.isDisableChain = builder.isDisableChain;
        this.isDebug = builder.isDebug;
        this.debugConfig = builder.debugConfig;
        this.module = builder.module;
        this.component = builder.component;
    }

    public CheckpointConfigT getCheckpointConfig() {
        return checkpointConfig;
    }

    public void setCheckpointConfig(CheckpointConfigT checkpointConfig) {
        this.checkpointConfig = checkpointConfig;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public StreamMetricConfig getMetricConfig() {
        return metricConfig;
    }

    public Boolean isDisableChain() {
        return this.isDisableChain;
    }

    public Boolean isDebug() {
        return this.isDebug;
    }

    public StreamDebugConfig getDebugConfig() {
        return debugConfig;
    }

    public String getModule() {
        return module;
    }

    public String getComponent() {
        return component;
    }

    public abstract static class AbstractStreamBuilder extends AbstractBuilder {

        private String timeZone;
        private boolean isDebug;
        private boolean isDisableChain;
        private StreamMetricConfig metricConfig = new StreamMetricConfig();
        private StreamDebugConfig debugConfig;
        private String module = ConstantVar.Role.stream.toString();
        private String component = ConstantVar.Component.flink.toString();

        /**
         * stream 拓扑建造者
         *
         * @param parameters 构建 stream 拓扑需要的参数
         */
        public AbstractStreamBuilder(Map<String, Object> parameters) {
            super(parameters);
            this.timeZone = parameters.get("time_zone").toString();
            // chain优化是否禁用
            this.isDisableChain = Objects.equals(parameters.getOrDefault("chain", "enable"), "disable");
            // debug 信息
            this.isDebug = Objects.equals(ConstantVar.RunMode.debug.toString(), this.getRunMode())
                    || Objects.equals(ConstantVar.RunMode.udf_debug.toString(), this.getRunMode());
            if (this.isDebug) {
                Map<String, String> debugInfo = (Map<String, String>) parameters.get("debug");
                this.debugConfig = new StreamDebugConfig();
                this.debugConfig.setDebugId(debugInfo.get("debug_id").toString());
                this.debugConfig.setDebugErrorDataApi(debugInfo.get("debug_error_data_rest_api_url").toString());
                this.debugConfig.setDebugResultDataApi(debugInfo.get("debug_result_data_rest_api_url").toString());
                this.debugConfig.setDebugNodeMetricApi(debugInfo.get("debug_node_metric_rest_api_url").toString());
            }
            // metric 信息
            Map<String, String> metricInfo = (Map<String, String>) parameters.get("metric");
            this.metricConfig.setMetricKafkaTopic(metricInfo.get("metric_kafka_topic"));
            this.metricConfig.setMetricKafkaServer(metricInfo.get("metric_kafka_server"));
        }

        public String getTimeZone() {
            return this.timeZone;
        }

    }
}
