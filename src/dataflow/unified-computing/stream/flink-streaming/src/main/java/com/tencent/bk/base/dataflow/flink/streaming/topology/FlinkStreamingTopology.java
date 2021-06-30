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

package com.tencent.bk.base.dataflow.flink.streaming.topology;

import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class FlinkStreamingTopology extends StreamTopology<FlinkStreamingCheckpointConfig> {

    private static final String FILE_SYSTEM_STATE_BACKEND = "filesystem";

    private String geogAreaCode;
    private String timeZone;

    public FlinkStreamingTopology(FlinkStreamingBuilder builder) {
        super(builder);
        this.timeZone = builder.getTimeZone();
        this.geogAreaCode = builder.geogAreaCode;
        this.setCheckpointConfig(builder.checkpointConfig);

        this.mapNodes(builder.typeTables);
        this.mapUdfs(builder.udf);
    }

    public boolean isFileSystemState() {
        String stateBackend = this.getCheckpointConfig().getStateBackend();
        return null != stateBackend && stateBackend.equalsIgnoreCase(FILE_SYSTEM_STATE_BACKEND);
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getGeogAreaCode() {
        return geogAreaCode;
    }

    public static class FlinkStreamingBuilder extends AbstractStreamBuilder {

        private String geogAreaCode = null;
        private FlinkStreamingCheckpointConfig checkpointConfig = new FlinkStreamingCheckpointConfig();

        private Map<String, Map<String, Object>> typeTables;
        private Map<String, Object> udf;

        /**
         * 初始化 FlinkStreamingBuilder
         *
         * @param parameters 构造 FlinkStreaming需要的参数
         */
        public FlinkStreamingBuilder(Map<String, Object> parameters) {
            super(parameters);

            this.geogAreaCode = (String) parameters.getOrDefault("geog_area_code", null);

            // checkpoint 信息
            Map<String, Object> checkpointInfo = (Map<String, Object>) parameters.get("checkpoint");
            if (checkpointInfo.get("checkpoint_interval") != null
                    && StringUtils.isNotBlank(checkpointInfo.get("checkpoint_interval").toString())) {
                this.checkpointConfig.setCheckpointInterval((Integer) checkpointInfo.get("checkpoint_interval"));
            }

            this.checkpointConfig.setStateBackend(
                    (String) checkpointInfo.getOrDefault("state_backend", null));
            this.checkpointConfig.setStateCheckpointsDir(
                    (String) checkpointInfo.getOrDefault("state_checkpoints_dir", null));

            this.checkpointConfig.setCheckpointRedisHost(
                    (String) checkpointInfo.getOrDefault("checkpoint_redis_host", null));
            this.checkpointConfig.setCheckpointRedisPort(
                    (Integer) checkpointInfo.getOrDefault("checkpoint_redis_port", 0));
            this.checkpointConfig.setCheckpointRedisPassword(
                    (String) checkpointInfo.getOrDefault("checkpoint_redis_password", null));
            this.checkpointConfig.setCheckpointRedisSentinelHost(
                    (String) checkpointInfo.getOrDefault("checkpoint_redis_sentinel_host", null));
            this.checkpointConfig.setCheckpointRedisSentinelName(
                    (String) checkpointInfo.getOrDefault("checkpoint_redis_sentinel_name", null));
            this.checkpointConfig.setCheckpointRedisSentinelPort(
                    (Integer) checkpointInfo.getOrDefault("checkpoint_redis_sentinel_port", 0));

            this.checkpointConfig.setManager(checkpointInfo.get("manager").toString());
            this.checkpointConfig.setStartPosition(checkpointInfo.get("start_position").toString());

            // nodes
            this.typeTables = (Map<String, Map<String, Object>>) parameters.get("nodes");
            // udf
            this.udf = (Map<String, Object>) parameters.get("udf");
        }

        public FlinkStreamingTopology build() {
            return new FlinkStreamingTopology(this);
        }

    }
}
