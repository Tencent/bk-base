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

package com.tencent.bk.base.dataflow.flink.topology;

import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.util.List;
import java.util.Map;

public class FlinkCodeTopology extends StreamTopology<FlinkCodeCheckpointConfig> {

    private List<String> userArgs;
    private String userMainClass;
    private String timeZone;

    public FlinkCodeTopology(FlinkSDKBuilder builder) {
        super(builder);
        init(builder);
    }

    private void init(FlinkSDKBuilder builder) {
        // 必传参数
        this.userMainClass = builder.userMainClass;
        this.userArgs = builder.userArgs;
        // checkpoint 相关
        FlinkCodeCheckpointConfig checkpointConfig = new FlinkCodeCheckpointConfig();
        checkpointConfig.setPath(builder.hdfsPath);
        checkpointConfig.setStartPosition(builder.startPosition);
        this.setCheckpointConfig(checkpointConfig);
        // 部署参数
        this.timeZone = builder.getTimeZone();

        // 构建节点
        this.mapNodes(builder.typeTables);
    }

    public static class FlinkSDKBuilder extends AbstractStreamBuilder {

        private List<String> userArgs;
        private String startPosition;
        private String hdfsPath;
        private String userMainClass;

        private Map<String, Map<String, Object>> typeTables;

        public FlinkSDKBuilder(Map<String, Object> parameters) {
            super(parameters);

            this.userArgs = (List<String>) parameters.get("user_args");
            this.userMainClass = parameters.get("user_main_class").toString();
            Map<String, Object> savepoint = (Map<String, Object>) parameters.get("savepoint");
            this.startPosition = savepoint.get("start_position").toString();
            this.hdfsPath = savepoint.get("hdfs_path").toString();
            this.typeTables = (Map<String, Map<String, Object>>) parameters.get("nodes");

        }

        public FlinkCodeTopology build() {
            return new FlinkCodeTopology(this);
        }

    }

    public String getUserMainClass() {
        return this.userMainClass;
    }

    public List<String> getUserArgs() {
        return userArgs;
    }

    public String getTimeZone() {
        return timeZone;
    }
}
