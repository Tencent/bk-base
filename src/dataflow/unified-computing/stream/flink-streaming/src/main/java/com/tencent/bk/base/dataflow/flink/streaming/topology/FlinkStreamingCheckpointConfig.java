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

import com.tencent.bk.base.dataflow.core.topo.CheckpointConfig;

public class FlinkStreamingCheckpointConfig extends CheckpointConfig {

    private static final long serialVersionUID = 1L;

    /**
     * checkpoint manager is redis or dummy
     */
    private String manager = "dummy";

    // checkpoint 连接信息
    private String checkpointRedisHost = null;
    private Integer checkpointRedisPort = 0;
    private String checkpointRedisPassword = null;
    private String checkpointRedisSentinelHost = null;
    private Integer checkpointRedisSentinelPort = 0;
    private String checkpointRedisSentinelName = null;

    // flink 内置 checkpoint
    private Integer checkpointInterval = 600000;
    private String stateBackend;
    private String stateCheckpointsDir;

    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public String getCheckpointRedisHost() {
        return checkpointRedisHost;
    }

    public void setCheckpointRedisHost(String checkpointRedisHost) {
        this.checkpointRedisHost = checkpointRedisHost;
    }

    public Integer getCheckpointRedisPort() {
        return checkpointRedisPort;
    }

    public void setCheckpointRedisPort(Integer checkpointRedisPort) {
        this.checkpointRedisPort = checkpointRedisPort;
    }

    public String getCheckpointRedisPassword() {
        return checkpointRedisPassword;
    }

    public void setCheckpointRedisPassword(String checkpointRedisPassword) {
        this.checkpointRedisPassword = checkpointRedisPassword;
    }

    public String getCheckpointRedisSentinelHost() {
        return checkpointRedisSentinelHost;
    }

    public void setCheckpointRedisSentinelHost(String checkpointRedisSentinelHost) {
        this.checkpointRedisSentinelHost = checkpointRedisSentinelHost;
    }

    public Integer getCheckpointRedisSentinelPort() {
        return checkpointRedisSentinelPort;
    }

    public void setCheckpointRedisSentinelPort(Integer checkpointRedisSentinelPort) {
        this.checkpointRedisSentinelPort = checkpointRedisSentinelPort;
    }

    public String getCheckpointRedisSentinelName() {
        return checkpointRedisSentinelName;
    }

    public void setCheckpointRedisSentinelName(String checkpointRedisSentinelName) {
        this.checkpointRedisSentinelName = checkpointRedisSentinelName;
    }

    public Integer getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(Integer checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public String getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(String stateBackend) {
        this.stateBackend = stateBackend;
    }

    public String getStateCheckpointsDir() {
        return stateCheckpointsDir;
    }

    public void setStateCheckpointsDir(String stateCheckpointsDir) {
        this.stateCheckpointsDir = stateCheckpointsDir;
    }
}
