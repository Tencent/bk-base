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

package com.tencent.bk.base.dataflow.flink.runtime;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology;
import com.tencent.bk.base.dataflow.flink.checkpoint.FlinkCodeCheckpointManager;
import com.tencent.bk.base.dataflow.runtime.AbstractFlinkRuntime;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkCodeRuntime extends
        AbstractFlinkRuntime<StreamTableEnvironment, FlinkCodeTopology, FlinkCodeCheckpointManager> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkCodeRuntime.class);

    public FlinkCodeRuntime(Topology topology) {
        super(topology);
    }

    @Override
    public FlinkCodeCheckpointManager createCheckpointManager() {
        FlinkCodeCheckpointManager checkpointManager = new FlinkCodeCheckpointManager();
        checkpointManager.setStartPosition(this.getTopology().getCheckpointConfig().getStartPosition());
        checkpointManager.setPath(this.getTopology().getCheckpointConfig().getPath());
        return checkpointManager;
    }

    @Override
    public StreamExecutionEnvironment asEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (this.getTopology().getRunMode().equalsIgnoreCase(ConstantVar.RunMode.product.toString())) {
            env.enableCheckpointing(600000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            env.getCheckpointConfig().setCheckpointTimeout(500000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        }

        if (this.getTopology().getRunMode().equalsIgnoreCase(ConstantVar.RunMode.debug.toString())) {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
        // event time 窗口驱动
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    @Override
    public StreamTableEnvironment asTableEnv() {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        return StreamTableEnvironment.create(this.getEnv(), fsSettings);
    }

    @Override
    public void execute() {
        try {
            String jobName = this.getTopology().getJobName();
            this.getEnv().execute(jobName);
            LOGGER.info("--------------------------------SUBMIT FLINK SDK JOB {}--------------------------------",
                    jobName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
