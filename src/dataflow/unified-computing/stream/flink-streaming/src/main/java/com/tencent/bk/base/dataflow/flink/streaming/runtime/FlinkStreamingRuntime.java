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

package com.tencent.bk.base.dataflow.flink.streaming.runtime;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.DummyCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.RedisCheckpointManager;
import com.tencent.bk.base.dataflow.runtime.AbstractFlinkRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingCheckpointConfig;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;

public class FlinkStreamingRuntime extends AbstractFlinkRuntime<StreamTableEnvironment, FlinkStreamingTopology,
        AbstractFlinkStreamingCheckpointManager> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStreamingRuntime.class);

    public FlinkStreamingRuntime(Topology topology) {
        super(topology);
    }

    @Override
    public void execute() {
        String jobName = this.getTopology().getJobName();

        try {
            this.getEnv().execute(jobName);
            LOGGER.info("--------------------------------SUBMIT FLINK JOB {}--------------------------------",
                    jobName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 加载 flink 的 env
     *
     * @return flink env
     */
    @Override
    public StreamExecutionEnvironment asEnv() {
        StreamExecutionEnvironment env;
        FlinkStreamingCheckpointConfig checkPointConfig = this.getTopology().getCheckpointConfig();
        String runMode = this.getTopology().getRunMode();
        Boolean isDisableChain = this.getTopology().isDisableChain();
        switch (ConstantVar.RunMode.valueOf(runMode)) {
            case udf_debug:
                env = StreamExecutionEnvironment.getExecutionEnvironment();
                break;
            default:
                env = StreamExecutionEnvironment.getExecutionEnvironment();

                if (runMode.equalsIgnoreCase(ConstantVar.RunMode.product.toString())) {
                    if (checkPointConfig.getStateBackend() != null
                            && checkPointConfig.getStateCheckpointsDir() != null) {
                        StateBackend backend = null;
                        String jobId = this.getTopology().getJobId();
                        if (((FlinkStreamingTopology) getTopology()).isFileSystemState()) {
                            backend = new FsStateBackend(checkPointConfig.getStateCheckpointsDir());
                            LOGGER.info(String.format("The job %s use filesystem state backend", jobId));
                        } else {
                            backend = makeRocksdbBackend(checkPointConfig);
                            LOGGER.info(String.format("The job %s use rocksdb state backend", jobId));
                        }
                        env.setStateBackend(backend);
                    }
                    env.enableCheckpointing(checkPointConfig.getCheckpointInterval());
                    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
                    env.getCheckpointConfig().setCheckpointTimeout(500000);
                    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
                    env.getCheckpointConfig().enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                    env.getConfig().enableObjectReuse();
                }

                if (runMode.equalsIgnoreCase(ConstantVar.RunMode.debug.toString())) {
                    env.setRestartStrategy(RestartStrategies.noRestart());
                }

                if (isDisableChain) {
                    env.disableOperatorChaining();
                }
        }
        // event time 窗口驱动
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    /**
     * 加载flink的table env
     *
     * @return flink table env
     */
    @Override
    public StreamTableEnvironment asTableEnv() {
        return TableEnvironment.getTableEnvironment(this.getEnv());
    }

    /**
     * 获取flink的checkpoint类型
     *
     * @return checkpoint class
     */
    @Override
    public AbstractFlinkStreamingCheckpointManager createCheckpointManager() {
        FlinkStreamingCheckpointConfig checkpointConfig = this.getTopology().getCheckpointConfig();
        LOGGER.info("checkpointManager: {}", checkpointConfig.getManager());
        switch (checkpointConfig.getManager()) {
            case "redis":
                //return new RedisCheckpointManager("127.0.0.1", 6379, "", 50000);
                AbstractFlinkStreamingCheckpointManager checkpointManager = null;
                String sentinelHost = checkpointConfig.getCheckpointRedisSentinelHost();
                Integer sentinelPort = checkpointConfig.getCheckpointRedisSentinelPort();
                String masterName = checkpointConfig.getCheckpointRedisSentinelName();
                if (StringUtils.isNotBlank(sentinelHost) && null != sentinelPort && StringUtils
                        .isNotBlank(masterName)) {
                    if (!StringUtils.startsWith(sentinelHost, "__")
                            && !StringUtils.endsWith(sentinelHost, "__")
                            && !StringUtils.startsWith(masterName, "__")
                            && !StringUtils.endsWith(masterName, "__")) {
                        // 如果是sentinel 则返回sentinel模式
                        Set<String> sentinels = new HashSet<String>();
                        sentinels.add(new HostAndPort(sentinelHost, sentinelPort).toString());
                        checkpointManager = new RedisCheckpointManager(
                                checkpointConfig.getCheckpointRedisHost(),
                                checkpointConfig.getCheckpointRedisPort(),
                                checkpointConfig.getCheckpointRedisPassword(),
                                5000,
                                masterName,
                                sentinels);
                    }
                }
                if (checkpointManager == null) {
                    // redis单例模式
                    checkpointManager = new RedisCheckpointManager(
                            checkpointConfig.getCheckpointRedisHost(),
                            checkpointConfig.getCheckpointRedisPort(),
                            checkpointConfig.getCheckpointRedisPassword(),
                            5000);
                }
                checkpointManager.setStartPosition(checkpointConfig.getStartPosition());
                return checkpointManager;
            case "dummy":
                return new DummyCheckpointManager();
            default:
                throw new RuntimeException();
        }
    }

    private StateBackend makeRocksdbBackend(FlinkStreamingCheckpointConfig checkPointConfig) {
        RocksDBStateBackend rocksdbBackend = null;
        try {
            rocksdbBackend = new RocksDBStateBackend(checkPointConfig.getStateCheckpointsDir());
        } catch (IOException e) {
            LOGGER.error("prepare rocksdb error", e);
            throw new RuntimeException(e);
        }
        rocksdbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        return (StateBackend) rocksdbBackend;
    }
}
