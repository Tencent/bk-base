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

package com.tencent.bk.base.dataflow.flink.streaming.sink;

import com.tencent.bk.base.dataflow.core.topo.JobRegister;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.CheckpointValue;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.CheckpointValue.OutputCheckpoint;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.AbstractCheckpointKey;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.OffsetCheckpointKey;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.TimestampCheckpointKey;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.metric.MetricManger;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistedStateGuardMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersistedStateGuardMap.class);

    /**
     * utc 时间格式
     */
    private final SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("UTC"));
        }
    };

    private AbstractFlinkStreamingCheckpointManager checkpointManager;
    private CheckpointValue checkpointValue = new CheckpointValue();
    private SinkNode node;
    private boolean isOffset;
    private long lastDiscardPrintTime = 0;
    private FlinkStreamingTopology topology;
    private String nodeAndTaskId;
    // MetricManger变更为非静态类
    private MetricManger metricManger;
    private Map<AbstractCheckpointKey, OutputCheckpoint> windowSinkingCheckpoint;
    private Map<AbstractCheckpointKey, Boolean> checkpointKeyPersistFlag = new HashMap<>();

    public PersistedStateGuardMap(
            FlinkStreamingTopology topology,
            AbstractFlinkStreamingCheckpointManager checkpointManager,
            SinkNode node) {
        this.checkpointManager = checkpointManager;
        this.node = node;
        this.topology = topology;
        this.isOffset = NodeUtils.haveOffsetField(node.getFields());
        JobRegister.register(topology.getJobId());
    }

    /**
     * 初始化打点信息
     *
     * @param runtimeContext context
     */
    public void initMetrix(RuntimeContext runtimeContext) {
        int taskId = runtimeContext.getIndexOfThisSubtask();
        this.nodeAndTaskId = MetricManger.getNodeMetricId(node, taskId);
        this.metricManger = new MetricManger();
        this.metricManger.registerMetric(taskId, node, topology, node.getNodeId());
    }

    /**
     * 在map中判断数据是否符合存储的条件
     *
     * @param input 输入记录
     * @return 返回记录
     * @throws Exception 异常
     */
    public Row map(Row input) throws Exception {
        // 构造checkpoint分别是offset和时间的checkpoint key
        AbstractCheckpointKey checkpointKey;
        if (isOffset) {
            String parOffset = input.getField(1).toString();
            // [avroIndex]_[partition]_[offset]_[topic]
            String[] parAndOffset = parOffset.split("_", 4);
            checkpointKey = new OffsetCheckpointKey(parAndOffset[3], node.getNodeId(), parAndOffset[1]);
        } else {
            // 窗口transform的checkpoint是时间，不需要识别topic
            checkpointKey = new TimestampCheckpointKey(node.getNodeId());
        }

        // 初始化存储flag为false
        checkpointKeyPersistFlag.putIfAbsent(checkpointKey, false);

        if (!checkpointKeyPersistFlag.get(checkpointKey)) {
            checkpointValue.setUpdatedCheckpoints(checkpointKey, getCheckpointKey(checkpointKey));
        }

        OutputCheckpoint outputCheckpoint = getOutputCheckpoint(input);
        // 针对单个node设置redis checkpoint开关
        boolean shouldPersist = checkpointKeyPersistFlag.get(checkpointKey) || !(
                checkpointValue.getUpdatedCheckpoint(checkpointKey).getPointValue() > 0
                        && outputCheckpoint.compareTo(checkpointValue.getUpdatedCheckpoint(checkpointKey)) <= 0);

        if (shouldPersist) {
            // at least once
            OutputCheckpoint maxPoint = Collections
                    .max(Arrays.asList(checkpointValue.getUpdatedCheckpoint(checkpointKey), outputCheckpoint));
            // 这里减1（创建新的对象）
            checkpointValue.setUpdatedCheckpoints(checkpointKey, maxPoint.subtraction(1));
            checkpointKeyPersistFlag.put(checkpointKey, true);
            return input;
        } else {
            // 打印checkpoint丢弃数据
            if (System.currentTimeMillis() - lastDiscardPrintTime >= 10000) {
                LOGGER.warn("discard old output from checkpoint key {}, and the record is {}, checkpoint value is {}",
                        checkpointKey.toString(), input.toString(),
                        checkpointValue.getUpdatedCheckpoint(checkpointKey).toDbValue());
                lastDiscardPrintTime = System.currentTimeMillis();
            }
            this.metricManger.getMetricObject(nodeAndTaskId).recordDrop("checkpoint_drop_cnt", input);
            return null;
        }
    }

    /**
     * 保存checkpoint
     */
    public synchronized void saveCheckPoint() {
        for (Map.Entry<AbstractCheckpointKey, CheckpointValue.OutputCheckpoint> entry : checkpointValue.iterable()) {
            checkpointManager.savePoint(entry.getKey(), entry.getValue());
            if (!isOffset) {
                if (null == windowSinkingCheckpoint) {
                    // 有并发
                    windowSinkingCheckpoint = new ConcurrentHashMap<>();
                }
                windowSinkingCheckpoint.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 窗口结束是更新checkpoint
     */
    public synchronized void saveCheckPointForWindowEnd() {
        if (!isOffset && null != windowSinkingCheckpoint) {
            for (Map.Entry<AbstractCheckpointKey, CheckpointValue.OutputCheckpoint> entry : windowSinkingCheckpoint
                    .entrySet()) {
                checkpointManager.savePoint(entry.getKey(), entry.getValue().subtraction(-1));
            }
            windowSinkingCheckpoint.clear();
        }
    }

    private OutputCheckpoint getCheckpointKey(AbstractCheckpointKey checkpointKey) {
        OutputCheckpoint checkpoint = checkpointValue.getUpdatedCheckpoint(checkpointKey);
        if (null == checkpoint) {
            checkpoint = checkpointManager.getPoint(checkpointKey);
            if (null == checkpoint) {
                return new OutputCheckpoint();
            } else {
                LOGGER.info(String.format("load persisted checkpoint %s: %d", checkpointKey.toString(),
                        checkpoint.getPointValue()));
                return checkpoint;
            }
        } else {
            return checkpoint;
        }
    }

    private OutputCheckpoint getOutputCheckpoint(Row input) {
        long pointValue = -1;
        OutputCheckpoint outputCheckpoint = new OutputCheckpoint();
        if (isOffset) {
            String parOffset = input.getField(1).toString();
            // [avroIndex]_[partition]_[offset]_[topic]
            String[] parAndOffset = parOffset.split("_", 4);
            int index = Integer.valueOf(parAndOffset[0]);
            pointValue = Long.valueOf(parAndOffset[2]);
            outputCheckpoint.setPointValue(pointValue);
            // 增加avroindex
            outputCheckpoint.setIndex(index);
        } else {
            try {
                // 13位时间戳转化为10位
                pointValue = utcFormat.parse(input.getField(0).toString()).getTime() / 1000;
                outputCheckpoint.setPointValue(pointValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return outputCheckpoint;
    }

    /**
     * destroy redis pool
     */
    public void destroyRedisPool() {
        if (checkpointManager != null) {
            this.checkpointManager.close();
        }
    }
}
