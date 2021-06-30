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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessTimeAndCountTrigger<W extends Window> extends Trigger<Object, W> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessTimeAndCountTrigger.class);

    private final long interval;
    private final long maxCount;

    /**
     * When merging we take the lowest of all fire timestamps as the new fire timestamp.
     */
    private final ReducingStateDescriptor<Long> processTimeStateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    private ProcessTimeAndCountTrigger(Time processTimeInterval, long countInteval) {
        this.interval = processTimeInterval.toMilliseconds();
        this.maxCount = countInteval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // process time
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimeStateDesc);
        timestamp = ctx.getCurrentProcessingTime();
        // process time
        if (fireTimestamp.get() == null || fireTimestamp.get() < timestamp) {
            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }

        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        count.add(1L);

        if (count.get() >= maxCount) {
            ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            fireTimestamp.clear();
            fireTimestamp.add(timestamp + interval);
            ctx.registerProcessingTimeTimer(timestamp + interval);
            count.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimeStateDesc);
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);

        count.clear();
        ctx.deleteProcessingTimeTimer(fireTimestamp.get());
        fireTimestamp.clear();
        fireTimestamp.add(time + interval);
        ctx.registerProcessingTimeTimer(time + interval);

        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(processTimeStateDesc);
        long timestamp = fireTimestamp.get();
        ctx.deleteProcessingTimeTimer(timestamp);
        fireTimestamp.clear();
        ctx.getPartitionedState(countStateDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window,
            OnMergeContext ctx) {
        ctx.mergePartitionedState(processTimeStateDesc);
        ctx.mergePartitionedState(countStateDesc);
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    @Override
    public String toString() {
        return "ContinuousProcessingTimeTrigger(" + interval + ")";
    }

    /**
     * Creates a trigger that continuously fires based on the given interval.
     *
     * @param timeInterval The time interval at which to fire.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <W extends Window> ProcessTimeAndCountTrigger<W> of(Time timeInterval, long countInterval) {
        return new ProcessTimeAndCountTrigger<>(timeInterval, countInterval);
    }

    private static class Min implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private static class Sum implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
