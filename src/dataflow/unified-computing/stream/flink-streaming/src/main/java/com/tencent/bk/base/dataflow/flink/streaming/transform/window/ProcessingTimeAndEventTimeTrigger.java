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

package com.tencent.bk.base.dataflow.flink.streaming.transform.window;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.types.CRow;

public class ProcessingTimeAndEventTimeTrigger<W extends TimeWindow> extends Trigger<CRow, W> {

    private static final long serialVersionUID = 1L;

    // 基于processingTime的等待时间(如果达到窗口长度 + 等待时间了，watermark没有触发将有由系统触发)
    private final long processingTimeInterval;

    /**
     * When merging we take the lowest of all fire timestamps as the new fire timestamp.
     */
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private final ReducingStateDescriptor<Long> windowLengthStateDesc =
            new ReducingStateDescriptor<>("bk-window-length", new ProcessingTimeAndEventTimeTrigger.Max(),
                    LongSerializer.INSTANCE);

    private ProcessingTimeAndEventTimeTrigger(long processingTimeInterval) {
        this.processingTimeInterval = processingTimeInterval;
    }

    @Override
    public TriggerResult onElement(CRow element, long timestamp, W window, Trigger.TriggerContext ctx)
            throws Exception {

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        // 需要继续注册ProcessingTimer
        // 【重要】使用状态保证一个窗口只注册一个值ProcessingTimeTimer
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        ReducingState<Long> windowLengthState = ctx.getPartitionedState(windowLengthStateDesc);
        long processingTime = ctx.getCurrentProcessingTime();
        Long oldFireTimestamp = fireTimestamp.get();
        Long oldWindowLength = windowLengthState.get();

        if (oldFireTimestamp == null) {
            // 窗口被系统定时触发时间 = 当前系统时间 + 窗口长度 + 等待时间
            long nextFireTimestamp = processingTime + processingTimeInterval;
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        } else {
            // 因为session窗口会改变窗口的namespace所以需要重新注册。
            ctx.registerProcessingTimeTimer(oldFireTimestamp);
        }

        long windowLength = window.getEnd() - window.getStart();
        if (oldWindowLength == null) {
            windowLengthState.add(windowLength);
        } else {
            if (null != oldFireTimestamp) {
                // 针对session窗口场景，会改变窗口长度。
                long windowSizeIncrease = windowLength - oldWindowLength;
                if (windowSizeIncrease >= 1000) {
                    // 如果窗口长度发生了变化，并且超过1s，
                    windowLengthState.add(windowLength);
                    // 重新所以需要删除
                    ctx.deleteProcessingTimeTimer(oldFireTimestamp);
                    // 需要重置fire-time 状态。
                    fireTimestamp.clear();
                    // 重新注册定时器。
                    long nextFireTimestamp = processingTime + processingTimeInterval;
                    ctx.registerProcessingTimeTimer(nextFireTimestamp);
                    fireTimestamp.add(nextFireTimestamp);
                }
            }
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, Trigger.TriggerContext ctx) throws Exception {

        if (time == window.maxTimestamp()) {
            //
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            Long oldFireTimestamp = fireTimestamp.get();
            if (oldFireTimestamp != null) {
                ctx.deleteProcessingTimeTimer(oldFireTimestamp);
                fireTimestamp.clear();
            }
            ReducingState<Long> windowLengthState = ctx.getPartitionedState(windowLengthStateDesc);
            if (windowLengthState.get() != null) {
                windowLengthState.clear();
            }
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, Trigger.TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long oldFireTimestamp = fireTimestamp.get();
        if (oldFireTimestamp != null && oldFireTimestamp == time) {
            ctx.deleteProcessingTimeTimer(time);
            fireTimestamp.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, Trigger.TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long oldFireTimestamp = fireTimestamp.get();
        if (oldFireTimestamp != null) {
            long timestamp = oldFireTimestamp;
            ctx.deleteProcessingTimeTimer(timestamp);
            fireTimestamp.clear();
        }
        ReducingState<Long> windowLengthState = ctx.getPartitionedState(windowLengthStateDesc);
        if (windowLengthState.get() != null) {
            windowLengthState.clear();
        }

        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window,
            Trigger.OnMergeContext ctx) throws Exception {

        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
        ctx.mergePartitionedState(stateDesc);
        ctx.mergePartitionedState(windowLengthStateDesc);
    }

    @VisibleForTesting
    public long getInterval() {
        return processingTimeInterval;
    }

    @Override
    public String toString() {
        return "ProcessingTimeAndEventTimeTrigger(" + processingTimeInterval + ")";
    }

    /**
     * Creates a trigger that continuously fires based on the given interval.
     *
     * @param processingTimeInterval The time interval at which to fire.
     * @param <W> The type of {@link TimeWindow Windows} on which this trigger can operate.
     */
    public static <W extends TimeWindow> ProcessingTimeAndEventTimeTrigger<W> of(Time processingTimeInterval) {
        return new ProcessingTimeAndEventTimeTrigger<>(processingTimeInterval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private static class Max implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.max(value1, value2);
        }
    }
}
