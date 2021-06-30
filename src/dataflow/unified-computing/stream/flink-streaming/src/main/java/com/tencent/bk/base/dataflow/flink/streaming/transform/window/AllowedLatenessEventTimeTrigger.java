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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.runtime.types.CRow;

/**
 * 允许延迟数据进入窗口计算的trigger
 */
public class AllowedLatenessEventTimeTrigger extends Trigger<CRow, Window> {

    private static final long serialVersionUID = 1L;

    // 基于processingTime的统计频率
    private final long processingTimeInterval;

    // 允许进入窗口计算的延迟时长
    private final long allowedLatenessEventTime;

    /**
     * When merging we take the lowest of all fire timestamps as the new fire timestamp.
     */
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time-allowed-lateness", new Min(), LongSerializer.INSTANCE);

    private AllowedLatenessEventTimeTrigger(long processingTimeInterval, long allowedLatenessEventTime) {
        this.processingTimeInterval = processingTimeInterval;
        this.allowedLatenessEventTime = allowedLatenessEventTime;
    }

    @Override
    public TriggerResult onElement(CRow element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // 如果watermark已经越过了窗口，则注册一个指定时间基于ProcessingTime定时器，到时间后fire窗口
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            if (fireTimestamp.get() == null) {
                timestamp = ctx.getCurrentProcessingTime();
                long nextFireTimestamp = timestamp + this.processingTimeInterval;
                ctx.registerProcessingTimeTimer(nextFireTimestamp);
                fireTimestamp.add(nextFireTimestamp);
            }
            return TriggerResult.CONTINUE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        // fire 同时清理窗口state，使得延迟时间是增量的计算。
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else if (time == window.maxTimestamp() + allowedLatenessEventTime) {
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            if (fireTimestamp.get() != null) {
                ctx.deleteProcessingTimeTimer(fireTimestamp.get());
                fireTimestamp.clear();
            }
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        if (fireTimestamp.get() != null) {
            long timestamp = fireTimestamp.get();
            ctx.deleteProcessingTimeTimer(timestamp);
            fireTimestamp.clear();
        }
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(Window window, OnMergeContext ctx) throws Exception {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "AllowedLatenessEventTimeTrigger()";
    }

    /**
     * 根据参数创建允许延迟数据进入窗口计算的trigger
     *
     * @param processingTimeInterval 延迟数据推出窗口计算结果的间隔，以秒为单位
     * @param allowedLatenessEventTime 允许延迟时间内的数据进入窗口计算，以小时为单位
     * @return AllowedLatenessEventTimeTrigger
     */
    public static AllowedLatenessEventTimeTrigger of(int processingTimeInterval, int allowedLatenessEventTime) {
        return new AllowedLatenessEventTimeTrigger(
                Time.seconds(processingTimeInterval).toMilliseconds(),
                Time.hours(allowedLatenessEventTime).toMilliseconds()
        );
    }

    private static class Min implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
