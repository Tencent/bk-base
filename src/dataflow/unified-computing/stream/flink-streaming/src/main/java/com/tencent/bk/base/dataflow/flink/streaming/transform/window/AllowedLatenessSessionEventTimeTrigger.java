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

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.runtime.types.CRow;

public class AllowedLatenessSessionEventTimeTrigger extends Trigger<CRow, Window> {

    private static final long serialVersionUID = 1L;

    private AllowedLatenessSessionEventTimeTrigger() {
    }

    @Override
    public TriggerResult onElement(CRow element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(Window window,
            OnMergeContext ctx) {
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
        return "AllowedLatenessSessionEventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static AllowedLatenessSessionEventTimeTrigger create() {
        return new AllowedLatenessSessionEventTimeTrigger();
    }
}
