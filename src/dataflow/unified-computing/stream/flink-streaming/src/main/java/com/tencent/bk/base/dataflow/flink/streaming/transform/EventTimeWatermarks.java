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

package com.tencent.bk.base.dataflow.flink.streaming.transform;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.EVENT_TIME_WATERMARK_BATCH_PROCESS_INTERVAL_MS;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.EVENT_TIME_WATERMARK_BATCH_PROCESS_NUM;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.EVENT_TIME_WATERMARK_FUTURE_TIME_TOLERANCE_MS;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

public final class EventTimeWatermarks implements AssignerWithPeriodicWatermarks<Row> {

    /**
     * utc 时间格式
     */
    private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("UTC"));
        }
    };

    private Long maxOutOfOrderness;
    private Long currentMaxTimestamp = 0L;
    private Long lastTick = 0L;
    private Long currentTickMinTimestamp = 0L;
    private Long rowCount = 0L;

    public EventTimeWatermarks(Long waitingSecond) {
        this.maxOutOfOrderness = waitingSecond;
    }

    @Override
    public Watermark getCurrentWatermark() {
        Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        return watermark;
    }

    @Override
    public long extractTimestamp(Row element, long l) {
        try {
            rowCount++;
            Date date = utcFormat.parse(element.getField(0).toString());
            Long timestamp = date.getTime();

            long now = System.currentTimeMillis();
            // 增加数据时间最多比本地时间大于60s
            if ((timestamp - now) < EVENT_TIME_WATERMARK_FUTURE_TIME_TOLERANCE_MS) {
                if ((now - lastTick) > EVENT_TIME_WATERMARK_BATCH_PROCESS_INTERVAL_MS) {
                    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                    // 一秒钟重置当前Tick最小时间
                    currentTickMinTimestamp = timestamp;
                    // 重置计数
                    rowCount = 0L;
                    // 重置上一次更新时间
                    lastTick = now;
                } else if (rowCount > EVENT_TIME_WATERMARK_BATCH_PROCESS_NUM) {
                    // 保证Watermark只增，一秒钟或者1000条更新一次Watermark。
                    currentMaxTimestamp = Math.max(currentTickMinTimestamp, currentMaxTimestamp);
                    // 一秒钟重置当前Tick最小时间
                    currentTickMinTimestamp = timestamp;
                    // 重置计数
                    rowCount = 0L;
                    // 重置上一次更新时间
                    lastTick = now;
                } else {
                    currentTickMinTimestamp = Math.min(currentTickMinTimestamp, timestamp);
                }

                return timestamp;
            } else {
                // 未来时间数据
                return 0;
            }
        } catch (ParseException e) {
            throw new RuntimeException("Parse time stamp failed.");
        }
    }
}
