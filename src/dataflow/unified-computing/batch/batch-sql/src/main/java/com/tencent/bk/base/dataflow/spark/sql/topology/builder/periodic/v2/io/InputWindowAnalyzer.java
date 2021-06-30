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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2.io;

import com.tencent.bk.base.dataflow.spark.BatchTimeDelta;
import com.tencent.bk.base.dataflow.spark.BatchTimeStamp;
import com.tencent.bk.base.dataflow.spark.exception.BatchAccumulateNotInRangeException;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class InputWindowAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(InputWindowAnalyzer.class);

    private BatchTimeStamp scheduleTimeInHour;
    private String windowType;
    private BatchTimeDelta windowSize;
    private BatchTimeDelta windowOffset;
    private BatchTimeDelta windowStartOffset;
    private BatchTimeDelta windowEndOffset;
    private BatchTimeStamp accumulateStartTime;

    private BatchTimeStamp currentAccumulateStartTime;

    private boolean isAccumulateNotInRange;

    private BatchTimeStamp startTime;
    private BatchTimeStamp endTime;

    /**
     * 创建输入window解析器
     * @param info 数据输入参数
     */
    public InputWindowAnalyzer(Map<String, Object> info) {
        this.isAccumulateNotInRange = false;
        this.windowType = info.get("window_type").toString().toLowerCase();
        if (!(this.isAccumulate() || this.isSlide() || this.isScroll())) {
            throw new UnsupportedOperationException(
                    String.format("Found unsupported window type(%s)", this.windowType));
        }
        long scheduleTime = CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)info.get("schedule_time"));
        this.scheduleTimeInHour = new BatchTimeStamp(scheduleTime);

        this.windowSize = new BatchTimeDelta(info.get("window_size").toString());
        this.windowOffset = new BatchTimeDelta(info.get("window_offset").toString());

        if (this.isAccumulate()) {
            this.windowStartOffset = new BatchTimeDelta(info.get("window_start_offset").toString());
            this.windowEndOffset = new BatchTimeDelta(info.get("window_end_offset").toString());
            this.accumulateStartTime = new BatchTimeStamp(((Double)info.get("accumulate_start_time")).longValue());
            this.calculateCurrentAccumulateStartTime();
        }

        this.calculateStartTime();
        this.calculateEndTime();
    }

    private void calculateStartTime() {
        if (this.isSlide() || this.isScroll()) {
            this.startTime = this.scheduleTimeInHour.minus(this.windowOffset).minus(this.windowSize);
        } else {
            if (this.windowStartOffset != null) {
                this.startTime = this.currentAccumulateStartTime.plus(this.windowStartOffset);
                if (this.scheduleTimeInHour.minus(this.windowOffset).getTimeInMilliseconds()
                        <= startTime.getTimeInMilliseconds()) {
                    this.isAccumulateNotInRange = true;
                }
            } else {
                throw new RuntimeException("windowStartOffset can't be null when using accumulate window");
            }
        }
        LOGGER.info(String.format("Get current window start time %d", this.startTime.getTimeInMilliseconds()));
    }

    private void calculateEndTime() {
        if (this.isSlide() || this.isScroll()) {
            this.endTime = this.scheduleTimeInHour.minus(this.windowOffset);
        } else {
            if (this.windowEndOffset != null) {
                BatchTimeStamp endOffsetTime = this.currentAccumulateStartTime.plus(this.windowEndOffset);
                this.endTime = this.scheduleTimeInHour.minus(this.windowOffset);
                if (endTime.getTimeInMilliseconds() > endOffsetTime.getTimeInMilliseconds()) {
                    this.isAccumulateNotInRange = true;
                }
            } else {
                throw new RuntimeException("windowEndOffset can't be null when using accumulate window");
            }
        }
        LOGGER.info(String.format("Get current window end time %d", this.endTime.getTimeInMilliseconds()));
    }

    public long getStartTimeInMilliseconds() throws BatchAccumulateNotInRangeException {
        return this.startTime.getTimeInMilliseconds();
    }

    public long getEndTimeInMilliseconds() throws BatchAccumulateNotInRangeException {
        return this.endTime.getTimeInMilliseconds();
    }

    public boolean isAccumulateNotInRange() {
        return this.isAccumulateNotInRange;
    }

    private void calculateCurrentAccumulateStartTime() {
        if (this.isAccumulate()) {
            BatchTimeStamp limitTimeStamp = this.scheduleTimeInHour.minus(this.windowOffset);
            if (this.accumulateStartTime.getTimeInMilliseconds() >= limitTimeStamp.getTimeInMilliseconds()) {
                BatchTimeStamp curBatchTimeStamp = this.accumulateStartTime;

                while (curBatchTimeStamp.getTimeInMilliseconds() >= limitTimeStamp.getTimeInMilliseconds()) {
                    curBatchTimeStamp = curBatchTimeStamp.minus(this.windowSize);
                }
                this.currentAccumulateStartTime = curBatchTimeStamp;
            } else {
                BatchTimeStamp nextBatchTimeStamp = this.accumulateStartTime.plus(this.windowSize);
                BatchTimeStamp curBatchTimeStamp = this.accumulateStartTime;

                while (nextBatchTimeStamp.getTimeInMilliseconds() < limitTimeStamp.getTimeInMilliseconds()) {
                    curBatchTimeStamp = nextBatchTimeStamp;
                    nextBatchTimeStamp = nextBatchTimeStamp.plus(this.windowSize);
                }
                this.currentAccumulateStartTime = curBatchTimeStamp;
            }
            LOGGER.info(String.format("Get current accumulate start time %d",
                    this.currentAccumulateStartTime.getTimeInMilliseconds()));
        }
    }

    private boolean isAccumulate() {
        return "accumulate".equals(this.windowType);
    }

    private boolean isScroll() {
        return "scroll".equals(this.windowType);
    }

    private boolean isSlide() {
        return "slide".equals(this.windowType);
    }
}
