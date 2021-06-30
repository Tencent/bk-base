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

package com.tencent.bk.base.dataflow.spark.topology.nodes;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.AbstractInputChannel;
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter$;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchIcebergInput extends AbstractInputChannel<String> implements SupportIcebergCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchIcebergInput.class);
    private String icebergTableName;

    private String filterExpression;
    private int limit = -1;

    private List<TimeRangeTuple> timeRangeList;

    private boolean isOptimizedCountValided;

    public BatchIcebergInput(String icebergTableName, String filterExpression) {
        this.icebergTableName = icebergTableName;
        this.filterExpression = filterExpression;
        this.isOptimizedCountValided = false;
    }

    public BatchIcebergInput(String icebergTableName) {
        this.icebergTableName = icebergTableName;
    }

    public BatchIcebergInput(String icebergTableName, long timeRangeStart, long timeRangeEnd) {
        this.timeRangeList = new LinkedList<>();
        this.icebergTableName = icebergTableName;
        this.filterExpression = BatchIcebergInput.genIcebergFilterExpression(timeRangeStart, timeRangeEnd);
        this.timeRangeList.add(new TimeRangeTuple(timeRangeStart, timeRangeEnd));
        this.isOptimizedCountValided = true;
    }

    public BatchIcebergInput(String icebergTableName, List<TimeRangeTuple> timeRangeList) {
        this.icebergTableName = icebergTableName;
        this.timeRangeList = timeRangeList;
        this.filterExpression = genExpressionFromCollection();
        this.isOptimizedCountValided = true;
    }

    public static String genIcebergFilterExpression(long startTime, long endTime) {
        String timeRangeStartStr = TimeFormatConverter$.MODULE$.icebergFilterTimeFormat().format(startTime);
        String timeRangeEndStr = TimeFormatConverter$.MODULE$.icebergFilterTimeFormat().format(endTime);
        LOGGER.info(
                String.format("Start to generate filter expression from %s to %s", timeRangeStartStr, timeRangeEndStr));
        long startTimeStampMicroSecond = startTime / 1000;
        long endTimeStampMicroSecond = endTime / 1000;

        String filterExpression =
                String.format("(____et >= cast(%d as timestamp) AND ____et < cast(%d as timestamp))",
                        startTimeStampMicroSecond, endTimeStampMicroSecond);
        return filterExpression;
    }

    @Override
    public String getInputInfo() {
        return icebergTableName;
    }

    @Override
    public ConstantVar.ChannelType getType() {
        return ConstantVar.ChannelType.iceberg;
    }

    private String genExpressionFromCollection() {
        List<String> expressions = new LinkedList<>();

        if (this.timeRangeList != null) {
            for (TimeRangeTuple item : this.timeRangeList) {
                expressions.add(BatchIcebergInput
                        .genIcebergFilterExpression(item.getStartTime(), item.getEndTime()));
            }
        }
        return String.join(" OR ", expressions);
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean isIcebergOptimizedCountEnable() {
        return this.isOptimizedCountValided;
    }

    @Override
    public List<TimeRangeTuple> getTimeRangeList() {
        return this.timeRangeList;
    }

    @Override
    public boolean isCountTotalValue() {
        return false;
    }

    public String getFilterExpression() {
        return this.filterExpression;
    }
}
