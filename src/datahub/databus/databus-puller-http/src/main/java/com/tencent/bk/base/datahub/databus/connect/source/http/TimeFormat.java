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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import java.text.SimpleDateFormat;
import java.util.function.Function;

public class TimeFormat {

    public static final String UNIX_TIME_STAMP_MILLISECONDS = "Unix Time Stamp(milliseconds)";
    public static final String UNIX_TIME_STAMP_MINS = "Unix Time Stamp(mins)";
    public static final String UNIX_TIME_STAMP_SECONDS = "Unix Time Stamp(seconds)";

    private String timeFormat;
    private SimpleDateFormat timeFieldFormat;
    private final Function<Long, String> formatFunction;

    public TimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
        if (UNIX_TIME_STAMP_MILLISECONDS.equalsIgnoreCase(this.timeFormat)) {
            formatFunction = (time) -> String.valueOf(time);
        } else if (UNIX_TIME_STAMP_SECONDS.equalsIgnoreCase(this.timeFormat)) {
            formatFunction = (time) -> String.valueOf(time / 1000);
        } else if (UNIX_TIME_STAMP_MINS.equalsIgnoreCase(this.timeFormat)) {
            formatFunction = (time) -> String.valueOf(time / 60000);
        } else {
            this.timeFieldFormat = new SimpleDateFormat(timeFormat);
            formatFunction = (time) -> this.timeFieldFormat.format(time);
        }
    }

    /**
     * 格式化指定的时间戳
     *
     * @param time 时间戳
     * @return 格式化后的日期格式
     */
    public final String format(long time) {
        return this.formatFunction.apply(time);
    }


}
