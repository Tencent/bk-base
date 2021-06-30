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

package com.tencent.bk.base.dataflow.flink.streaming.transform.join;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;
import org.apache.commons.lang3.time.FastDateFormat;

public class JoinTimeFieldUtils {

    private static final String dtEventTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    /**
     * 取join窗口时间开始时间
     *
     * @param dtEventTime 事件时间
     * @param joinWindowSize 窗口大小
     * @return
     */
    public static String getJoinWindowStartTime(String dtEventTime, Long joinWindowSize) {
        try {
            FastDateFormat format = FastDateFormat.getInstance(dtEventTimeFormat, utcTimeZone);
            Long dtEventTimeStamp = format.parse(dtEventTime).getTime();
            // 取窗口开始时间
            dtEventTimeStamp = dtEventTimeStamp / joinWindowSize * joinWindowSize;
            return format.format(new Date(dtEventTimeStamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dtEventTime;
    }

    /**
     * 取join窗口时间结束时间
     *
     * @param dtEventTime 事件时间
     * @param joinWindowSize 窗口大小
     * @return
     */
    public static String getJoinWindowEndTime(String dtEventTime, Long joinWindowSize) {
        try {
            FastDateFormat format = FastDateFormat.getInstance(dtEventTimeFormat, utcTimeZone);
            Long dtEventTimeStamp = format.parse(dtEventTime).getTime();
            // 取窗口开始时间
            dtEventTimeStamp = (dtEventTimeStamp / joinWindowSize * joinWindowSize) + joinWindowSize;
            return format.format(new Date(dtEventTimeStamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dtEventTime;
    }
}
