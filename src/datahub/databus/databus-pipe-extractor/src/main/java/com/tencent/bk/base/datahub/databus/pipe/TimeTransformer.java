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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.exception.TimeFormatError;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeTransformer {

    public interface Transformer {

        Long transform(Object timeVal) throws TimeFormatError;

        String getFormat();
    }

    /**
     * 时间转换器的工厂函数.
     */
    public static Transformer transformerFactory(final String timeFormat,
            final int timestampLen,
            final double timezone,
            final int timeRound) {

        if (timestampLen > 0) {
            return new Transformer() {
                public Long transform(Object data) {
                    try {
                        Long timeValue = Long.parseLong(data.toString().trim());
                        Long localTime;
                        if (timestampLen == 13) {  //millisecond
                            localTime = timeValue;
                        } else if (timestampLen == 10) {  // seconds
                            localTime = timeValue * 1000;
                        } else if (timestampLen == 8) {  // minutes
                            localTime = timeValue * 60 * 1000;
                        } else if (timestampLen == 16) {  // microseconds
                            localTime = timeValue / 1000;
                        } else {
                            localTime = timeValue / (10 * (10 - timestampLen)) * 1000;
                        }
                        return (localTime / timeRound) * timeRound;
                    } catch (Exception e) {
                        throw new TimeFormatError(e.getMessage());
                    }
                }

                public String getFormat() {
                    return timeFormat;
                }
            };

        } else if ("".equals(timeFormat)) {
            throw new RuntimeException("At least we need one of time_fomat"
                    + " and timestamp_len, please check"
                    + " configuration");

        } else {
            return new Transformer() {
                public Long transform(Object data) {
                    try {
                        String timeValue = data.toString();
                        return ((long) ((TimeTransformer.convertToTimestamp(timeFormat, timeValue)
                                - (timezone) * 3600 * 1000) / timeRound) * timeRound);
                    } catch (Exception e) {
                        throw new TimeFormatError(e.getMessage());
                    }
                }

                public String getFormat() {
                    return timeFormat;
                }
            };
        }
    }


    /**
     * 根据时间格式，将字符串的日期时间装换成Long类型的时间戳.
     */
    public static Long convertToTimestamp(String format, String data) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);

        dateFormat.setTimeZone(TimeZone.getTimeZone(EtlConsts.DEFAULT_TIMEZONE));
        Date parsedTimeStamp = dateFormat.parse(data);

        Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());

        return timestamp.getTime();
    }
}
