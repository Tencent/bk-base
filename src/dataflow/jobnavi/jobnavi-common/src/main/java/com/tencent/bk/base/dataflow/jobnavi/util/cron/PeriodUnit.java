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

package com.tencent.bk.base.dataflow.jobnavi.util.cron;

import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public enum PeriodUnit {

    second, minute, hour, day, week, month, year, dayOfWeek, dayOfMonth, once, cycle;

    /**
     * get period value from period unit char
     * @param periodUnitChar
     * @return
     */
    public static PeriodUnit value(char periodUnitChar) {
        switch (periodUnitChar) {
            case 'H':
            case 'h':
                return PeriodUnit.hour;
            case 'm':
                return PeriodUnit.minute;
            case 'd':
                return PeriodUnit.day;
            case 'M':
                return PeriodUnit.month;
            case 'D':
                return PeriodUnit.dayOfMonth;
            case 's':
                return PeriodUnit.second;
            case 'W':
                return PeriodUnit.week;
            case 'w':
                return PeriodUnit.dayOfWeek;
            case 'o':
                return PeriodUnit.once;
            case 'c':
                return PeriodUnit.cycle;
            default:
                throw new UnsupportedOperationException("Unsupported period Unit " + periodUnitChar);
        }
    }

    public static PeriodUnit value(String periodUnit) {
        char periodUnitChar = periodUnit.toCharArray()[0];
        return value(periodUnitChar);
    }

    /**
     * get period timestamp range
     *
     * @param timeMills
     * @return
     */
    public Long[] getPeriodTimestampRange(long timeMills, TimeZone timeZone) {
        DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(timeZone);
        DateTime time = new DateTime(timeMills, dateTimeZone);
        Long[] range = new Long[2];
        switch (this) {
            case second:
                range[0] = time.withMillisOfSecond(0).toDate().getTime();
                range[1] = time.withMillisOfSecond(999).toDate().getTime();
                break;
            case minute:
                range[0] = time.withSecondOfMinute(0).withMillisOfSecond(0).toDate().getTime();
                range[1] = time.withSecondOfMinute(59).withMillisOfSecond(999).toDate().getTime();
                break;
            case hour:
                range[0] = time.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate().getTime();
                range[1] = time.withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999).toDate().getTime();
                break;
            case day:
                range[0] = time.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
                        .toDate().getTime();
                range[1] = time.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999)
                        .toDate().getTime();
                break;
            case week:
                range[0] = time.withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
                        .withMillisOfSecond(0).toDate().getTime();
                range[1] = time.withDayOfWeek(7).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
                        .withMillisOfSecond(999).toDate().getTime();
                break;
            case month:
                range[0] = time.minusMonths(1).plusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
                        .withMillisOfSecond(0).toDate().getTime();
                range[1] = time.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999)
                        .toDate().getTime();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported period Unit:" + this);
        }
        return range;
    }

}
