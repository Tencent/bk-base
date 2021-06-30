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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class PeriodUtil {

    /**
     * get period int by keyword
     *
     * @param keyword
     * @param unit
     * @param timeMills
     * @param timeZone
     * @return period int
     */
    public static int getPeriodIntByKeyword(PeriodKeyword keyword, PeriodUnit unit, long timeMills, TimeZone timeZone) {
        switch (keyword) {
            case NOW:
                DateTime time = new DateTime(timeMills, DateTimeZone.forTimeZone(timeZone));
                switch (unit) {
                    case hour:
                        return time.getHourOfDay();
                    case dayOfWeek:
                        return time.getDayOfWeek();
                    case day:
                    case dayOfMonth:
                        return time.getDayOfMonth();
                    default:
                        throw new UnsupportedOperationException("PeriodUnit " + unit + " may not support");
                }
            default:
                throw new UnsupportedOperationException("Keyword " + keyword + " may not support");
        }
    }

    /**
     * get time mills range by period string
     *
     * @param startPeriodStr
     * @param endPeriodStr
     * @param timeZone
     * @param timeMills
     * @return time mills
     */
    public static Long[] getTimeMillsRangeByPeriodStr(String startPeriodStr, String endPeriodStr, TimeZone timeZone,
            long timeMills) {
        if (startPeriodStr.charAt(startPeriodStr.length() - 1) != endPeriodStr.charAt(endPeriodStr.length() - 1)) {
            throw new IllegalArgumentException("Inconsistent period unit between start period and end period");
        }
        // negative periodInt indicates the abs(periodInt)th period from period end
        DateTime baseDateTime = new DateTime(timeMills, DateTimeZone.forTimeZone(timeZone));
        long startBaseTimeMills;
        long endBaseTimeMills;
        final PeriodUnit periodUnit = PeriodUnit.value(startPeriodStr.charAt(startPeriodStr.length() - 1));
        switch (periodUnit) {
            case hour:
                startBaseTimeMills = baseDateTime.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
                        .withMillisOfSecond(0).getMillis();
                endBaseTimeMills = baseDateTime.plusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
                        .withMillisOfSecond(0).getMillis();
                return getTimeMillsRangeByPeriodStr(startPeriodStr, endPeriodStr, timeZone, startBaseTimeMills,
                        endBaseTimeMills);
            case dayOfWeek:
                startBaseTimeMills = baseDateTime.withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0)
                        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
                endBaseTimeMills = baseDateTime.plusWeeks(1).withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0)
                        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
                return getTimeMillsRangeByPeriodStr(startPeriodStr, endPeriodStr, timeZone, startBaseTimeMills,
                        endBaseTimeMills);
            case day:
            case dayOfMonth:
                startBaseTimeMills = baseDateTime.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
                        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
                endBaseTimeMills = baseDateTime.plusMonths(1).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
                        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
                return getTimeMillsRangeByPeriodStr(startPeriodStr, endPeriodStr, timeZone, startBaseTimeMills,
                        endBaseTimeMills);
            default:
                throw new IllegalArgumentException("Invalid schedule period unit '" + periodUnit);
        }
    }

    /**
     * get time mills range by period string
     *
     * @param startPeriodStr
     * @param endPeriodStr
     * @param timeZone
     * @param startBaseTimeMills
     * @param endBaseTimeMills
     * @return time mills
     */
    public static Long[] getTimeMillsRangeByPeriodStr(String startPeriodStr, String endPeriodStr, TimeZone timeZone,
            long startBaseTimeMills, long endBaseTimeMills) {
        // positive periodInt begin with 1
        // negative periodInt indicates the abs(periodInt)th period from period end
        int startPeriodInt = getPeriodInt(startPeriodStr);
        if (startPeriodInt == 0) {
            //compatible with old range dependence period,
            //"1H~0H" means this 1st hour to the last hour of the day, equals to "1H~24H"
            startPeriodInt = -1;
        }
        int endPeriodInt = getPeriodInt(endPeriodStr);
        if (endPeriodInt == 0) {
            endPeriodInt = -1; //the same as startPeriodInt
        }
        Long[] range = new Long[2];
        DateTime startBaseDateTime = new DateTime(startBaseTimeMills, DateTimeZone.forTimeZone(timeZone));
        DateTime endBaseDateTime = new DateTime(endBaseTimeMills, DateTimeZone.forTimeZone(timeZone));
        final PeriodUnit startPeriodUnit = PeriodUnit.value(startPeriodStr.charAt(startPeriodStr.length() - 1));
        DateTime startTime = parseTimeRangeStart(startPeriodInt, startPeriodUnit, startBaseDateTime, endBaseDateTime);
        range[0] = startTime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate().getTime();
        final PeriodUnit endPeriodUnit = PeriodUnit.value(endPeriodStr.charAt(endPeriodStr.length() - 1));
        DateTime endTime = parseTimeRangeEnd(endPeriodInt, endPeriodUnit, startBaseDateTime, endBaseDateTime);
        range[1] = endTime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate().getTime();
        return range;
    }

    /**
     * get time mills range by period string
     *
     * @param periodStr
     * @param timeZone
     * @param timeMills
     * @return time mills range
     */
    public static Long[] getTimeMillsRangeByPeriodStr(String periodStr, TimeZone timeZone, long timeMills) {
        return getTimeMillsRangeByPeriodStr(periodStr, periodStr, timeZone, timeMills);
    }

    /**
     * get time mills by period unit
     * @param unit
     * @return time mills
     */
    public static long getTimeMills(PeriodUnit unit) {
        switch (unit) {
            case second:
                return 1000;
            case minute:
                return 1000 * 60;
            case hour:
                return 1000 * 60 * 60;
            case day:
                return 1000 * 60 * 60 * 24;
            case week:
                return 1000 * 60 * 60 * 24 * 7;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit '"
                        + unit + "'");
        }
    }

    public static int getPeriodInt(String periodStr) {
        return Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
    }

    public static String getYear() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy");
        return format.format(new Date());
    }

    public static String getYear(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy");
        return format.format(date);
    }

    public static String getMonth() {
        SimpleDateFormat format = new SimpleDateFormat("MM");
        return format.format(new Date());
    }

    public static String getMonth(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("MM");
        return format.format(date);
    }

    public static String getDay() {
        SimpleDateFormat format = new SimpleDateFormat("dd");
        return format.format(new Date());
    }

    public static String getDay(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("dd");
        return format.format(date);
    }

    private static DateTime parseTimeRangeStart(int startPeriodInt, PeriodUnit startPeriodUnit,
        DateTime startBaseDateTime, DateTime endBaseDateTime) {
        DateTime startTime;
        switch (startPeriodUnit) {
            case hour:
                startTime = (startPeriodInt > 0) ? startBaseDateTime.plusHours(startPeriodInt - 1)
                    : endBaseDateTime.plusHours(startPeriodInt);
                break;
            case day:
                startTime = (startPeriodInt > 0) ? startBaseDateTime.plusDays(startPeriodInt - 1)
                    : endBaseDateTime.plusDays(startPeriodInt);
                break;
            case week:
                startTime = (startPeriodInt > 0) ? startBaseDateTime.plusWeeks(startPeriodInt - 1)
                    : endBaseDateTime.plusWeeks(startPeriodInt);
                break;
            case month:
                startTime = (startPeriodInt > 0) ? startBaseDateTime.plusMonths(startPeriodInt - 1)
                    : endBaseDateTime.plusMonths(startPeriodInt);
                break;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit:" + startPeriodUnit);
        }
        return startTime;
    }

    private static DateTime parseTimeRangeEnd(int endPeriodInt, PeriodUnit endPeriodUnit,
        DateTime startBaseDateTime, DateTime endBaseDateTime) {
        DateTime endTime;
        switch (endPeriodUnit) {
            case hour:
                endTime = (endPeriodInt > 0) ? startBaseDateTime.plusHours(endPeriodInt)
                    : endBaseDateTime.plusHours(endPeriodInt + 1);
                break;
            case day:
                endTime = (endPeriodInt > 0) ? startBaseDateTime.plusDays(endPeriodInt)
                    : endBaseDateTime.plusDays(endPeriodInt + 1);
                break;
            case week:
                endTime = (endPeriodInt > 0) ? startBaseDateTime.plusWeeks(endPeriodInt)
                    : endBaseDateTime.plusWeeks(endPeriodInt + 1);
                break;
            case month:
                endTime = (endPeriodInt > 0) ? startBaseDateTime.plusMonths(endPeriodInt)
                    : endBaseDateTime.plusMonths(endPeriodInt + 1);
                break;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit:" + endPeriodUnit);
        }
        return endTime;
    }
}
