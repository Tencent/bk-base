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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;

public class CronUtil {

    /**
     * parse to CronExpression
     *
     * @param cronExpression A cron expression is a string separated by white space, to provide a parser and
     *         evaluator for Quartz cron expressions.
     * @param timezone timezone
     * @return org.quartz.CronExpression object.
     */
    public static CronExpression parseCronExpression(final String cronExpression, final TimeZone timezone)
            throws NaviException {
        if (cronExpression != null) {
            try {
                final CronExpression ce = new CronExpression(cronExpression);
                ce.setTimeZone(timezone);
                return ce;
            } catch (final ParseException e) {
                throw new NaviException("this cron expression {" + cronExpression + "} can not be parsed. "
                        + "Please Check Quartz Cron Syntax.", e);
            }
        } else {
            return null;
        }
    }

    /**
     * check if the cronExpression is valid or not.
     *
     * @return if the cronExpression is valid or not.
     */
    public static boolean isCronExpressionValid(final String cronExpression, final TimeZone timezone)
            throws NaviException {
        if (!CronExpression.isValidExpression(cronExpression)) {
            throw new NaviException("this cron expression {" + cronExpression + "} can not be parsed. "
                    + "Please Check Quartz Cron Syntax.");
        }

        final CronExpression cronExecutionTime = parseCronExpression(cronExpression, timezone);
        if (cronExecutionTime == null) {
            return false;
        }

        if (cronExecutionTime.getNextValidTimeAfter(new Date()) == null) {
            throw new NaviException("this cron expression can not start because start time is before now.");
        }

        return true;
    }

    /**
     * parse period string to mills
     *
     * @param periodStr
     * @return
     */
    public static Long parsePeriodStringToMills(String periodStr) {
        if (periodStr == null || periodStr.isEmpty()) {
            return 0L;
        }
        final char periodUnit = periodStr.charAt(periodStr.length() - 1);
        final int periodInt = Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
        PeriodUnit unit = PeriodUnit.value(periodUnit);
        switch (unit) {
            case second:
                return 1000L * periodInt;
            case minute:
                return 1000L * 60 * periodInt;
            case hour:
                return 1000L * 60 * 60 * periodInt;
            case day:
                return 1000L * 60 * 60 * 24 * periodInt;
            case week:
                return 1000L * 60 * 60 * 24 * 7 * periodInt;
            case month:
                return 1000L * 60 * 60 * 24 * 31 * periodInt; //default 31 days
            default:
                throw new IllegalArgumentException("Invalid schedule period unit:" + periodUnit);
        }
    }

    /**
     * parse period string to mills
     *
     * @param periodStr
     * @param baseTimeMills
     * @param timeZone
     * @return
     */
    public static Long parsePeriodStringToMills(String periodStr, long baseTimeMills, TimeZone timeZone) {
        return parsePeriodStringToMills(periodStr, baseTimeMills, timeZone, false);
    }

    /**
     * parse period string to mills
     *
     * @param periodStr
     * @param baseTimeMills
     * @param timeZone
     * @param forward parse period string to mills forward from baseTimeMills. e.g. 1M means 1 month period
     *         after baseTimeMills if forward is true
     * @return
     */
    public static Long parsePeriodStringToMills(String periodStr, long baseTimeMills, TimeZone timeZone,
            boolean forward) {
        if (periodStr == null || periodStr.isEmpty()) {
            return 0L;
        }
        final int periodInt = Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
        final PeriodUnit periodUnit = PeriodUnit.value(periodStr.charAt(periodStr.length() - 1));
        return parsePeriodStringToMills(periodInt, periodUnit, baseTimeMills, timeZone, forward);
    }

    /**
     * parse period string to mills
     *
     * @param periodInt
     * @param periodUnit
     * @return
     */
    public static Long parsePeriodStringToMills(int periodInt, PeriodUnit periodUnit) {
        switch (periodUnit) {
            case second:
                return 1000L * periodInt;
            case minute:
                return 1000L * 60 * periodInt;
            case hour:
                return 1000L * 60 * 60 * periodInt;
            case day:
                return 1000L * 60 * 60 * 24 * periodInt;
            case week:
                return 1000L * 60 * 60 * 24 * 7 * periodInt;
            case month:
                return 1000L * 60 * 60 * 24 * 31 * periodInt; //default 31 days
            default:
                throw new IllegalArgumentException("Invalid schedule period unit:" + periodUnit);
        }
    }

    /**
     * parse period string to mills
     *
     * @param periodInt
     * @param periodUnit
     * @param baseTimeMills
     * @param timeZone
     * @return
     */
    public static Long parsePeriodStringToMills(int periodInt, PeriodUnit periodUnit, long baseTimeMills,
            TimeZone timeZone) {
        return parsePeriodStringToMills(periodInt, periodUnit, baseTimeMills, timeZone, false);
    }

    /**
     * parse period string to mills
     *
     * @param periodInt
     * @param periodUnit
     * @param baseTimeMills
     * @param timeZone
     * @param forward parse period string to mills forward from baseTimeMills. e.g. 1M means 1 month period
     *         after baseTimeMills if forward is true
     * @return
     */
    public static Long parsePeriodStringToMills(int periodInt, PeriodUnit periodUnit, long baseTimeMills,
            TimeZone timeZone, boolean forward) {
        switch (periodUnit) {
            case second:
            case minute:
            case hour:
            case day:
            case week:
                return parsePeriodStringToMills(periodInt, periodUnit);
            case month:
                DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(timeZone);
                DateTime baseTime = new DateTime(baseTimeMills, dateTimeZone);
                if (forward) {
                    DateTime endTime = baseTime.plusMonths(periodInt);
                    return endTime.toDate().getTime() - baseTime.toDate().getTime();
                } else {
                    DateTime endTime = baseTime.minusMonths(periodInt);
                    return baseTime.toDate().getTime() - endTime.toDate().getTime();
                }
            default:
                throw new IllegalArgumentException("Invalid schedule period unit:" + periodUnit);
        }
    }

    /**
     * get time range by period
     *
     * @param startPeriodStr
     * @param endPeriodStr
     * @param timezone
     * @param timeMills
     * @return
     */
    public static Long[] getTimeRangeByPeriod(String startPeriodStr, String endPeriodStr, TimeZone timezone,
            Long timeMills) {
        return PeriodUtil.getTimeMillsRangeByPeriodStr(startPeriodStr, endPeriodStr, timezone, timeMills);
    }

    /**
     * parse period end time from period string
     *
     * @param periodStr
     * @param timezone
     * @param timeMills
     * @return
     */
    public static Long parsePeriodEndTime(String periodStr, TimeZone timezone, Long timeMills) {
        final char periodUnit = periodStr.charAt(periodStr.length() - 1);
        PeriodUnit unit = PeriodUnit.value(periodUnit);
        return unit.getPeriodTimestampRange(timeMills, timezone)[1];
    }

    /**
     * format time
     *
     * @param timeMills
     * @return
     */
    public static String getPrettyTime(Long timeMills) {
        return getPrettyTime(timeMills, "yyyy-MM-dd HH:mm:ss.SSSXXX");
    }

    /**
     * format time
     *
     * @param timeMills
     * @param pattern
     * @return
     */
    public static String getPrettyTime(Long timeMills, String pattern) {
        Date d = new Date(timeMills);
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(d);
    }

    /**
     * format time
     *
     * @param timeMills
     * @param pattern
     * @return
     */
    public static String getPrettyTime(Long timeMills, String pattern, TimeZone timeZone) {
        Date d = new Date(timeMills);
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        format.setTimeZone(timeZone);
        return format.format(d);
    }
}
