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

package com.tencent.bk.base.datalab.queryengine.common.time;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class DateUtil {

    public static final int MINUTE_5 = 5;
    public static final int MINUTE_6 = 6;
    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    public static final String YYYYMMDD = "yyyyMMdd";
    public static final String YYYYMMDD_HH_MM_SS = "yyyyMMdd HH:mm:ss";
    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYY_MM_DD_HHMM = "yyyy-MM-dd_HHmm";

    public static String getMonthFirstDay(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_MONTH, calendar
                .getActualMinimum(Calendar.DAY_OF_MONTH));
        Date date = calendar.getTime();
        return dateFormat.format(date);
    }

    /**
     * 得到本月的最后一天
     *
     * @param format 返回的日期格式
     * @return 日期字符串
     */
    public static String getMonthLastDay(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_MONTH, calendar
                .getActualMaximum(Calendar.DAY_OF_MONTH));
        Date date = calendar.getTime();
        return dateFormat.format(date);
    }

    /**
     * 根据指定格式返回当前日期字符串
     *
     * @param pattern 日期格式
     * @return 日期字符串
     */
    public static String getCurrentDate(String pattern) {
        Calendar c = Calendar.getInstance();
        DateFormat format = new SimpleDateFormat(pattern);
        return format.format(c.getTime());
    }

    /**
     * 根据日期和指定格式返回日期字符串
     *
     * @param pattern 日期格式
     * @param date Date实例
     * @return 日期字符串
     */
    public static String getSpecifiedDate(String pattern, Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        DateFormat format = new SimpleDateFormat(pattern);
        return format.format(c.getTime());
    }

    /**
     * 返回指定日期多少分钟之前的日期，并格式化输出
     *
     * @param pattern 日期格式
     * @param date 日期
     * @param minutes 多少分钟之前
     * @return 日期字符串
     */
    public static String getSpecialDateBefore(String pattern, Date date,
            int minutes) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, minutes);
        DateFormat format = new SimpleDateFormat(pattern);
        return format.format(c.getTime());
    }

    /**
     * 返回指定日期多少天之前的日期，并格式化输出
     *
     * @param pattern 日期格式
     * @param date Date实例
     * @param day 天数
     * @return 日期字符串
     */
    public static String getSpecialDateBeforeDay(String pattern, Date date,
            int day) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, day);
        DateFormat format = new SimpleDateFormat(pattern);
        return format.format(c.getTime());
    }

    /**
     * 返回当前时间多少分钟之前的时间，并格式化输出
     *
     * @param pattern 日期格式
     * @param minutes 多少分钟之前
     * @return 日期字符串
     */
    public static String getCurrentDateBefore(String pattern, int minutes) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, -minutes);
        DateFormat format = new SimpleDateFormat(pattern);
        return format.format(c.getTime());
    }

    /**
     * unix 时间戳(毫秒)转换成 Date 实例
     *
     * @param mills 毫秒
     * @return Date实例
     */
    public static Date millsUnix2Date(long mills) {
        return new Date(mills);
    }

    /**
     * unix 时间戳(秒)转换成 Date 实例
     *
     * @param seconds 秒
     * @return Date实例
     */
    public static Date unix2Date(long seconds) {
        return new Date(seconds * 1000);
    }

    /**
     * 将 yyyy-MM-dd HH:mm:ss 格式的字符串转换成 yyyyMMddHHmm 格式的字符串
     *
     * @param dateStr 需要转换的日期字符串
     * @return 日期字符串
     */
    public static String getMinutesStr(String dateStr) {
        String result = null;
        String date = dateStr;
        if (date != null) {
            String year = date.substring(0, 4);
            String month = date.substring(5, 7);
            String day = date.substring(8, 10);
            String hour = date.substring(11, 13);
            String minutes = date.substring(14, 16);
            result = year + month + day + hour + minutes;
        }
        return result;
    }

    /**
     * 将指定格式的日期字符串转换成 Date 实例
     *
     * @param dateStr 日期字符串
     * @param pattern 日期格式
     * @return Date实例
     */
    public static Date getDateByString(String dateStr, String pattern) {
        DateFormat format;
        format = new SimpleDateFormat(pattern);
        try {
            return format.parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 获取指定日期的周一
     *
     * @param date 输入日期
     * @param inDatePattern 输入日期格式 如yyyy-MM-dd HH:mm:ss
     * @param outDatePattern 输出日期格式 如yyyy-MM-dd HH:mm:ss
     * @return 日期字符串
     */
    public static String getMonday(String date, String inDatePattern, String outDatePattern) {
        if (StringUtils.isBlank(date)) {
            return "";
        }
        SimpleDateFormat inFormat = new SimpleDateFormat(inDatePattern);
        SimpleDateFormat outFormat = new SimpleDateFormat(outDatePattern);
        Date d;
        try {
            d = inFormat.parse(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            cal.setFirstDayOfWeek(Calendar.MONDAY);
            cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            return outFormat.format(cal.getTime());
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取指定日期所在周的周日
     *
     * @param date 输入日期
     * @param inDatePattern 输入日期格式 如 yyyy-MM-dd HH:mm:ss
     * @param outDatePattern 输出日期格式 如 yyyy-MM-dd HH:mm:ss
     * @return 日期字符串
     */
    public static String getSunday(String date, String inDatePattern, String outDatePattern) {
        if (StringUtils.isBlank(date)) {
            return "";
        }
        SimpleDateFormat inFormat = new SimpleDateFormat(inDatePattern);
        SimpleDateFormat outFormat = new SimpleDateFormat(outDatePattern);
        try {
            Date d = inFormat.parse(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            cal.setFirstDayOfWeek(Calendar.MONDAY);
            cal.add(Calendar.DATE, -1 * 7);
            cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
            return outFormat.format(cal.getTime());
        } catch (Exception e) {
            return "";
        }
    }


    /**
     * 获取指定日期所在月份第一天
     *
     * @param date 输入日期
     * @param inDatePattern 输入日期格式 如 yyyy-MM-dd HH:mm:ss
     * @param outDatePattern 输出日期格式 如 yyyy-MM-dd HH:mm:ss
     * @return 日期字符串
     */
    public static String getfirstDayofMonth(String date, String inDatePattern,
            String outDatePattern) {
        if (StringUtils.isBlank(date)) {
            return "";
        }
        SimpleDateFormat inFormat = new SimpleDateFormat(inDatePattern);
        SimpleDateFormat outFormat = new SimpleDateFormat(outDatePattern);
        try {
            Date d = inFormat.parse(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
            return outFormat.format(cal.getTime());
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取指定日期所在月份最后一天
     *
     * @param date 输入日期
     * @param inDatePattern 输入日期格式 如 yyyy-MM-dd HH:mm:ss
     * @param outDatePattern 输出日期格式 如 yyyy-MM-dd HH:mm:ss
     * @return 日期字符串
     */
    public static String getlastDayofMonth(String date, String inDatePattern,
            String outDatePattern) {
        if (StringUtils.isBlank(date)) {
            return "";
        }
        SimpleDateFormat inFormat = new SimpleDateFormat(inDatePattern);
        SimpleDateFormat outFormat = new SimpleDateFormat(outDatePattern);
        try {
            Date d = inFormat.parse(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
            return outFormat.format(cal.getTime());
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 返回两个日期之间相差多少毫秒
     *
     * @param beginDateStr 开始日期
     * @param endDateStr 结束日期
     * @param pattern 日期格式
     * @return 相差毫秒数
     */
    public static long getTimeDiff(String beginDateStr, String endDateStr, String pattern) {
        long diffInSeconds = 0L;
        try {
            Date beginDate = getDateByString(beginDateStr, pattern);
            Date endDate = getDateByString(endDateStr, pattern);
            if (Objects.nonNull(beginDate) && Objects.nonNull(endDate)) {
                diffInSeconds = endDate.getTime() - beginDate.getTime();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return diffInSeconds;
    }

    /**
     * 开始日期和结束日期相差天数
     *
     * @param sDate 开始日期
     * @param eDate 结束日期
     * @param format 日期格式
     * @return 相差天数
     */
    public static int getDayDiffBetween(String sDate, String eDate, String format) {
        int result = 0;
        if (StringUtils.isNotBlank(sDate)
                && StringUtils.isNotBlank(eDate)) {
            Date regDate = getDateByString(sDate, format);
            Date loginDate = getDateByString(eDate, format);
            Calendar regCalendar = Calendar.getInstance();
            regCalendar.setTime(regDate);
            Calendar loginCalendar = Calendar.getInstance();
            loginCalendar.setTime(loginDate);
            if (loginCalendar.after(regCalendar)) {
                regCalendar.set(Calendar.HOUR_OF_DAY, 0);
                regCalendar.set(Calendar.MINUTE, 0);
                regCalendar.set(Calendar.SECOND, 0);
                loginCalendar.set(Calendar.HOUR_OF_DAY, 0);
                loginCalendar.set(Calendar.MINUTE, 0);
                loginCalendar.set(Calendar.SECOND, 0);
                long loginTimeMill = loginCalendar.getTimeInMillis();
                long regTimeMill = regCalendar.getTimeInMillis();
                long dayDiff = (loginTimeMill - regTimeMill) / (1000 * 3600 * 24);
                result = (int) dayDiff;
            }
        }
        return result;
    }

    /**
     * 获取两个 Date 之间的天数差
     *
     * @param firstDate 开始时间
     * @param secondDate 结束时间
     * @return 天数差
     */
    public static int getDayDiffBetween(Date firstDate, Date secondDate) {
        Calendar firstCal = Calendar.getInstance();
        firstCal.setTime(firstDate);
        Calendar secondCal = Calendar.getInstance();
        secondCal.setTime(secondDate);
        int firstDay = firstCal.get(Calendar.DAY_OF_YEAR);
        int secondDay = secondCal.get(Calendar.DAY_OF_YEAR);
        int firstYear = firstCal.get(Calendar.YEAR);
        int secondYear = secondCal.get(Calendar.YEAR);
        if (firstYear != secondYear) {
            int timeDistance = 0;
            for (int i = firstYear; i < secondYear; i++) {
                if (i % 4 == 0 && i % 100 != 0 || i % 400 == 0) {
                    timeDistance += 366;
                } else {
                    timeDistance += 365;
                }
            }
            return timeDistance + (secondDay - firstDay);
        } else {
            return secondDay - firstDay;
        }
    }

    /**
     * 开始日期和结束日期相差月数
     *
     * @param sDay 开始日期
     * @param eDay 结束日期
     * @param format 日期格式
     * @return 相差月数
     */
    public static int getMonthDiff(String sDay, String eDay, String format) {
        if (StringUtils.isAnyBlank(sDay, eDay, format)) {
            return 0;
        }
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Date sDate = sdf.parse(sDay);
            Date eDate = sdf.parse(eDay);
            Calendar sCal = Calendar.getInstance();
            Calendar eCal = Calendar.getInstance();
            sCal.setTime(sDate);
            eCal.setTime(eDate);
            int yDiff = eCal.get(Calendar.YEAR) - sCal.get(Calendar.YEAR);
            int mDiff = eCal.get(Calendar.MONTH) - sCal.get(Calendar.MONTH);
            return yDiff * 12 + mDiff;
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}