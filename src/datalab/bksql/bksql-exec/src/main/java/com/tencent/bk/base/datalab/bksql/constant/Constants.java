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

package com.tencent.bk.base.datalab.bksql.constant;

import com.google.common.base.Joiner;
import java.time.format.DateTimeFormatter;

public class Constants {

    /**
     * rt保留字段 thedate
     */
    public static final String THEDATE = "thedate";

    /**
     * rt保留字段 dteventtime
     */
    public static final String DTEVENTTIME = "dteventtime";

    /**
     * rt保留字段 dteventtimestamp
     */
    public static final String DTEVENTTIMESTAMP = "dteventtimestamp";

    /**
     * hive表 hour分区关键字
     */
    public static final String DTPARUNIT = "dt_par_unit";

    /**
     * 日期格式 yyyy-MM-dd
     */
    public static final String PATTERN_OF_YYYY_MM_DD = "yyyy-MM-dd";

    /**
     * 日期格式 yyyyMMdd
     */
    public static final String PATTERN_OF_YYYYMMDD = "yyyyMMdd";

    /**
     * 日期格式 yyyyMMdd HH:mm:ss"
     */
    public static final String PATTERN_OF_YYYYMMDD_HH_MM_SS = "yyyyMMdd HH:mm:ss";

    /**
     * 日期格式 yyyy-MM-dd HH:mm:ss
     */
    public static final String PATTERN_OF_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * 日期格式 yyyy-MM-dd'T'HH:mm:ss'Z'
     */
    public static final String PATTERN_OF_YYYY_MM_DD_T_HH_MM_SS_WITH_TIMEZONE = "yyyy-MM-dd'T'HH:mm"
            + ":ss'Z'";

    /**
     * 日期格式 yyyy-MM-dd HH:mm:ss UTC
     */
    public static final String PATTERN_OF_YYYY_MM_DD_HH_MM_SS_WITH_UTC = "yyyy-MM-dd HH:mm:ss "
            + "'UTC'";

    /**
     * 日期格式 yyyyMMddHH
     */
    public static final String PATTERN_OF_YYYYMMDDHH = "yyyyMMddHH";

    /**
     * 完整时间格式
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER_FULL = DateTimeFormatter
            .ofPattern(PATTERN_OF_YYYY_MM_DD_HH_MM_SS);

    /**
     * 小时时间格式
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER_HOUR = DateTimeFormatter
            .ofPattern(PATTERN_OF_YYYYMMDDHH);

    /**
     * 天时间格式
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER_DAY = DateTimeFormatter
            .ofPattern(PATTERN_OF_YYYYMMDD);

    /**
     * 时区
     */
    public static final String BK_TIMEZONE = "BK_TIMEZONE";

    /**
     * 上海时区
     */
    public static final String ASIA_SHANGHAI = "Asia/Shanghai";

    /**
     * 逗号 Joiner
     */
    public static final Joiner DOT_JOINER = Joiner.on(".");
}
