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

package com.tencent.bk.base.dataflow.metrics.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 时间操作类
 */
public class TimeUtils {

  private static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private String timeFormat;
  private DateTimeFormatter format = null;
  private long lastValue = 0L;
  private long offset = 0L;

  /**
   * 构造默认 TimeUtils ，格式为 "yyyy-MM-dd HH:mm:ss"
   */
  public TimeUtils() {
    this(DEFAULT_TIME_FORMAT);
  }

  /**
   * 构造 TimeUtils
   *
   * @param timeFormat 时间格式串，可为null
   *                   为null或者空串时，获取的时unix时间戳，单位：秒
   *                   为有效时间格式串时，获取对应格式的时间串；如果格式串非法，则使用默认格式串 "yyyy-MM-dd HH:mm:ss"
   */
  public TimeUtils(String timeFormat) {
    this(timeFormat, 0L);
  }

  /**
   * 构造 TimeUtils
   *
   * @param timeFormat 时间格式串，可为null
   *                   为null或者空串时，获取的时unix时间戳，单位：秒
   *                   为有效时间格式串时，获取对应格式的时间串；如果格式串非法，则使用默认格式串 "yyyy-MM-dd HH:mm:ss"
   * @param offset     时间偏移量，单位秒
   */
  public TimeUtils(String timeFormat, long offset) {
    this.offset = offset;
    if (timeFormat != null && !timeFormat.isEmpty()) {
      this.timeFormat = timeFormat;
      try {
        this.format = DateTimeFormatter.ofPattern(timeFormat);
      } catch (IllegalArgumentException ex) {
        // use default
        this.timeFormat = DEFAULT_TIME_FORMAT;
        this.format = DateTimeFormatter.ofPattern(timeFormat);
      }
    } else {
      // do nothing
    }
  }

  /**
   * 获取当前时间串
   *
   * @return java.lang.String
   */
  public String getCurrentDataTime() {
    LocalDateTime now = LocalDateTime.now();
    now = now.minusSeconds(offset);
    return now.format(format);
  }

  /**
   * 获取当前 unix 时间戳，单位：秒
   *
   * @return long
   */
  public long getCurrentEpochSecond() {
    LocalDateTime now = LocalDateTime.now();
    now = now.minusSeconds(offset);
    return now.atZone(ZoneId.systemDefault()).toEpochSecond();
  }

  /**
   * 获取上一次的 unix 时间戳，单位：秒
   *
   * @return long
   */
  public long getLastEpochSecond() {
    if (lastValue > 0) {
      return lastValue;
    }
    LocalDateTime now = LocalDateTime.now();
    return now.atZone(ZoneId.systemDefault()).toEpochSecond();
  }

  /**
   * 获取时间格式串
   *
   * @return java.lang.String
   */
  public String getTimeFormat() {
    return timeFormat;
  }

}
