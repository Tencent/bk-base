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

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class TimeFormatTest {

  @Test
  public final void formatUnixTimeStampMillisecondsTest() {
    TimeFormat format = new TimeFormat(TimeFormat.UNIX_TIME_STAMP_MILLISECONDS);
    Assert.assertEquals("1594559407389", format.format(1594559407389L));
  }


  @Test
  public final void formatUnixTimeStampSecondTest() {
    TimeFormat format = new TimeFormat(TimeFormat.UNIX_TIME_STAMP_SECONDS);
    Assert.assertEquals("1594559407", format.format(1594559407389L));
  }

  @Test
  public final void formatUnixTimeStampMinuteTest() {
    TimeFormat format = new TimeFormat(TimeFormat.UNIX_TIME_STAMP_MINS);
    Assert.assertEquals("26575990", format.format(1594559407389L));
  }


  @Test
  public final void formatPatternTest() {
    TimeFormat format = new TimeFormat("yyyy-MM-dd HH:mm:ss");
    Assert.assertEquals("2020-07-12 21:10:07", format.format(1594559407389L));

    format = new TimeFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    Assert.assertEquals("2020-07-12 21:10:07.000389", format.format(1594559407389L));

    format = new TimeFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    Assert.assertEquals("2020-07-12T21:10:07+08:00", format.format(1594559407389L));

    format = new TimeFormat("yyyy-MM-dd+HH:mm:ss");
    Assert.assertEquals("2020-07-12+21:10:07", format.format(1594559407389L));

    format = new TimeFormat("yyyy-MM-dd");
    Assert.assertEquals("2020-07-12", format.format(1594559407389L));

    format = new TimeFormat("yy-MM-dd HH:mm:ss");
    Assert.assertEquals("20-07-12 21:10:07", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMdd HH:mm:ss");
    Assert.assertEquals("20200712 21:10:07", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMdd HH:mm:ss.SSSSSS");
    Assert.assertEquals("20200712 21:10:07.000389", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMddHHmm");
    Assert.assertEquals("202007122110", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMddHHmm");
    Assert.assertEquals("202007122110", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMddHHmmss");
    Assert.assertEquals("20200712211007", format.format(1594559407389L));

    format = new TimeFormat("yyyyMMdd");
    Assert.assertEquals("20200712", format.format(1594559407389L));

    format = new TimeFormat("dd/MMM/yyyy:HH:mm:ss");
    Assert.assertEquals("12/七月/2020:21:10:07", format.format(1594559407389L));

    format = new TimeFormat("MM/dd/yyyy HH:mm:ss");
    Assert.assertEquals("07/12/2020 21:10:07", format.format(1594559407389L));
  }


}
