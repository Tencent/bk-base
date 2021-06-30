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

import java.util.Date;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.CronExpression;

public class TestCronUtil {

    @Test
    public void testCronExpressionValid1() {
        String expression = "0 0 1 * * ?";
        TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
        try {
            boolean isValid = CronUtil.isCronExpressionValid(expression, timeZone);
            Assert.assertTrue(isValid);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCronExpressionValid2() {
        String expression = "0 0 3 ? * * 22";
        TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
        try {
            boolean isValid = CronUtil.isCronExpressionValid(expression, timeZone);
            Assert.assertTrue(!isValid);
        } catch (NaviException e) {
            Assert.assertTrue(e.getMessage(), true);
        }
    }

    @Test
    public void testParseCronExpression() {
        String expression = "0 0 1 * * ?";
        TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
        try {
            CronExpression cronExpression = CronUtil.parseCronExpression(expression, timeZone);
            Date date = cronExpression.getNextValidTimeAfter(new Date());
            DateTime dateTime = new DateTime().plusDays(1).withTime(1, 0, 0, 0);
            Assert.assertEquals(dateTime.toDate().getTime(), date.getTime());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetCronExpressionPeriod1() {
        String expression = "0 0 0/1 * * ?";
        try {
            Date d = new Date();
            for (int i = 1; i <= 100; i++) {
                d = CronUtil.parseCronExpression(expression, TimeZone.getDefault()).getNextValidTimeAfter(d);
                System.out.println(CronUtil.getPrettyTime(d.getTime()));
            }
        } catch (NaviException e) {
            e.printStackTrace();
        }

    }
//
//  @Test
//  public void testGetCronExpressionPeriod2() {
//    String expression = "0 0/5 14 * * ?";
//    PeriodUnit period = CronUtil.getCronExpressionPeriod(expression);
//    Assert.assertEquals(period, PeriodUnit.minute);
//  }
//
//  @Test
//  public void testGetCronExpressionPeriod3() {
//    String expression = "0 0-5 14 * * ?";
//    PeriodUnit period = CronUtil.getCronExpressionPeriod(expression);
//    Assert.assertEquals(period, PeriodUnit.minute);
//  }
//
//  @Test
//  public void testGetCronExpressionPeriod4() {
//    String expression = "0 0 12 1/5 * ?";
//    PeriodUnit period = CronUtil.getCronExpressionPeriod(expression);
//    Assert.assertEquals(period, PeriodUnit.day);
//  }

}
