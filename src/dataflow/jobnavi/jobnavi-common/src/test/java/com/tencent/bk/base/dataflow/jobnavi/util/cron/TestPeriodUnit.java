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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;

public class TestPeriodUnit {

    @Test
    public void testGetPeriodTimestampRange1() {
        Long[] range = PeriodUnit.minute.getPeriodTimestampRange(1516624112000L, TimeZone.getDefault());
        Assert.assertEquals(range[0].longValue(), 1516624080000L);
        Assert.assertEquals(range[1].longValue(), 1516624139999L);
    }


    @Test
    public void testGetPeriodTimestampRange2() {
        Long[] range = PeriodUnit.hour.getPeriodTimestampRange(1516624112000L, TimeZone.getDefault());
        Assert.assertEquals(range[0].longValue(), 1516622400000L);
        Assert.assertEquals(range[1].longValue(), 1516625999999L);
    }


    @Test
    public void testGetPeriodTimestampRange3() {
        Long[] range = PeriodUnit.day.getPeriodTimestampRange(1516624112000L, TimeZone.getDefault());
        Assert.assertEquals(range[0].longValue(), 1516550400000L);
        Assert.assertEquals(range[1].longValue(), 1516636799999L);
    }


    @Test
    public void testCalc() {
        Calendar ca = Calendar.getInstance();
        ca.add(Calendar.DATE, -3);// 日期减1
        Date resultDate = ca.getTime(); // 结果
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        System.out.println(sdf.format(resultDate));

    }
}
