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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean;

import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import java.util.TimeZone;
import junit.framework.TestCase;

public class TestFixedDependencyParam extends TestCase {

    private FixedDependencyParam testParm;
    private long baseTime;

    public void testGetDependenceDataTimeRange1() {
        baseTime = 1601033144000L; //2020/09/25 19:25:44
        testParm = new FixedDependencyParam("1H", baseTime, TimeZone.getTimeZone("Asia/Shanghai")); //1 hour
        assertEquals(1601028000000L, testParm.getDependenceStartTime());
        assertEquals(1601031600000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange2() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        testParm = new FixedDependencyParam("1d", baseTime, TimeZone.getTimeZone("Asia/Shanghai")); //1 day
        assertEquals(1600876800000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange3() {
        baseTime = 1600014344000L; //2020/09/14 00:25:44
        testParm = new FixedDependencyParam("1W", baseTime, TimeZone.getTimeZone("Asia/Shanghai")); //1 week
        assertEquals(1599408000000L, testParm.getDependenceStartTime());
        assertEquals(1600012800000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange4() {
        baseTime = 1601483144000L; //2020/10/01 00:25:44
        testParm = new FixedDependencyParam("1M", baseTime, TimeZone.getTimeZone("Asia/Shanghai")); //1 month
        assertEquals(1598889600000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
    }

    /**
     * test get dependence data time range case 5
     */
    public void testGetDependenceDataTimeRange5() {
        Period parentPeriod = new Period();
        parentPeriod.setFrequency(2);
        parentPeriod.setPeriodUnit(PeriodUnit.hour);
        baseTime = 1601483144000L; //2020/10/01 00:25:44
        testParm = new FixedDependencyParam("2c", baseTime, TimeZone.getTimeZone("Asia/Shanghai"),
                                            parentPeriod); //2 parent cycles (4 hours)
        assertEquals(1601467200000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
        parentPeriod.setPeriodUnit(PeriodUnit.day);
        testParm = new FixedDependencyParam("2c", baseTime, TimeZone.getTimeZone("Asia/Shanghai"),
                                            parentPeriod); //2 parent cycles (4 days)
        assertEquals(1601136000000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
        parentPeriod.setPeriodUnit(PeriodUnit.week);
        testParm = new FixedDependencyParam("1c", baseTime, TimeZone.getTimeZone("Asia/Shanghai"),
                                            parentPeriod); //1 parent cycles (2 weeks)
        assertEquals(1600272000000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
        parentPeriod.setPeriodUnit(PeriodUnit.month);
        testParm = new FixedDependencyParam("3c", baseTime, TimeZone.getTimeZone("Asia/Shanghai"),
                                            parentPeriod); //3 parent cycles (6 months)
        assertEquals(1585670400000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
    }
}
