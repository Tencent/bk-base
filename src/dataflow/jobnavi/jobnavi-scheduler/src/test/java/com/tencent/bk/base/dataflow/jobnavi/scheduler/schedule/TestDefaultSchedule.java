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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import java.util.Date;
import java.util.TimeZone;
import junit.framework.TestCase;

public class TestDefaultSchedule extends TestCase {

    private DefaultSchedule schedule;
    private ScheduleInfo scheduleInfo;

    /**
     * Sets up the fixture, for example, open a network connection.
     * This method is called before a test is executed.
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        schedule = new DefaultSchedule();
        scheduleInfo = new ScheduleInfo();
        scheduleInfo.setScheduleId("test_schedule");
        scheduleInfo.setPeriod(new Period());
        scheduleInfo.getPeriod().setTimezone(TimeZone.getTimeZone("Asia/Shanghai"));
        scheduleInfo.getPeriod().setFirstScheduleTime(new Date(1598891144000L)); //2020/09/01 00:25:44
    }

    /**
     * test case 1
     * @throws NaviException
     */
    public void testCalcNextDataTime1() throws NaviException {
        scheduleInfo.getPeriod().setFrequency(1);
        scheduleInfo.getPeriod().setPeriodUnit(PeriodUnit.hour);
        scheduleInfo.setDataTimeOffset("1H");
        long before = 1601481600000L; //2020/10/01 00:00:00.000
        long nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601481600000L, nextDataTime); //2020/10/01 00:00:00.000
        before = 1601481600000L + 1; //2020/10/01 00:00:00.001
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601485200000L, nextDataTime); //2020/10/01 01:00:00.000
        scheduleInfo.getPeriod().setPeriodUnit(PeriodUnit.day);
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601564400000L, nextDataTime); //2020/10/01 23:00:00.000
        scheduleInfo.getPeriod().setPeriodUnit(PeriodUnit.week);
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601910000000L, nextDataTime); //2020/10/05 23:00:00.000
        scheduleInfo.getPeriod().setPeriodUnit(PeriodUnit.month);
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1604070000000L, nextDataTime); //2020/10/30 23:00:00.000
    }

    public void testCalcNextDataTime2() throws NaviException {
        scheduleInfo.getPeriod().setFrequency(2);
        scheduleInfo.getPeriod().setPeriodUnit(PeriodUnit.hour);
        scheduleInfo.setDataTimeOffset("1H");
        long before = 1601481600000L; //2020/10/01 00:00:00.000
        long nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601485200000L, nextDataTime); //2020/10/01 01:00:00.000
        scheduleInfo.setDataTimeOffset("1H+1H");
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601481600000L, nextDataTime); //2020/10/01 00:00:00.000
    }

    public void testCalcNextDataTime3() throws NaviException {
        scheduleInfo.getPeriod().setCronExpression("0 13 0/1 * * ?");
        scheduleInfo.setDataTimeOffset("1H");
        long before = 1601481600000L; //2020/10/01 00:00:00.000
        long nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601481600000L, nextDataTime); //2020/10/01 01:00:00.000
        before = 1601481600000L + 1; //2020/10/01 00:00:00.001
        nextDataTime = schedule.calcNextDataTime(scheduleInfo, before);
        assertEquals(1601485200000L, nextDataTime); //2020/10/01 01:00:00.000
    }
}
