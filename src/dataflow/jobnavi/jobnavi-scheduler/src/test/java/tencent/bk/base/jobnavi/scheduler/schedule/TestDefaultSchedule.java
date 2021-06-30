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

package tencent.bk.base.jobnavi.scheduler.schedule;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyParamType;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyRule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tencent.bk.base.jobnavi.scheduler.metadata.mysql.util.MetaDataTestUtil;

public class TestDefaultSchedule {

    Schedule schedule;
    private Configuration conf;

    /**
     * init test
     */
    @Before
    public void before() {
        try {
            conf = MetaDataTestUtil.initConfig();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
        try {
            MetaDataManager.init(conf);
            ScheduleManager.init(conf);
            TaskStateManager.init(conf);
        } catch (NaviException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        schedule = ScheduleManager.getSchedule();

        ScheduleInfo scheduleInfo1 = new ScheduleInfo();
        scheduleInfo1.setScheduleId("test_1");
        Period period1 = new Period();
        period1.setPeriodUnit(PeriodUnit.hour);
        period1.setFirstScheduleTime(new Date(1519920720000L));
        period1.setFrequency(1);
        scheduleInfo1.setPeriod(period1);

        ScheduleInfo scheduleInfo2 = new ScheduleInfo();
        scheduleInfo2.setScheduleId("test_2");
        Period period2 = new Period();
        period2.setPeriodUnit(PeriodUnit.hour);
        period2.setFirstScheduleTime(new Date(1519920720000L));
        period2.setFrequency(1);
        scheduleInfo2.setPeriod(period2);

        DependencyInfo dependencyInfo2 = new DependencyInfo();
        dependencyInfo2.setValue("1h");
        dependencyInfo2.setType(DependencyParamType.fixed);
        dependencyInfo2.setRule(DependencyRule.all_finished);
        dependencyInfo2.setParentId("test_1");
        List<DependencyInfo> parents = new ArrayList<>();
        scheduleInfo2.setParents(parents);

        ScheduleInfo scheduleInfo3 = new ScheduleInfo();
        scheduleInfo3.setScheduleId("test_3");
        Period period3 = new Period();
        period3.setCronExpression("0 0 1-6 * * ?");
        period3.setFirstScheduleTime(new Date(1520266920000L));
        period3.setTimezone(TimeZone.getDefault());
        scheduleInfo3.setPeriod(period3);

        ScheduleInfo scheduleInfo4 = new ScheduleInfo();
        scheduleInfo4.setScheduleId("test_4");
        Period period4 = new Period();
        period4.setPeriodUnit(PeriodUnit.hour);
        period4.setFirstScheduleTime(new Date(1521130680000L));
        period4.setTimezone(TimeZone.getDefault());
        period4.setFrequency(1);
        scheduleInfo4.setPeriod(period4);

        ScheduleInfo scheduleInfo5 = new ScheduleInfo();
        scheduleInfo5.setScheduleId("test_5");
        Period period5 = new Period();
        period5.setPeriodUnit(PeriodUnit.hour);
        period5.setFirstScheduleTime(new Date(1534350060000L));
        period5.setTimezone(TimeZone.getDefault());
        period5.setFrequency(6);
        scheduleInfo5.setPeriod(period5);

        try {
            schedule.addSchedule(scheduleInfo1);
            schedule.addSchedule(scheduleInfo2);
            schedule.addSchedule(scheduleInfo3);
            schedule.addSchedule(scheduleInfo4);
            schedule.addSchedule(scheduleInfo5);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCalcNextScheduleTime1() {
        try {
            long nextScheduleTime = schedule.calcNextScheduleTime("test_1", 1519986109828L);
            Assert.assertEquals(1519989120000L, nextScheduleTime);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCalcNextScheduleTime2() {
        try {
            long nextScheduleTime = schedule.calcNextScheduleTime("test_1", 1519989120000L);
            Assert.assertEquals(1519989120000L, nextScheduleTime);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCalcNextScheduleTime3() {
        try {
            long nextScheduleTime = schedule.calcNextScheduleTime("test_3", 1520805600000L);
            Assert.assertEquals(1520805600000L, nextScheduleTime);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCalcNextScheduleTime4() {
        try {
            long nextScheduleTime = schedule.calcNextScheduleTime("test_4", 1521129600000L);
            Assert.assertEquals(1521130680000L, nextScheduleTime);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCalcNextScheduleTime5() {
        try {
            long now = System.currentTimeMillis();
            long nextScheduleTime = schedule.calcNextScheduleTime("test_5", now);
            Assert.assertEquals(1521130680000L, nextScheduleTime);
        } catch (NaviException e) {
            Assert.fail(e.getMessage());
        }
    }
}
