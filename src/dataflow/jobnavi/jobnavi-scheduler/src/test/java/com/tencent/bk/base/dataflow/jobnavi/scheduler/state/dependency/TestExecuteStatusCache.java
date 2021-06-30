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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.Schedule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyParamType;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyRule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;

public class TestExecuteStatusCache extends TestCase {

    private static final String TEST_SCHEDULE_CLASS_NAME
            = "com.tencent.blueking.dataflow.jobnavi.scheduler.schedule.TestSchedule";

    /**
     * construct test case
     *
     * @throws Exception
     */
    public TestExecuteStatusCache() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.setString(Constants.JOBNAVI_SCHEDULER_SCHEDULE_CLASS, TEST_SCHEDULE_CLASS_NAME);
        ScheduleManager.init(configuration);
        ExecuteStatusCache.setMaxCapacity(5);
        ExecuteStatusCache.destroy();
        //schedule B
        ScheduleInfo b = new ScheduleInfo();
        b.setScheduleId("B");
        Period periodB = new Period();
        periodB.setTimezone("UTC");
        b.setPeriod(periodB);
        //A->B
        DependencyInfo dependencyInfoAToB = new DependencyInfo();
        dependencyInfoAToB.setParentId("A");
        dependencyInfoAToB.setRule(DependencyRule.all_finished);
        dependencyInfoAToB.setType(DependencyParamType.fixed);
        dependencyInfoAToB.setValue("1d");
        List<DependencyInfo> dependencyInfoListB = new ArrayList<>();
        dependencyInfoListB.add(dependencyInfoAToB);
        //D->B
        DependencyInfo dependencyInfoDToB = new DependencyInfo();
        dependencyInfoDToB.setParentId("D");
        dependencyInfoDToB.setRule(DependencyRule.all_finished);
        dependencyInfoDToB.setType(DependencyParamType.fixed);
        dependencyInfoDToB.setValue("1d");
        dependencyInfoListB.add(dependencyInfoDToB);
        //G->B
        DependencyInfo dependencyInfoGToB = new DependencyInfo();
        dependencyInfoGToB.setParentId("G");
        dependencyInfoGToB.setRule(DependencyRule.all_finished);
        dependencyInfoGToB.setType(DependencyParamType.fixed);
        dependencyInfoGToB.setValue("1d");
        dependencyInfoListB.add(dependencyInfoGToB);
        b.setParents(dependencyInfoListB);
        Schedule schedule = ScheduleManager.getSchedule();
        schedule.addSchedule(b);
        //schedule C
        ScheduleInfo c = new ScheduleInfo();
        c.setScheduleId("C");
        Period periodC = new Period();
        periodC.setTimezone("UTC");
        c.setPeriod(periodC);
        //A->C
        DependencyInfo dependencyInfoAToC = new DependencyInfo();
        dependencyInfoAToC.setParentId("A");
        dependencyInfoAToC.setRule(DependencyRule.all_finished);
        dependencyInfoAToC.setType(DependencyParamType.fixed);
        dependencyInfoAToC.setValue("1d");
        List<DependencyInfo> dependencyInfoListC = new ArrayList<>();
        dependencyInfoListC.add(dependencyInfoAToC);
        //E->C
        DependencyInfo dependencyInfoEToC = new DependencyInfo();
        dependencyInfoEToC.setParentId("E");
        dependencyInfoEToC.setRule(DependencyRule.all_finished);
        dependencyInfoEToC.setType(DependencyParamType.fixed);
        dependencyInfoEToC.setValue("1d");
        dependencyInfoListC.add(dependencyInfoEToC);
        //F->C
        DependencyInfo dependencyInfoFToC = new DependencyInfo();
        dependencyInfoFToC.setParentId("F");
        dependencyInfoFToC.setRule(DependencyRule.all_finished);
        dependencyInfoFToC.setType(DependencyParamType.fixed);
        dependencyInfoFToC.setValue("1d");
        dependencyInfoListC.add(dependencyInfoFToC);
        //G->C
        DependencyInfo dependencyInfoGToC = new DependencyInfo();
        dependencyInfoGToC.setParentId("G");
        dependencyInfoGToC.setRule(DependencyRule.all_finished);
        dependencyInfoGToC.setType(DependencyParamType.fixed);
        dependencyInfoGToC.setValue("1d");
        dependencyInfoListC.add(dependencyInfoGToC);
        //H->C
        DependencyInfo dependencyInfoHToC = new DependencyInfo();
        dependencyInfoHToC.setParentId("H");
        dependencyInfoHToC.setRule(DependencyRule.all_finished);
        dependencyInfoHToC.setType(DependencyParamType.fixed);
        dependencyInfoHToC.setValue("1d");
        dependencyInfoListC.add(dependencyInfoHToC);
        c.setParents(dependencyInfoListC);
        schedule.addSchedule(c);
        //schedule J
        ScheduleInfo j = new ScheduleInfo();
        j.setScheduleId("J");
        Period periodJ = new Period();
        periodJ.setTimezone("UTC");
        j.setPeriod(periodJ);
        //I->J
        DependencyInfo dependencyInfoIToJ = new DependencyInfo();
        dependencyInfoIToJ.setParentId("I");
        dependencyInfoIToJ.setRule(DependencyRule.all_finished);
        dependencyInfoIToJ.setType(DependencyParamType.fixed);
        dependencyInfoIToJ.setValue("1d");
        List<DependencyInfo> dependencyInfoListJ = new ArrayList<>();
        dependencyInfoListJ.add(dependencyInfoIToJ);
        j.setParents(dependencyInfoListJ);
        schedule.addSchedule(j);
        //schedule L
        ScheduleInfo l = new ScheduleInfo();
        l.setScheduleId("L");
        Period periodL = new Period();
        periodL.setTimezone("UTC");
        l.setPeriod(periodL);
        //K->L
        DependencyInfo dependencyInfoKToL = new DependencyInfo();
        dependencyInfoKToL.setParentId("K");
        dependencyInfoKToL.setRule(DependencyRule.all_finished);
        dependencyInfoKToL.setType(DependencyParamType.fixed);
        dependencyInfoKToL.setValue("1d");
        List<DependencyInfo> dependencyInfoListL = new ArrayList<>();
        dependencyInfoListL.add(dependencyInfoKToL);
        l.setParents(dependencyInfoListL);
        schedule.addSchedule(l);
        //schedule A
        ScheduleInfo a = new ScheduleInfo();
        a.setScheduleId("A");
        schedule.addSchedule(a);
        //schedule D
        ScheduleInfo d = new ScheduleInfo();
        d.setScheduleId("D");
        schedule.addSchedule(d);
        //schedule E
        ScheduleInfo e = new ScheduleInfo();
        e.setScheduleId("E");
        schedule.addSchedule(e);
        //schedule F
        ScheduleInfo f = new ScheduleInfo();
        f.setScheduleId("F");
        schedule.addSchedule(f);
        //schedule G
        ScheduleInfo g = new ScheduleInfo();
        g.setScheduleId("G");
        schedule.addSchedule(g);
        //schedule H
        ScheduleInfo h = new ScheduleInfo();
        h.setScheduleId("H");
        schedule.addSchedule(h);
        //schedule I
        ScheduleInfo i = new ScheduleInfo();
        i.setScheduleId("I");
        schedule.addSchedule(i);
        //schedule K
        ScheduleInfo k = new ScheduleInfo();
        k.setScheduleId("K");
        schedule.addSchedule(k);
    }

    public void setUp() throws Exception {
        super.setUp();
        ExecuteStatusCache.setMaxCapacity(5);
        ExecuteStatusCache.destroy();
        ExecuteStatusCache.putStatus("A", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("A", 10000000000L, "B", 10080000000L, 10000000000L);
        ExecuteStatusCache.putReference("A", 10000000000L, "C", 10080000000L, 10000000000L);
        ExecuteStatusCache.putStatus("D", 10000000000L, TaskStatus.failed);
        ExecuteStatusCache.putReference("D", 10000000000L, "B", 10080000000L, 10000000000L);
        ExecuteStatusCache.putStatus("E", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("E", 10000000000L, "C", 10080000000L, 10000000000L);
        ExecuteStatusCache.putStatus("F", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("F", 10000000000L, "C", 10080000000L, 10000000000L);
    }

    public void testGetStatus() {
        TaskStatus status = ExecuteStatusCache.getStatus("D", 10000000000L);
        assertEquals(TaskStatus.failed, status);
    }

    public void testPut() throws NaviException {
        ExecuteStatusCache.putStatus("G", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("G", 10000000000L, "B", 10080000000L, 10000000000L);
        TaskStatus status = ExecuteStatusCache.getStatus("G", 10000000000L);
        assertEquals(TaskStatus.finished, status);
        status = ExecuteStatusCache.getStatus("A", 10000000000L);
        assertEquals(TaskStatus.finished, status);
        ExecuteStatusCache.putReference("G", 10000000000L, "C", 10080000000L, 10000000000L);
        status = ExecuteStatusCache.getStatus("G", 10000000000L);
        assertEquals(TaskStatus.finished, status);
        status = ExecuteStatusCache.getStatus("A", 10000000000L);
        assertEquals(5, ExecuteStatusCache.size());
        //the eldest 2 references should be remove from cache
        assertEquals(TaskStatus.none, status);
    }

    public void testUpdate() {
        ExecuteStatusCache.updateStatus("A", 10000000000L, TaskStatus.failed_succeeded);
        TaskStatus status = ExecuteStatusCache.getStatus("A", 10000000000L);
        assertEquals(status, TaskStatus.failed_succeeded);
    }

    /**
     * test remove reference
     * @throws NaviException
     */
    public void testRemoveReference() throws NaviException {
        ExecuteStatusCache.removeReference("B", 10000000000L);
        TaskStatus status = ExecuteStatusCache.getStatus("D", 10000000000L);
        //the reference is marked as cold, but not removed from cache yet
        assertEquals(TaskStatus.failed, status);
        ExecuteStatusCache.putStatus("H", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("H", 10000000000L, "C", 10080000000L, 10000000000L);
        ExecuteStatusCache.putStatus("I", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("I", 10000000000L, "J", 10080000000L, 10000000000L);
        assertEquals(5, ExecuteStatusCache.size());
        status = ExecuteStatusCache.getStatus("D", 10000000000L);
        //size of cache reach the max capacity, remove cold references first
        assertEquals(TaskStatus.none, status);
        //test remove and put again
        status = ExecuteStatusCache.getStatus("A", 10000000000L);
        assertEquals(TaskStatus.finished, status);
        status = ExecuteStatusCache.getStatus("I", 10000000000L);
        assertEquals(TaskStatus.finished, status);
        ExecuteStatusCache.removeReference("J", 10000000000L);
        ExecuteStatusCache.putReference("I", 10000000000L, "J", 10080000000L, 10000000000L);
        ExecuteStatusCache.putStatus("K", 10000000000L, TaskStatus.finished);
        ExecuteStatusCache.putReference("K", 10000000000L, "L", 10080000000L, 10000000000L);
        status = ExecuteStatusCache.getStatus("I", 10000000000L);
        assertEquals(TaskStatus.none, status);
        status = ExecuteStatusCache.getStatus("A", 10000000000L);
        assertEquals(TaskStatus.finished, status);
    }
}
