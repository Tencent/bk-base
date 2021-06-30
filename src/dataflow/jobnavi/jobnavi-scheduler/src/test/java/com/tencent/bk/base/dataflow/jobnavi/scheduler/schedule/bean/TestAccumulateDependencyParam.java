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

import java.util.TimeZone;
import junit.framework.TestCase;

public class TestAccumulateDependencyParam extends TestCase {

    private AccumulateDependencyParam testParm;
    private long baseTime;

    public void testGetDependenceDataTimeRange1() {
        baseTime = 1601033144000L; //2020/09/25 19:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st hour to 24th hour (00 ~ 23 o'clock)
        testParm = new AccumulateDependencyParam("1598891144000:24H:1H~24H", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600963200000L, testParm.getDependenceStartTime());
        assertEquals(1601031600000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange2() {
        baseTime = 1601033144000L; //2020/09/25 19:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st hour to the last hour (00 ~ 23 o'clock)
        testParm = new AccumulateDependencyParam("1598891144000:24H:1H~-1H", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600963200000L, testParm.getDependenceStartTime());
        assertEquals(1601031600000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange3() {
        baseTime = 1601033144000L; //2020/09/25 19:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st hour to 10th hour (00 ~ 09 o'clock)
        testParm = new AccumulateDependencyParam("1598891144000:24H:1H~10H", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600963200000L, testParm.getDependenceStartTime());
        assertEquals(1600999200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange4() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st hour to 24th hour (00 ~ 23 o'clock)
        testParm = new AccumulateDependencyParam("1598891144000:24H:1H~24H", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600876800000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange5() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st hour to the last hour (00 ~ 23 o'clock)
        testParm = new AccumulateDependencyParam("1598891144000:24H:1H~-1H", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600876800000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange6() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44 Fri
        //accumulate start base:2020/09/14 00:25:44, 1st day to 7th hour (Mon ~ Sun)
        testParm = new AccumulateDependencyParam("1600014344000:7d:1d~7d", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600617600000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange7() {
        baseTime = 1601223944000L; //2020/09/28 00:25:44 Mon
        //accumulate start base:2020/09/14 00:25:44, 1st day to 7th hour (Mon ~ Sun)
        testParm = new AccumulateDependencyParam("1600014344000:7d:1d~7d", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600617600000L, testParm.getDependenceStartTime());
        assertEquals(1601222400000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange8() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st day to the last day of month
        testParm = new AccumulateDependencyParam("1598891144000:1M:1d~-1d", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1598889600000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange9() {
        baseTime = 1601483144000L; //2020/10/01 00:25:44
        //accumulate start base:2020/09/01 00:25:44, 1st day to the last day of month
        testParm = new AccumulateDependencyParam("1598891144000:1M:1d~-1d", baseTime,
                                                 TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1598889600000L, testParm.getDependenceStartTime());
        assertEquals(1601481600000L, testParm.getDependenceEndTime());
    }
}
