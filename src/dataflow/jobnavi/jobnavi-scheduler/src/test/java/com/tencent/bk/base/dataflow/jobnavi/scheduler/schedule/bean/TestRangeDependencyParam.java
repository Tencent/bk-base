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

public class TestRangeDependencyParam extends TestCase {

    private RangeDependencyParam testParm;
    private long baseTime;

    public void testGetDependenceDataTimeRange1() {
        baseTime = 1601033144000L; //2020/09/25 19:25:44
        testParm = new RangeDependencyParam("1H~$NOWH", baseTime, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600963200000L, testParm.getDependenceStartTime());
        assertEquals(1601031600000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange2() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        testParm = new RangeDependencyParam("1H~$NOWH", baseTime, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600876800000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }

    public void testGetDependenceDataTimeRange3() {
        baseTime = 1600964744000L; //2020/09/25 00:25:44
        testParm = new RangeDependencyParam("1H~0H", baseTime, TimeZone.getTimeZone("Asia/Shanghai"));
        assertEquals(1600876800000L, testParm.getDependenceStartTime());
        assertEquals(1600963200000L, testParm.getDependenceEndTime());
    }
}
