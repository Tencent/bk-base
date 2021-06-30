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

package com.tencent.bk.base.dataflow.spark;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class BatchTimeTest {

    @Before
    public void prepare() {

    }

    @Test
    public void testBatchTimeDelta() {
        BatchTimeDelta testTimeDelta1 = new BatchTimeDelta("10D");
        assert testTimeDelta1.getHourExceptMonth() == 240;

        BatchTimeDelta testTimeDelta2 = new BatchTimeDelta("10D+20H");
        assert testTimeDelta2.getHourExceptMonth() == 260;

        BatchTimeDelta testTimeDelta3 = new BatchTimeDelta("3M");
        assert testTimeDelta3.getMonth() == 3;

        BatchTimeDelta testTimeDelta4 = new BatchTimeDelta("3M+20H");
        assert testTimeDelta4.getHourExceptMonth() == 20;
        assert testTimeDelta4.getMonth() == 3;
    }

    @Test
    public void testBatchTimePlus() {
        BatchTimeStamp testTimeStamp1 = new BatchTimeStamp(1611574080000L, "Asia/Shanghai");
        BatchTimeDelta testTimeDelta1 = new BatchTimeDelta("10D");

        BatchTimeStamp testTimeStamp2 = testTimeStamp1.minus(testTimeDelta1);
        assert testTimeStamp2.getTimeInMilliseconds() == 1610710080000L;
        assert testTimeStamp1.getTimeInMilliseconds() == 1611574080000L;

        BatchTimeDelta testTimeDelta2 = new BatchTimeDelta("2M");
        BatchTimeStamp testTimeStamp3 = testTimeStamp1.minus(testTimeDelta2);
        System.out.println(testTimeStamp1.getTimeInMilliseconds());
        assert testTimeStamp1.getTimeInMilliseconds() == 1611574080000L;
        assert testTimeStamp3.getTimeInMilliseconds() == 1606303680000L;

    }

    @Test
    public void testBatchTimeMinus() {
        BatchTimeStamp testTimeStamp1 = new BatchTimeStamp(1611574080000L, "Asia/Shanghai");
        BatchTimeDelta testTimeDelta1 = new BatchTimeDelta("10D");

        BatchTimeStamp testTimeStamp2 = testTimeStamp1.plus(testTimeDelta1);
        assert testTimeStamp2.getTimeInMilliseconds() == 1612438080000L;
        assert testTimeStamp1.getTimeInMilliseconds() == 1611574080000L;

        BatchTimeDelta testTimeDelta2 = new BatchTimeDelta("2M");
        BatchTimeStamp testTimeStamp3 = testTimeStamp1.plus(testTimeDelta2);
        assert testTimeStamp3.getTimeInMilliseconds() == 1616671680000L;
        assert testTimeStamp1.getTimeInMilliseconds() == 1611574080000L;
    }

}
