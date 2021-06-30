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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import com.tencent.bk.base.datalab.bksql.exception.FailedOnCheckException;
import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import org.junit.Test;

public class TimeBasedFilterOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testEqualsToFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT t.col FROM tab t WHERE t.thedate = '20080808' and t.a=1",
                new DeParsingMatcher(
                        "SELECT t.col FROM tab AS t WHERE ((t.thedate = '20080808') AND ((t"
                                + ".dtEventTimeStamp >= 1218124800000) AND (t.dtEventTimeStamp < "
                                + "1218211200000))) AND (t.a = 1)"));
    }

    @Test
    public void testNotEqualsToFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate <> '20080808' and a!=1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate <> '20080808') AND ("
                                + "(dtEventTimeStamp > 1218211200000) OR (dtEventTimeStamp < "
                                + "1218124800000))) AND (a <> 1)"));
    }

    @Test
    public void testNotEqualsToFilter2() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate != '20080808' and a!=1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate <> '20080808') AND ("
                                + "(dtEventTimeStamp > 1218211200000) OR (dtEventTimeStamp < "
                                + "1218124800000))) AND (a <> 1)"));
    }

    @Test
    public void testLargerThanFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate > '20080808' and a>1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate > '20080808') AND (dtEventTimeStamp "
                                + "> 1218211199999)) AND (a > 1)"));
    }

    @Test
    public void testLargerThanEqualsFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate >= '20080808' and a >= 1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate >= '20080808') AND (dtEventTimeStamp"
                                + " >= 1218124800000)) AND (a >= 1)"));
    }

    @Test
    public void testRangeFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate >= '20191204' and thedate<='20191204' and a >="
                        + " 1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (((thedate >= '20191204') AND "
                                + "(dtEventTimeStamp >= 1575388800000)) AND ((thedate <= "
                                + "'20191204') AND (dtEventTimeStamp <= 1575475199999))) AND (a "
                                + ">= 1)"));
    }

    @Test
    public void testMinorThanFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate < '20080808' and a < 1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate < '20080808') AND (dtEventTimeStamp "
                                + "< 1218124800000)) AND (a < 1)"));
    }

    @Test
    public void testMinorThanEqualsFilter() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate <= '20080808' and a <= 1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate <= '20080808') AND (dtEventTimeStamp"
                                + " <= 1218211199999)) AND (a <= 1)"));
    }

    @Test
    public void testHourTime() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE dtEventTime = '2008-08-08 12:00:00' and a=1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((dtEventTime = '2008-08-08 12:00:00') AND ("
                                + "(dtEventTimeStamp >= 1218168000000) AND (dtEventTimeStamp < "
                                + "1218168001000))) AND (a = 1)"));
    }


    @Test
    public void testJoin() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT a.col FROM tab a join tab b on(a.id=b.id and a.thedate=b.thedate) WHERE a"
                        + ".dtEventTime = '2008-08-08 12:00:00' and a.c1=1",
                new DeParsingMatcher(
                        "SELECT a.col FROM tab AS a INNER JOIN tab AS b ON ((a.id = b.id) AND (a"
                                + ".thedate = b.thedate)) WHERE ((a.dtEventTime = '2008-08-08 "
                                + "12:00:00') AND ((a.dtEventTimeStamp >= 1218168000000) AND (a"
                                + ".dtEventTimeStamp < 1218168001000))) AND (a.c1 = 1)"));
    }

    @Test
    public void testSelectThedate() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT `if`(thedate>=20200501,'after05') as c1 FROM "
                        + "tab",
                new DeParsingMatcher(
                        "SELECT if((thedate >= 20200501) AND (dtEventTimeStamp >= 1588262400000),"
                                + " 'after05') AS c1 FROM tab"));
    }

    @Test
    public void testBetween1() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate between '20080808' and '20090808' and a > 1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (((dtEventTimeStamp >= 1218124800000) AND "
                                + "(dtEventTimeStamp <= 1249747199999)) AND (thedate BETWEEN "
                                + "'20080808' AND '20090808')) AND (a > 1)"));
    }

    @Test
    public void testBetween2() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "select * from tab where abc between '2020-0810' and '2020-0810'",
                new DeParsingMatcher(
                        "SELECT * FROM tab WHERE abc BETWEEN '2020-0810' AND '2020-0810'"));
    }

    @Test
    public void testBetween3() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "select * from tab where thedate between '2020-0810\n' and '2020-0810'",
                new DeParsingMatcher(
                        "SELECT * FROM tab WHERE thedate BETWEEN '2020-0810\n' AND '2020-0810'"));
    }

    @Test
    public void testBetween4() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "select * from tab where thedate between '2020-08-10' and '2020-0810'",
                new DeParsingMatcher(
                        "SELECT * FROM tab WHERE thedate BETWEEN '2020-08-10' AND '2020-0810'"));
    }

    @Test
    public void testBetween5() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "select * from tab where thedate between '20200810' and '20200810'",
                new DeParsingMatcher(
                        "SELECT * FROM tab WHERE ((dtEventTimeStamp >= 1596988800000) AND "
                                + "(dtEventTimeStamp <= 1597075199999)) AND (thedate BETWEEN "
                                + "'20200810' AND '20200810')"));
    }

    @Test
    public void testBetween6() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "select * from tab where thedate between '20200810\n' and '20200810\r'",
                new DeParsingMatcher(
                        "SELECT * FROM tab WHERE ((dtEventTimeStamp >= 1596988800000) AND "
                                + "(dtEventTimeStamp <= 1597075199999)) AND (thedate BETWEEN "
                                + "'20200810\n' AND '20200810\r')"));
    }

    @Test
    public void testUtcLargerThanFilter() throws Exception {
        System.setProperty("BK_TIMEZONE", "UTC");
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate > '20080808' and a>1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((thedate > '20080808') AND (dtEventTimeStamp "
                                + "> 1218239999999)) AND (a > 1)"));
        System.clearProperty("BK_TIMEZONE");
    }

    @Test(expected = FailedOnCheckException.class)
    public void testIllegalDtEventTime() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE dteventtime > '1218239999999' and a>1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime > '1218239999999' and a>1"));
    }

    @Test(expected = FailedOnCheckException.class)
    public void testIllegalThedate() throws Exception {
        assertThat(
                new TimeBasedFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate > '2020abcd' and a>1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE thedate > '2020abcd' and a>1"));

    }
}
