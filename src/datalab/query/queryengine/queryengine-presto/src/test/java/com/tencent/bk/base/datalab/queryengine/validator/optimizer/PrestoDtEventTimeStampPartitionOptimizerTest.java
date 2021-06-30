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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import org.junit.Test;

public class PrestoDtEventTimeStampPartitionOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testSql1() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp = 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit = 2020120218) AND "
                                + "(dteventtimestamp = 1606905267000)"));
    }

    @Test
    public void testTimeZone() throws Exception {
        System.setProperty("BK_TIMEZONE", "Asia/Tokyo");
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp > 1606905267000 and a>1",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE ((dt_par_unit >= 2020120219) "
                                + "AND (dteventtimestamp > 1606905267000)) AND (a > 1)"));
        System.clearProperty("BK_TIMEZONE");
    }

    @Test
    public void testSql2() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp != 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtimestamp <> 1606905267000"));
    }

    @Test
    public void testSql3() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp > 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2020120218)"
                                + " AND (dteventtimestamp > 1606905267000)"));
    }

    @Test
    public void testSql4() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp >= 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2020120218) "
                                + "AND (dteventtimestamp >= 1606905267000)"));
    }

    @Test
    public void testSql5() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp < 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2020120218) "
                                + "AND (dteventtimestamp < 1606905267000)"));
    }

    @Test
    public void testSql6() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtimestamp <= 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2020120218) "
                                + "AND (dteventtimestamp <= 1606905267000)"));
    }

    @Test
    public void testSql7() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT a.col FROM tab a WHERE a.dteventtimestamp between 1606905267000 and "
                        + "1606916390000",
                new DeParsingMatcher(
                        "SELECT a.col FROM tab AS a "
                                + "WHERE ((a.dt_par_unit >= 2020120218) "
                                + "AND (a.dteventtimestamp >= 1606905267000)) "
                                + "AND ((a.dt_par_unit <= 2020120221) "
                                + "AND (a.dteventtimestamp <= 1606916390000))"));
    }

    @Test
    public void testSql8() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT a.id FROM table_1 a join "
                        + "table_1 b on(a.id=b.id and a.dteventtimestamp=b"
                        + ".thedate2) where a.dteventtimestamp>1606905267000",
                new DeParsingMatcher(
                        "SELECT a.id FROM table_1 AS a "
                                + "INNER JOIN table_1 AS b "
                                + "ON ((a.id = b.id) AND (a.dteventtimestamp = b.thedate2)) "
                                + "WHERE (a.dt_par_unit >= 2020120218) "
                                + "AND (a.dteventtimestamp > 1606905267000)"));
    }

    @Test
    public void testSql10() throws Exception {
        assertThat(
                new PrestoDtEventTimeStampPartitionOptimizer(),
                "SELECT if(dteventtimestamp>=1606905267000,'after05') as c1 FROM "
                        + "table_1",
                new DeParsingMatcher(
                        "SELECT if((dt_par_unit >= 2020120218) "
                                + "AND (dteventtimestamp >= 1606905267000), 'after05') AS c1 "
                                + "FROM table_1"));
    }
}