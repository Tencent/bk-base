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

import com.tencent.bk.base.datalab.bksql.constant.Constants;
import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Test;

public class AbstractColumnReplacementOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testDtEventTimeSql1() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime = '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit = 2019112123) AND (dteventtime = "
                                + "'2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql2() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime != '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime <> '2019-11-21 23:59:59'"));
    }

    @Test
    public void testDtEventTimeSql3() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime > '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2019112123) AND (dteventtime >"
                                + " '2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql4() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime >= '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2019112123) AND (dteventtime "
                                + ">= '2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql5() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime < '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2019112123) AND (dteventtime <"
                                + " '2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql6() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT col FROM tab WHERE dteventtime <= '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2019112123) AND (dteventtime "
                                + "<= '2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql7() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT a.col FROM tab a WHERE a.dteventtime between '2019-11-21 \n00:00:00' and "
                        + "'2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.col FROM tab AS a WHERE ((a.dt_par_unit >= 2019112100) AND (a"
                                + ".dteventtime >= '2019-11-21 \n00:00:00')) AND ((a.dt_par_unit <="
                                + " 2019112123) AND (a.dteventtime <= '2019-11-21 23:59:59'))"));
    }

    @Test
    public void testDtEventTimeSql8() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.dteventtime=b"
                        + ".dteventtime) where a.dteventtime='2019-11-21 23:59:59' and b"
                        + ".dteventtime='2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".dteventtime = b.dteventtime)) WHERE ((a.dt_par_unit = "
                                + "2019112123) AND (a.dteventtime = '2019-11-21 23:59:59')) AND ("
                                + "(b.dt_par_unit = 2019112123) AND (b.dteventtime = '2019-11-21 "
                                + "23:59:59'))"));
    }

    @Test
    public void testDtEventTimeSql9() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.dteventtime=b"
                        + ".thedate2) where a.dteventtime>'2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".dteventtime = b.thedate2)) WHERE (a.dt_par_unit >= "
                                + "2019112123) AND (a.dteventtime > '2019-11-21 23:59:59')"));
    }

    @Test
    public void testDtEventTimeSql10() throws Exception {
        assertThat(
                getDtEventTimePartitionOp(),
                "SELECT if(dteventtime>='2020-05-01 00:00:00','after05') as c1 FROM "
                        + "tab",
                new DeParsingMatcher(
                        "SELECT if((dt_par_unit >= 2020050100) AND (dteventtime >= '2020-05-01 "
                                + "00:00:00'), 'after05') AS c1 FROM "
                                + "tab"));
    }

    @Test
    public void testDtEventTimeStampSql1() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp = 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit = 2020120218) AND (dteventtimestamp = 1606905267000)"));
    }

    @Test
    public void testTimeZone() throws Exception {
        System.setProperty("BK_TIMEZONE", "Asia/Tokyo");
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp > 1606905267000 and a>1",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE ((dt_par_unit >= 2020120219) "
                                + "AND (dteventtimestamp > 1606905267000)) "
                                + "AND (a > 1)"));
        System.clearProperty("BK_TIMEZONE");
    }

    @Test
    public void testDtEventTimeStampSql2() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp != 1606905267000",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtimestamp <> 1606905267000"));
    }

    @Test
    public void testDtEventTimeStampSql3() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp > 1606905267000",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE (dt_par_unit >= 2020120218) "
                                + "AND (dteventtimestamp > 1606905267000)"));
    }

    @Test
    public void testDtEventTimeStampSql4() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp >= 1606905267000",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE (dt_par_unit >= 2020120218) "
                                + "AND (dteventtimestamp >= 1606905267000)"));
    }

    @Test
    public void testDtEventTimeStampSql5() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp < 1606905267000",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE (dt_par_unit <= 2020120218) "
                                + "AND (dteventtimestamp < 1606905267000)"));
    }

    @Test
    public void testDtEventTimeStampSql6() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab WHERE dteventtimestamp <= 1606905267000",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE (dt_par_unit <= 2020120218) "
                                + "AND (dteventtimestamp <= 1606905267000)"));
    }

    @Test
    public void testDtEventTimeStampSql7() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT a.col FROM tab a WHERE a.dteventtimestamp between 1606905267000 and 1606916390000",
                new DeParsingMatcher(
                        "SELECT a.col "
                                + "FROM tab AS a"
                                + " WHERE ((a.dt_par_unit >= 2020120218) "
                                + "AND (a.dteventtimestamp >= 1606905267000)) "
                                + "AND ((a.dt_par_unit <= 2020120221) "
                                + "AND (a.dteventtimestamp <= 1606916390000))"));
    }

    @Test
    public void testDtEventTimeStampSql8() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.dteventtimestamp=b"
                        + ".thedate2) where a.dteventtimestamp>1606905267000",
                new DeParsingMatcher(
                        "SELECT a.id "
                                + "FROM tab AS a "
                                + "INNER JOIN tab AS b "
                                + "ON ((a.id = b.id) "
                                + "AND (a.dteventtimestamp = b.thedate2)) "
                                + "WHERE (a.dt_par_unit >= 2020120218) "
                                + "AND (a.dteventtimestamp > 1606905267000)"));
    }

    @Test
    public void testDtEventTimeStampSql10() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT if(dteventtimestamp>=1606905267000,'after05') as c1 FROM "
                        + "tab",
                new DeParsingMatcher(
                        "SELECT if((dt_par_unit >= 2020120218) "
                                + "AND (dteventtimestamp >= 1606905267000), 'after05') AS c1 "
                                + "FROM tab"));
    }

    @Test
    public void testDtEventTimeStampEqualsFilter() throws Exception {
        assertThat(
                getDtEventTimeStampPartitionOp(),
                "SELECT col FROM tab where dteventtimestamp = 1607331600000",
                new DeParsingMatcher(
                        "SELECT col "
                                + "FROM tab "
                                + "WHERE (dt_par_unit = 2020120717) "
                                + "AND (dteventtimestamp = 1607331600000)"));
    }

    private AbstractColumnReplacementOptimizer getDtEventTimePartitionOp() {
        return new AbstractColumnReplacementOptimizer(Constants.DTEVENTTIME, Constants.DTPARUNIT, true, true) {
            @Override
            protected String getReplacementColumnValue(String strVal1) {
                LocalDateTime localDateTime = LocalDateTime
                        .parse(strVal1, Constants.DATE_TIME_FORMATTER_FULL);
                return localDateTime.format(Constants.DATE_TIME_FORMATTER_HOUR);
            }
        };
    }

    private AbstractColumnReplacementOptimizer getDtEventTimeStampPartitionOp() {
        return new AbstractColumnReplacementOptimizer(Constants.DTEVENTTIMESTAMP, Constants.DTPARUNIT, true, true) {
            @Override
            protected String getReplacementColumnValue(String strVal) {
                ZonedDateTime zonedDateTime = ZonedDateTime
                        .ofInstant(Instant.ofEpochMilli(Long.parseLong(strVal)),
                                ZoneId.of(System.getProperty(Constants.BK_TIMEZONE, Constants.ASIA_SHANGHAI)));
                return zonedDateTime.format(Constants.DATE_TIME_FORMATTER_HOUR);
            }
        };
    }
}
