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
import com.tencent.bk.base.datalab.bksql.validator.optimizer.ReservedKeyOptimizer;
import com.tencent.bk.base.datalab.queryengine.validator.constant.PrestoTestConstants;
import java.util.Arrays;
import org.junit.Test;

public class PrestoTheDatePartitionOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testSql1() throws Exception {
        assertThat(
                Arrays.asList(new PrestoTheDatePartitionOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT col FROM tab WHERE thedate = '20080808'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2008080800) AND (dt_par_unit "
                                + "<= 2008080823)"));
    }

    @Test
    public void testSql2() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate != '20080808'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit > 2008080823) OR (dt_par_unit < "
                                + "2008080800)"));
    }

    @Test
    public void testSql3() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate > '20191121'",
                new DeParsingMatcher("SELECT col FROM tab WHERE dt_par_unit > 2019112123"));
    }

    @Test
    public void testSql4() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate >= '20191121'",
                new DeParsingMatcher("SELECT col FROM tab WHERE dt_par_unit >= 2019112100"));
    }

    @Test
    public void testSql5() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate < '20191121'",
                new DeParsingMatcher("SELECT col FROM tab WHERE dt_par_unit < 2019112100"));
    }

    @Test
    public void testSql6() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate <= '20191121'",
                new DeParsingMatcher("SELECT col FROM tab WHERE dt_par_unit <= 2019112123"));
    }

    @Test
    public void testSql7() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab WHERE thedate between '20191121' and '20191125'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2019112100) AND (dt_par_unit "
                                + "<= 2019112523)"));
    }

    @Test
    public void testSql8() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT col FROM tab a WHERE a.thedate = '20080808'",
                new DeParsingMatcher(
                        "SELECT col FROM tab AS a WHERE (a.dt_par_unit >= 2008080800) AND (a"
                                + ".dt_par_unit <= 2008080823)"));
    }

    @Test
    public void testSql9() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "select a.cc from(SELECT cc FROM tab.hdfs WHERE "
                        + "thedate=20200225) a LIMIT 100",
                new DeParsingMatcher(
                        "SELECT a.cc FROM (SELECT cc FROM tab.hdfs "
                                + "WHERE (dt_par_unit >= 2020022500) AND (dt_par_unit <= "
                                + "2020022523)) AS a LIMIT 100"));
    }

    @Test
    public void testSql10() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "select a.cc from(SELECT cc FROM tab WHERE "
                        + "thedate=20200225 limit 100) a LIMIT 10",
                new DeParsingMatcher(
                        "SELECT a.cc FROM (SELECT cc FROM tab WHERE "
                                + "(dt_par_unit >= 2020022500) AND (dt_par_unit <= 2020022523) "
                                + "LIMIT 100) AS a LIMIT 10"));
    }

    @Test
    public void testSql11() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "select c.id from(SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id) where a"
                        + ".thedate=20200225 and b.thedate=20200225) c LIMIT 10",
                new DeParsingMatcher(
                        "SELECT c.id FROM (SELECT a.id FROM tab AS a "
                                + "INNER JOIN tab AS b ON (a.id = b"
                                + ".id) WHERE ((a.dt_par_unit >= 2020022500) AND (a.dt_par_unit "
                                + "<= 2020022523)) AND ((b.dt_par_unit >= 2020022500) AND (b"
                                + ".dt_par_unit <= 2020022523))) AS c LIMIT 10"));
    }

    @Test
    public void testSql12() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "with a as(SELECT cc FROM tab WHERE thedate=20200225 "
                        + "limit 100) select * from a limit 10",
                new DeParsingMatcher(
                        "WITH a AS (SELECT cc FROM tab WHERE "
                                + "(dt_par_unit >= 2020022500) AND (dt_par_unit <= 2020022523) "
                                + "LIMIT 100) SELECT * FROM a LIMIT 10"));
    }

    @Test
    public void testSql13() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT `user_count`, `dtEventTime`, LOCALTIME\n"
                        + "FROM `100558_tqos2913_daily_game_start_user`.hdfs\n"
                        + "WHERE `thedate` >= '20200226' AND `thedate` <= '20200227' AND "
                        + "LOCALTIME LIKE '2020-02-27 18:33:%'\n"
                        + "ORDER BY `dtEventTime` DESC\n"
                        + "limit 1",
                new DeParsingMatcher(
                        "SELECT user_count, dtEventTime, LOCALTIME FROM "
                                + "100558_tqos2913_daily_game_start_user.hdfs WHERE ((dt_par_unit"
                                + " >= 2020022600) AND (dt_par_unit <= 2020022723)) AND "
                                + "(LOCALTIME LIKE '2020-02-27 18:33:%') ORDER BY dtEventTime "
                                + "DESC LIMIT 1"));
    }

    @Test
    public void testSql14() throws Exception {
        assertThat(
                Arrays.asList(new PrestoTheDatePartitionOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT `time`,`table`,`from`,col FROM tab WHERE thedate = '2008\n0808'",
                new DeParsingMatcher(
                        "SELECT \"time\", \"table\", \"from\", col FROM tab WHERE (dt_par_unit >="
                                + " 2008080800) AND (dt_par_unit <= 2008080823)"));
    }

    @Test
    public void testSql15() throws Exception {
        assertThat(
                Arrays.asList(new PrestoTheDatePartitionOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT a.thedate,count(*) as cc FROM tab a WHERE a.thedate = '20080808' group by"
                        + " a.thedate",
                new DeParsingMatcher(
                        "SELECT a.\"thedate\", count(*) AS cc FROM tab AS a WHERE (a.dt_par_unit "
                                + ">= 2008080800) AND (a.dt_par_unit <= 2008080823) GROUP BY a"
                                + ".\"thedate\""));
    }

    @Test
    public void testSql16() throws Exception {
        assertThat(
                Arrays.asList(new PrestoTheDatePartitionOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT a.thedate as tt,count(*) as cc FROM tab a WHERE a.thedate = '20080808' "
                        + "group by a.thedate",
                new DeParsingMatcher(
                        "SELECT a.\"thedate\" AS tt, count(*) AS cc FROM tab AS a WHERE (a"
                                + ".dt_par_unit >= 2008080800) AND (a.dt_par_unit <= 2008080823) "
                                + "GROUP BY a.\"thedate\""));
    }

    @Test
    public void testSql17() throws Exception {
        assertThat(
                Arrays.asList(new PrestoTheDatePartitionOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT thedate,count(*) as cc FROM tab WHERE thedate = '20080808' group by "
                        + "thedate",
                new DeParsingMatcher(
                        "SELECT \"thedate\", count(*) AS cc FROM tab WHERE (dt_par_unit >= "
                                + "2008080800) AND (dt_par_unit <= 2008080823) GROUP BY "
                                + "\"thedate\""));
    }

    @Test
    public void testSql18() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.thedate=b"
                        + ".thedate) where a.thedate=20200225 and b.thedate=20200225",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".thedate = b.thedate)) WHERE ((a.dt_par_unit >= 2020022500) "
                                + "AND (a.dt_par_unit <= 2020022523)) AND ((b.dt_par_unit >= "
                                + "2020022500) AND (b.dt_par_unit <= 2020022523))"));
    }

    @Test
    public void testSql19() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.thedate=b"
                        + ".thedate2) where a.thedate=20200225 and b.thedate=20200225",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".thedate = b.thedate2)) WHERE ((a.dt_par_unit >= 2020022500) "
                                + "AND (a.dt_par_unit <= 2020022523)) AND ((b.dt_par_unit >= "
                                + "2020022500) AND (b.dt_par_unit <= 2020022523))"));
    }

    @Test
    public void testSql20() throws Exception {
        assertThat(
                new PrestoTheDatePartitionOptimizer(),
                "SELECT if(thedate>=20200501,'after05') as c1 FROM tab",
                new DeParsingMatcher(
                        "SELECT if(dt_par_unit >= 2020050100, 'after05') AS c1 FROM "
                                + "tab"));
    }
}