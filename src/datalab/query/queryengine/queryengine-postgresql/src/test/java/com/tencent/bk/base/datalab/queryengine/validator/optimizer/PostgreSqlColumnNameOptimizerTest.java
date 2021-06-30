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

import com.beust.jcommander.internal.Lists;
import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import org.junit.Test;

public class PostgreSqlColumnNameOptimizerTest extends OptimizerTestSupport {

    @Test
    public void test1() throws Exception {
        assertThat(
                Lists.newArrayList(new PostgreSqlColumnNameOptimizer()),
                "SELECT col FROM tab where id>0 group by col having col>0  ORDER BY dtEventTime",
                new DeParsingMatcher(
                        "SELECT \"col\" FROM \"tab\" WHERE \"id\" > 0 GROUP BY \"col\" HAVING "
                                + "\"col\" > 0 ORDER BY \"dtEventTime\""));
    }

    @Test
    public void test2() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "SELECT col FROM tab ORDER BY dtEventTime, dtEventTime",
                new DeParsingMatcher(
                        "SELECT \"col\" FROM \"tab\" ORDER BY \"dtEventTime\", \"dtEventTime\""));
    }

    @Test
    public void test3() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "SELECT col as col FROM tab ORDER BY dtEventTime DESC",
                new DeParsingMatcher(
                        "SELECT \"col\" AS \"col\" FROM \"tab\" ORDER BY \"dtEventTime\" DESC"));
    }

    @Test
    public void test4() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "SELECT col FROM tab ORDER BY dtEventTime ASC",
                new DeParsingMatcher("SELECT \"col\" FROM \"tab\" ORDER BY \"dtEventTime\""));
    }

    @Test
    public void testOrderBy5() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "select * from(SELECT col FROM tab ORDER BY dtEventTime ASC)",
                new DeParsingMatcher(
                        "SELECT * FROM (SELECT \"col\" FROM \"tab\" ORDER BY \"dtEventTime\")"));
    }

    @Test
    public void testOrderBy6() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "select * from(SELECT col FROM tab ORDER BY dtEventTime desc limit 10) limit 10",
                new DeParsingMatcher(
                        "SELECT * FROM (SELECT \"col\" FROM \"tab\" ORDER BY \"dtEventTime\" DESC"
                                + " LIMIT 10) LIMIT 10"));
    }

    @Test
    public void testOrderBy7() throws Exception {
        assertThat(
                new PostgreSqlColumnNameOptimizer(),
                "select a.* from(select a.* from(SELECT col FROM tab ORDER BY dtEventTime desc "
                        + "limit 10) a join (SELECT col FROM tab ORDER BY dtEventTime desc limit "
                        + "10) b on a.id = b.id where a.thedate=20200220) order by a.dtEventTime "
                        + "desc limit 10",
                new DeParsingMatcher(
                        "SELECT * FROM "
                                + "(SELECT * FROM "
                                + "(SELECT \"col\" FROM \"tab\" "
                                + "ORDER BY \"dtEventTime\" DESC LIMIT 10) AS \"a\" "
                                + "INNER JOIN "
                                + "(SELECT \"col\" FROM \"tab\" "
                                + "ORDER BY \"dtEventTime\" DESC LIMIT 10) AS \"b\" "
                                + "ON (\"id\" = \"id\") "
                                + "WHERE \"thedate\" = 20200220) "
                                + "ORDER BY \"dtEventTime\" DESC LIMIT 10"));
    }
}
