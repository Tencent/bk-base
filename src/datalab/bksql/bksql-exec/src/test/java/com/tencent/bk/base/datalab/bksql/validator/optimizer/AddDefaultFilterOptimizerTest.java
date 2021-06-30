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

import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class AddDefaultFilterOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testDefaultFilter() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab",
                new DeParsingMatcher(
                        String.format("SELECT col FROM tab WHERE thedate = '%s'", today())));
    }

    @Test
    public void testUtcDefaultFilter() throws Exception {
        System.setProperty("BK_TIMEZONE", "UTC");
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                String.format(
                        "SELECT col FROM tab WHERE thedate = '%s'",
                        ZonedDateTime.now(ZoneId.of("UTC"))
                                .format(DateTimeFormatter.ofPattern("yyyyMMdd"))),
                new DeParsingMatcher(
                        String.format("SELECT col FROM tab WHERE thedate = '%s'", today())));
        System.clearProperty("BK_TIMEZONE");
    }

    @Test
    public void testDefaultFilter2() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab WHERE thedate = '20080808'",
                new DeParsingMatcher("SELECT col FROM tab WHERE thedate = '20080808'"));
    }

    @Test
    public void testDefaultFilter3() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab WHERE id = 1 or id = 2",
                new DeParsingMatcher(
                        String.format(
                                "SELECT col FROM tab WHERE (thedate = '%s') AND ((id = 1) OR (id "
                                        + "= 2))",
                                today())));
    }

    @Test
    public void testDefaultFilter4() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab limit 1",
                new DeParsingMatcher(
                        String.format("SELECT col FROM tab WHERE thedate = '%s' LIMIT 1",
                                today())));
    }

    @Test
    public void testQuotedFilter() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab WHERE dtEventTime = '2018-08-08 00:00:00'",
                new DeParsingMatcher(
                        String.format(
                                "SELECT col FROM tab WHERE dtEventTime = '2018-08-08 00:00:00'",
                                today())));
    }

    @Test
    public void testSubQuery() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "select id from (SELECT col FROM tab) limit 1",
                new DeParsingMatcher(
                        "SELECT id FROM (SELECT col FROM tab) LIMIT 1"));
    }

    @Test
    public void testAs() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab as a WHERE dtEventTime = '2018-08-08 00:00:00'",
                new DeParsingMatcher(
                        String.format(
                                "SELECT col FROM tab AS a WHERE dtEventTime = '2018-08-08 "
                                        + "00:00:00'",
                                today())));
    }

    @Test
    public void testJoin() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT a.dteventtime as xx\n"
                        + "FROM `tab`.tspider AS `a`\n"
                        + "    INNER JOIN `tab`.tspider AS `b` ON `a`.`thedate` = "
                        + "`b`.`thedate`\n"
                        + " limit 10",
                new DeParsingMatcher(
                        "SELECT a.dteventtime AS xx FROM tab.tspider AS a INNER "
                                + "JOIN tab.tspider AS b ON (a.thedate = b"
                                + ".thedate) LIMIT 10"));
    }

    private String today() {
        ZoneId zoneId = ZoneId.of(System.getProperty("BK_TIMEZONE", "Asia/Shanghai"));
        return ZonedDateTime.now(zoneId)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    @Test
    public void testNotAddWithAlias() throws Exception {
        assertThat(
                new AddDefaultDateFilterOptimizer(),
                "SELECT col FROM tab a WHERE a.thedate = '20080808' and 1=1",
                new DeParsingMatcher(
                        "SELECT col FROM tab AS a WHERE (a.thedate = '20080808') AND (1 = 1)"));
    }
}
