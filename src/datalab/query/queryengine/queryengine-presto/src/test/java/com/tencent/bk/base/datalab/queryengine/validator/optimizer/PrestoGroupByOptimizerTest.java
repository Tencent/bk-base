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
import com.tencent.bk.base.datalab.bksql.validator.optimizer.ReservedKeyOptimizer;
import com.tencent.bk.base.datalab.queryengine.validator.constant.PrestoTestConstants;
import org.junit.Test;

public class PrestoGroupByOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testGroupByAlias1() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "SELECT a alias_a, min(c) alias_b, (1+2) alias_c, ('123'||'123') alias_d, e FROM "
                        + "tb group by alias_a, alias_c, alias_d, e",
                new DeParsingMatcher(
                        "SELECT a AS alias_a, min(c) AS alias_b, (1 + 2) AS alias_c, ('123' || "
                                + "'123') AS alias_d, e FROM tb GROUP BY a, 1 + 2, '123' || "
                                + "'123', e"));
    }

    @Test
    public void testGroupByAlias2() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "SELECT a alias_a, substring(b,0) alias_b, min(c) alias_c FROM tb group by "
                        + "alias_a, alias_b",
                new DeParsingMatcher(
                        "SELECT a AS alias_a, SUBSTRING(b, 0) AS alias_b, min(c) AS alias_c FROM "
                                + "tb GROUP BY a, SUBSTRING(b, 0)"));
    }

    @Test
    public void testGroupByAlias3() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "select a.c1 as alias_c1,count(*) from (SELECT c1,c2 FROM tb) a group by alias_c1",
                new DeParsingMatcher(
                        "SELECT a.c1 AS alias_c1, count(*) FROM (SELECT c1, c2 FROM tb) AS a "
                                + "GROUP BY a.c1"));
    }

    @Test
    public void testGroupByHaving1() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "SELECT a alias_a, min(c) alias_c FROM tb group by alias_a having alias_c>0",
                new DeParsingMatcher(
                        "SELECT a AS alias_a, min(c) AS alias_c FROM tb GROUP BY a HAVING (min(c)"
                                + ") > 0"));
    }

    @Test
    public void testGroupByHaving2() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "SELECT a alias_a, b alias_b FROM tb group by alias_a,alias_b having alias_b>0",
                new DeParsingMatcher(
                        "SELECT a AS alias_a, b AS alias_b FROM tb GROUP BY a, b HAVING b > 0"));
    }

    @Test
    public void testGroupByHaving3() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "SELECT a alias_a, b alias_b FROM tb group by alias_a,alias_b having 0>alias_b",
                new DeParsingMatcher(
                        "SELECT a AS alias_a, b AS alias_b FROM tb GROUP BY a, b HAVING 0 > b"));
    }

    @Test
    public void testGroupByHaving4() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer()),
                "select * from (SELECT a alias_a, b alias_b FROM tb group by alias_a,alias_b "
                        + "having 0>alias_b )",
                new DeParsingMatcher(
                        "SELECT * FROM (SELECT a AS alias_a, b AS alias_b FROM tb GROUP BY a, b "
                                + "HAVING 0 > b)"));
    }

    @Test
    public void testGroupByHaving5() throws Exception {
        assertThat(
                Lists.newArrayList(new PrestoGroupByOptimizer(),
                        new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT  count(*) as `count`, SUBSTRING(`dtEventTime` ,1,16) AS `dt`  FROM "
                        + "`table_1`.hdfs WHERE `thedate`=20200227 and "
                        + "`localtime`='2020-02-27 23:58:00' group by `dt` having `count` >0  "
                        + "order by `dt`  desc limit 10",
                new DeParsingMatcher(
                        "SELECT count(*) AS \"count\", SUBSTRING(\"dtEventTime\", 1, 16) AS "
                                + "\"dt\" FROM \"table_1\".hdfs WHERE "
                                + "(\"thedate\" = 20200227) AND (\"localtime\" = '2020-02-27 "
                                + "23:58:00') GROUP BY SUBSTRING(\"dtEventTime\", 1, 16) HAVING "
                                + "(count(*)) > 0 ORDER BY \"dt\" DESC LIMIT 10"));
    }
}
