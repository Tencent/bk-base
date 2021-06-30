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
import org.junit.Test;

public class AddDefaultLimitOptimizerTest extends OptimizerTestSupport {

    @Test
    public void test1() throws Exception {
        assertThat(
                new AddDefaultLimitOptimizer(100),
                "SELECT col FROM tab LIMIT 0, 100",
                new DeParsingMatcher("SELECT col FROM tab LIMIT 0, 100"));
    }

    @Test
    public void test2() throws Exception {
        assertThat(
                new AddDefaultLimitOptimizer(100),
                "SELECT col FROM tab LIMIT 100",
                new DeParsingMatcher("SELECT col FROM tab LIMIT 100"));
    }

    @Test
    public void test3() throws Exception {
        assertThat(
                new AddDefaultLimitOptimizer(100),
                "SELECT col FROM tab",
                new DeParsingMatcher("SELECT col FROM tab LIMIT 100"));
    }

    @Test
    public void test4() throws Exception {
        assertThat(
                new AddDefaultLimitOptimizer(100),
                "SELECT col FROM tab order by col",
                new DeParsingMatcher("SELECT col FROM tab ORDER BY col LIMIT 100"));
    }

    @Test
    public void test5() throws Exception {
        String sql = "SELECT col FROM tab order by col limit 10";
        String expected = "SELECT col FROM tab ORDER BY col LIMIT 10";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test6() throws Exception {
        String sql = "SELECT port1,count(*) as cnt\n"
                + "FROM (\n"
                + "SELECT port1,filename\n"
                + "FROM tab1.hdfs\n"
                + "WHERE thedate>='20191203' AND thedate<='20191203')a\n"
                + "    join (\n"
                + "SELECT filename\n"
                + "FROM tab2.hdfs\n"
                + "WHERE thedate>='20191203' AND thedate<='20191203'\n"
                + "    and replace(replace(replace(disconnect_extension_reason,"
                + "'u''ClientAltF4''',''),'u''AFKDetected''',''),', ','')\n"
                + "    <> '[]' and\n"
                + "    reconnect_extension_stage not like\n"
                + "    '%try:%')b on a.filename\n"
                + "    = b.filename\n"
                + "GROUP BY port1\n"
                + "LIMIT 1000";
        String expected = "SELECT port1, count(*) AS cnt FROM (SELECT port1, filename FROM "
                + "tab1.hdfs WHERE (thedate >= '20191203') AND (thedate <= "
                + "'20191203')) AS a INNER JOIN (SELECT filename FROM tab2.hdfs "
                + "WHERE (((thedate >= '20191203') AND (thedate <= '20191203')) AND ((replace"
                + "(replace(replace(disconnect_extension_reason, 'u''ClientAltF4''', ''), "
                + "'u''AFKDetected''', ''), ', ', '')) <> '[]')) AND (reconnect_extension_stage "
                + "NOT LIKE '%try:%')) AS b ON (a.filename = b.filename) GROUP BY port1 LIMIT 1000";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test7() throws Exception {
        String sql = "SELECT col FROM tab union SELECT col FROM tab";
        String expected = "(SELECT col FROM tab) UNION (SELECT col FROM tab)";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test8() throws Exception {
        String sql = "(SELECT col FROM tab limit 10) union SELECT col FROM tab";
        String expected = "(SELECT col FROM tab LIMIT 10) UNION (SELECT col FROM tab)";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test9() throws Exception {
        String sql = "(SELECT col FROM tab) union (SELECT col FROM tab) limit 10";
        String expected = "(SELECT col FROM tab) UNION (SELECT col FROM tab) LIMIT 10";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test10() throws Exception {
        String sql = "(SELECT col FROM tab) union (SELECT col FROM tab) limit 10";
        String expected = "(SELECT col FROM tab) UNION (SELECT col FROM tab) LIMIT 10";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test11() throws Exception {
        String sql = "(SELECT col FROM tab) union (SELECT col FROM tab) order by col";
        String expected = "(SELECT col FROM tab) UNION (SELECT col FROM tab) ORDER BY col LIMIT "
                + "100";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test12() throws Exception {
        String sql = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F') SELECT deptno "
                + "FROM femaleEmps";
        String expected = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F') SELECT "
                + "deptno FROM femaleEmps LIMIT 100";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test13() throws Exception {
        String sql = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp";
        String expected = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp LIMIT 100";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    public void test14() throws Exception {
        String sql = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp,marriedFemaleEmps";
        String expected = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp, marriedFemaleEmps LIMIT 100";
        assertThat(
                new AddDefaultLimitOptimizer(100),
                sql,
                new DeParsingMatcher(expected));
    }
}
