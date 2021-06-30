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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.tencent.bk.base.datalab.bksql.exception.ParseException;
import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.typesafe.config.ConfigFactory;
import java.util.Locale;
import org.apache.calcite.sql.parser.SqlParseException;
import org.hamcrest.core.IsEqual;
import org.junit.Ignore;
import org.junit.Test;

public class SqlTest extends DeParserTestSupport {

    static {
        LocaleHolder.instance().set(Locale.US);
    }

    @Test
    public void testSql1() throws Exception {
        String sql = "SELECT col, null, true, - (1 + 2) FROM tab WHERE (id IS NULL) AND (ID1 = "
                + "ID2)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "SELECT stream col FROM tab";
        String expected = "SELECT col FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "SELECT col FROM tab WHERE col LIKE 'foo'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "SELECT col FROM tab WHERE col NOT LIKE 'foo'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql5() throws Exception {
        String sql = "SELECT game_appid, acc_speed_mode, acc_effect, COUNT(DISTINCT CASE WHEN "
                + "(length(pvp_start_time)) > 10 THEN left(pvp_start_time, 10) WHEN "
                + "pvp_start_time > 0 THEN pvp_start_time ELSE null END) AS cnt, app_version FROM"
                + " tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql6() throws Exception {
        String sql = "SELECT col FROM tab WHERE col > 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test(expected = ParseException.class)
    public void testSql7() throws Exception {
        String sql = "SELECT (*) AS x FROM emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql8() throws Exception {
        String sql = "SELECT * FROM x WHERE (col LIKE '%123%') AND (c2 > 10)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql9() throws Exception {
        String sql = "SELECT count(*) AS c1, count() AS c2, count(id) AS c3 FROM emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql10() throws Exception {
        String sql = "SELECT (1 + 2) AS c1, count() AS c2, count(id) AS c3 FROM emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testSql11() throws Exception {
        String sql = "SELECT DISTINCT c1, c2 FROM emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testBetweenAnd1() throws Exception {
        final String sql = "SELECT * FROM emp WHERE deptno BETWEEN 1 AND 5";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testBetweenAnd2() throws Exception {
        final String sql = "SELECT * FROM emp WHERE deptno BETWEEN deptno - 1 AND 5";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testColumnAliasWithAs() throws Exception {
        final String sql = "SELECT 1 AS foo FROM database.schema.emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testColumnAliasWithoutAs() throws Exception {
        final String expected = "SELECT 1 AS foo FROM emp";
        final String sql = "SELECT 1 foo FROM emp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNot() throws Exception {
        final String expected = "SELECT NOT (true), NOT (false), NOT (null) FROM t WHERE id IS "
                + "NOT NULL";
        final String sql = "SELECT NOT (true), NOT (false), NOT (null) FROM t WHERE id IS NOT NULL";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testBoolean() throws Exception {
        final String expected = "SELECT * FROM t WHERE true AND false";
        final String sql = "select * from t where true AND false";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIsTrue() throws Exception {
        final String expected = "SELECT * FROM t WHERE (flag IS TRUE) AND (id > 10)";
        final String sql = "SELECT * FROM t WHERE (flag IS TRUE) AND (id > 10)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOperateOnColumn() throws Exception {
        final String expected = "SELECT c1 * 1, c2 + 2, c3 / 3, c4 - 4, c5 * c4 FROM t";
        final String sql = "select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExists() throws Exception {
        final String expected = "SELECT * FROM dept WHERE EXISTS ((SELECT 1 FROM emp WHERE emp"
                + ".deptno = dept.deptno))";
        final String sql = "select * from dept where exists (select 1 from emp where emp.deptno ="
                + " dept.deptno)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNotExists() throws Exception {
        final String expected = "SELECT * FROM dept WHERE NOT (EXISTS ((SELECT 1 FROM emp WHERE "
                + "emp.deptno = dept.deptno)))";
        final String sql = "SELECT * FROM dept WHERE NOT (EXISTS ((SELECT 1 FROM emp WHERE emp"
                + ".deptno = dept.deptno)))";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExistsInWhere() throws Exception {
        final String expected = "SELECT * FROM emp WHERE ((1 = 2) AND (EXISTS ((SELECT 1 FROM "
                + "dept)))) AND (3 = 4)";
        final String sql = "SELECT * FROM emp WHERE ((1 = 2) AND (EXISTS ((SELECT 1 FROM dept))))"
                + " AND (3 = 4)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByColumn() throws Exception {
        final String expected = "SELECT deptno, min(foo) AS x FROM emp GROUP BY deptno, gender";
        final String sql = "select deptno, min(foo) as x from emp group by deptno, gender";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByExp() throws Exception {
        final String expected = "SELECT substr(deptno, 2), gender, min(foo) AS x FROM emp GROUP "
                + "BY substr(deptno, 2), gender";
        final String sql = "select substr(deptno,2), gender, min(foo) as x from emp group by "
                + "substr(deptno, 2), gender";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByAlias() throws Exception {
        final String expected = "SELECT deptno AS a, gender AS b, min(foo) AS x FROM emp GROUP BY"
                + " a, b";
        final String sql = "select deptno as a, gender as b, min(foo) as x from emp group by a, b";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByPosition() throws Exception {
        final String expected = "SELECT deptno, gender, min(foo) AS x FROM emp GROUP BY 1, 2";
        final String sql = "select deptno, gender, min(foo) as x from emp group by 1, 2";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testHavingAfterGroup() throws Exception {
        final String expected = "SELECT deptno FROM emp GROUP BY deptno, emp HAVING ((COUNT(*)) >"
                + " 5) AND (1 = 2)";
        final String sql = "SELECT deptno FROM emp GROUP BY deptno, emp HAVING ((COUNT(*)) > 5) "
                + "AND (1 = 2)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOrderBy() throws Exception {
        final String expected = "SELECT * FROM emp ORDER BY empno, gender DESC, deptno, empno, "
                + "name DESC";
        final String sql = "select * from emp order by empno, gender desc, deptno asc, empno asc,"
                + " name desc";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOrderNullsFirst() throws Exception {
        final String expected = "SELECT * FROM emp ORDER BY gender DESC NULLS LAST, deptno NULLS "
                + "FIRST, empno NULLS FIRST";
        final String sql = "select * from emp order by gender desc nulls last, deptno asc nulls "
                + "first, empno nulls first";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOrderNullsLast() throws Exception {
        final String expected = "SELECT * FROM emp ORDER BY gender DESC NULLS LAST, deptno NULLS "
                + "FIRST, empno NULLS LAST";
        final String sql = "select * from emp order by gender desc nulls last, deptno asc nulls "
                + "first, empno nulls last";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOrderInternal01() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "(select * from emp order by empno) union select * from emp",
                new IsEqual<>("(SELECT * FROM emp ORDER BY empno) UNION (SELECT * FROM emp)"));
    }

    @Test
    public void testOrderInternal02() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select * from(select * from t order by x,y)where a=b",
                new IsEqual<>("SELECT * FROM (SELECT * FROM t ORDER BY x, y) WHERE a = b"));
    }

    @Test
    public void testLimit01() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select a from foo order by b, c limit 2 offset 1",
                new IsEqual<>("SELECT a FROM foo ORDER BY b, c LIMIT 1, 2"));
    }

    @Test
    public void testLimit02() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select a from foo order by b, c limit 2",
                new IsEqual<>("SELECT a FROM foo ORDER BY b, c LIMIT 2"));
    }

    @Test
    public void testLimit03() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select a from foo order by b, c limit 1, 2",
                new IsEqual<>("SELECT a FROM foo ORDER BY b, c LIMIT 1, 2"));
    }

    @Test
    public void testLimit04() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "SELECT a FROM foo LIMIT 2",
                new IsEqual<>("SELECT a FROM foo LIMIT 2"));
    }

    @Test
    public void testLimit05() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select a from foo limit 2 offset 1",
                new IsEqual<>("SELECT a FROM foo LIMIT 1, 2"));
    }

    @Test
    public void testSqlInlineComment() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select 1 from t --this is a comment\n",
                new IsEqual<>("SELECT 1 FROM t"));
    }

    @Test
    public void testMultilineCommentt() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select 1 /* , 2 */, 3 from t",
                new IsEqual<>("SELECT 1, 3 FROM t"));
    }

    @Test
    public void testQueryInFrom() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select * from (select * from emp) as e join (select * from dept) d",
                new IsEqual<>("SELECT * FROM (SELECT * FROM emp) AS e INNER JOIN (SELECT * FROM dept) "
                        + "AS d"));
    }

    @Test
    public void testScalarQueryInWhere() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select * from emp where 3 = (select count(*) from dept where dept.deptno = emp"
                        + ".deptno)",
                new IsEqual<>("SELECT * FROM emp WHERE 3 = (SELECT count(*) FROM dept WHERE dept"
                        + ".deptno = emp.deptno)"));
    }

    @Test
    public void testScalarQueryInSelect() throws Exception {
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG),
                "select x, (select count(*) from dept where dept.deptno = emp.deptno) from emp",
                new IsEqual<>("SELECT x, (SELECT count(*) FROM dept WHERE dept.deptno = emp.deptno) "
                        + "FROM emp"));
    }

    @Test
    public void testCommaJoin1() throws Exception {
        final String expected = "SELECT * FROM t1, t2 WHERE t1.id = t2.id";
        final String sql = "SELECT * FROM t1,t2 where t1.id=t2.id";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testCommaJoin2() throws Exception {
        final String expected = "SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id";
        final String sql = "SELECT * FROM t1,t2,t3 where t1.id=t2.id";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoinCross() throws Exception {
        final String expected = "SELECT * FROM a AS a2 CROSS JOIN b";
        final String sql = "select * from a as a2 cross join b";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoinOn() throws Exception {
        final String expected = "SELECT * FROM a LEFT OUTER JOIN b ON ((1 = 1) AND (2 = 2)) WHERE"
                + " 3 = 3";
        final String sql = "SELECT * FROM a LEFT OUTER JOIN b ON ((1 = 1) AND (2 = 2)) WHERE 3 = 3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test(expected = SqlParseException.class)
    public void testJoinOnParentheses() throws Exception {
        final String expected = "SELECT * FROM a LEFT OUTER JOIN (b INNER JOIN c ON 1 = 1) ON 2 ="
                + " 2 WHERE 3 = 3";
        final String sql = "select * from a left join (b join c on 1 = 1) on 2 = 2 where 3 = 3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test(expected = SqlParseException.class)
    public void testJoinOnParenthesesPlus() throws Exception {
        final String expected = "select * from a\n"
                + " left join (b as b1 (x, y) join (select * from c) c1 on 1 = 1) on 2 = 2\n"
                + "where 3 = 3";
        final String sql = "select * from a\n"
                + " left join (b as b1 (x, y) join (select * from c) c1 on 1 = 1) on 2 = 2\n"
                + "where 3 = 3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplicitTableInJoin() throws Exception {
        final String expected = "SELECT * FROM a LEFT OUTER JOIN (TABLE b) ON (2 = 2) WHERE 3 = 3";
        final String sql = "SELECT * FROM a LEFT OUTER JOIN (TABLE b) ON (2 = 2) WHERE 3 = 3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOuterJoinNoiseWord() throws Exception {
        final String expected = "SELECT * FROM a LEFT OUTER JOIN b ON ((1 = 1) AND (2 = 2)) WHERE"
                + " 3 = 3";
        final String sql = "SELECT * FROM a LEFT OUTER JOIN b ON ((1 = 1) AND (2 = 2)) WHERE 3 = 3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoinQuery() throws Exception {
        final String expected = "SELECT * FROM a INNER JOIN (SELECT * FROM b) AS b2 ON (true)";
        final String sql = "select * from a join (select * from b) as b2 on (true)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        final String expected = "SELECT * FROM a FULL OUTER JOIN b";
        final String sql = "select * from a full outer join b";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoinAssociativity1() throws Exception {
        final String expected = "SELECT * FROM a NATURAL LEFT OUTER JOIN b LEFT OUTER JOIN c ON "
                + "(b.c1 = c.c1)";
        final String sql = "select * from a natural left join b left join c on b.c1 = c.c1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test(expected = SqlParseException.class)
    public void testJoinAssociativity2() throws Exception {
        final String expected = "select * from a natural left join (b left join c on b.c1 = c.c1)";
        final String sql = "select * from a natural left join (b left join c on b.c1 = c.c1)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test(expected = SqlParseException.class)
    public void testJoinAssociativity3() throws Exception {
        final String expected = "select * from (a natural left join b) left join c on b.c1 = c.c1";
        final String sql = "select * from (a natural left join b) left join c on b.c1 = c.c1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNaturalCrossJoin() throws Exception {
        final String expected = "SELECT * FROM a CROSS JOIN b";
        final String sql = "select * from a natural cross join b";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoinUsing() throws Exception {
        final String expected = "SELECT * FROM a INNER JOIN b USING (x)";
        final String sql = "select * from a join b using (x)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJoin01() throws Exception {
        final String expected = "SELECT count(DISTINCT t2.oid) AS cnt FROM (SELECT oid FROM"
                + " tab.hdfs WHERE thedate = 20191203) AS t1 INNER JOIN"
                + " (SELECT oid FROM tab.hdfs WHERE thedate = "
                + "20191204) AS t2 ON (t1.oid = t2.oid) LIMIT 10000";
        final String sql = "select count(distinct t2.oid) as cnt from (SELECT oid FROM "
                + "tab.hdfs WHERE thedate=20191203 ) t1 join ( SELECT "
                + "oid from tab.hdfs where thedate=20191204) t2 on "
                + "t1.oid=t2.oid LIMIT 10000";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    @Ignore
    public void testLateralFunction() throws Exception {
        final String expected = "SELECT * FROM LATERAL TABLE(`ramp`(1))";
        final String sql = "select * from lateral table(ramp(1))";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIn() throws Exception {
        final String expected = "SELECT A FROM TAB WHERE B IN (expr1, expr2, expr3)";
        final String sql = "SELECT A FROM TAB WHERE B IN (expr1,expr2,expr3)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNotIn() throws Exception {
        final String expected = "SELECT A FROM TAB WHERE B NOT IN (expr1, expr2, expr3)";
        final String sql = "SELECT A FROM TAB WHERE B NOT IN (expr1,expr2,expr3)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSelectWithoutFrom1() throws Exception {
        final String expected = "SELECT 1";
        final String sql = "SELECT 1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSelectWithoutFrom2() throws Exception {
        final String expected = "(SELECT c1) UNION (SELECT c2)";
        final String sql = "SELECT c1 union SELECT c2";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTableNameStartWithNumber() throws Exception {
        final String expected = "SELECT * FROM tab";
        final String sql = "SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    @Ignore
    public void testQuoting() throws Exception {
        final String expected = "SELECT `sql`,id1 FROM testdata";
        final String sql = "SELECT `sql`, id1 FROM testdata";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testZhongwen1() throws Exception {
        final String expected = "SELECT id1 FROM tab WHERE id1 = '测试'";
        final String sql = "SELECT id1 FROM tab where id1= '测试'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testZhongwen2() throws Exception {
        final String expected = "SELECT id1 FROM tab WHERE (id1 = 'abc测试') AND (id2 = "
                + "'xxx')";
        final String sql = "SELECT id1 FROM tab where id1= 'abc测试' AND id2 = 'xxx'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSubQuery1() throws Exception {
        final String expected = "SELECT id1 FROM (SELECT id1 FROM tab)";
        final String sql = "SELECT id1 FROM (select id1 from tab)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSetUnion1() throws Exception {
        final String expected = "((SELECT A1 FROM TAB1) UNION (SELECT A2 FROM TAB2)) UNION "
                + "(SELECT A3 FROM TAB3)";
        final String sql = "SELECT A1 FROM TAB1 union SELECT A2 FROM TAB2 union SELECT A3 FROM "
                + "TAB3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSetUnion2() throws Exception {
        final String expected = "((SELECT A1 FROM TAB1) UNION (SELECT A2 FROM TAB2)) UNION "
                + "(SELECT A3 FROM TAB3)";
        final String sql = "(SELECT A1 FROM TAB1 union SELECT A2 FROM TAB2) union SELECT A3 FROM "
                + "TAB3";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSetUnion3() throws Exception {
        final String expected = "SELECT A1 FROM (((SELECT A1 FROM TAB1) UNION (SELECT A2 FROM "
                + "TAB2)) UNION (SELECT A3 FROM TAB3)) AS cc";
        final String sql = "SELECT A1 from (SELECT A1 FROM TAB1 union SELECT A2 FROM TAB2 union "
                + "SELECT A3 FROM TAB3) as cc";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testCase1() throws Exception {
        String sql = "SELECT game_appid, acc_speed_mode, acc_effect, COUNT(DISTINCT CASE WHEN "
                + "(length(pvp_start_time)) > 10 THEN left(pvp_start_time, 10) WHEN "
                + "pvp_start_time > 0 THEN pvp_start_time ELSE null END) AS cnt, app_version FROM"
                + " tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testCase2() throws Exception {
        String sql = "SELECT CASE state WHEN 1 THEN 'success' WHEN -1 THEN 'failed' ELSE "
                + "'unknown' END AS state, app_version FROM tab";
        String expected = "SELECT CASE WHEN state = 1 THEN 'success' WHEN state = -1 THEN "
                + "'failed' ELSE 'unknown' END AS state, app_version FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInterval01() throws Exception {
        String sql = "SELECT `USER`, `dataflow_task_active_num`\n"
                + "FROM tab\n"
                + "WHERE `thedate` >= '20200113' AND `thedate` <= '20200113' and localtime > "
                + "date_sub(now(), interval '1' day)\n"
                + "ORDER BY `dataflow_task_active_num` DESC\n"
                + "LIMIT 100";
        String expected = "SELECT USER, dataflow_task_active_num FROM "
                + "tab WHERE ((thedate >= '20200113')"
                + " AND (thedate <= '20200113')) AND (localtime > (date_sub(now(), INTERVAL '1' "
                + "DAY))) ORDER BY dataflow_task_active_num DESC LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testWith() throws Exception {
        String sql = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F') SELECT deptno "
                + "FROM femaleEmps";
        String expected = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F') SELECT "
                + "deptno FROM femaleEmps";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testWith2() throws Exception {
        String sql = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp";
        String expected = "WITH femaleEmps AS (SELECT * FROM emps WHERE gender = 'F'),  "
                + "marriedFemaleEmps (x, y) AS (SELECT * FROM femaleEmps WHERE maritaStatus = "
                + "'M') SELECT deptno FROM femaleEmp";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testUnique01() throws Exception {
        String sql = "SELECT distinct(`request_user`) as usernamexxx\n "
                + "FROM tab.tspider\n "
                + "WHERE thedate>='20200310' AND thedate<='20200310'\n "
                + "LIMIT 10";
        String expected = "SELECT DISTINCT request_user AS usernamexxx FROM "
                + "tab.tspider WHERE (thedate >= '20200310') AND "
                + "(thedate <= '20200310') LIMIT 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testUnique02() throws Exception {
        String sql = "select * from(SELECT distinct(`request_user`) as usernamexxx\n"
                + "             FROM tab.tspider\n"
                + "             WHERE thedate>='20200310' AND thedate<='20200310'\n"
                + "              LIMIT 10)";
        String expected = "SELECT * FROM (SELECT DISTINCT request_user AS usernamexxx FROM "
                + "tab.tspider WHERE (thedate >= '20200310') AND "
                + "(thedate <= '20200310') LIMIT 10)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testUnique03() throws Exception {
        String sql = "SELECT distinct `request_user`,`storage`\n"
                + "             FROM tab.tspider\n"
                + "             WHERE thedate>='20200310' AND thedate<='20200310'\n"
                + "              LIMIT 10";
        String expected = "SELECT DISTINCT request_user, storage FROM "
                + "tab.tspider WHERE (thedate >= '20200310') AND "
                + "(thedate <= '20200310') LIMIT 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testUnique04() throws Exception {
        String sql = "SELECT count(distinct(`request_user`)) as unique_user_count\n"
                + "             FROM tab.tspider\n"
                + "             WHERE thedate>='20200310' AND thedate<='20200310'\n"
                + "              LIMIT 10";
        String expected = "SELECT count(DISTINCT request_user) AS unique_user_count FROM "
                + "tab.tspider WHERE (thedate >= '20200310') AND "
                + "(thedate <= '20200310') LIMIT 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver1() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       OrderAmount, \n"
                + "       COUNT(OrderID) OVER(PARTITION BY Customercity) AS CountOfOrders, \n"
                + "       AVG(Orderamount) OVER(PARTITION BY Customercity) AS AvgOrderAmount, \n"
                + "       MIN(OrderAmount) OVER(PARTITION BY Customercity) AS MinOrderAmount, \n"
                + "       SUM(Orderamount) OVER(PARTITION BY Customercity) TotalOrderAmount\n"
                + "FROM orders";
        String expected = "SELECT Customercity, CustomerName, (ROW_NUMBER() OVER(PARTITION BY "
                + "Customercity ORDER BY OrderAmount DESC)) AS row_num, OrderAmount, (COUNT"
                + "(OrderID) OVER(PARTITION BY Customercity)) AS CountOfOrders, (AVG(Orderamount)"
                + " OVER(PARTITION BY Customercity)) AS AvgOrderAmount, (MIN(OrderAmount) OVER"
                + "(PARTITION BY Customercity)) AS MinOrderAmount, (SUM(Orderamount) OVER"
                + "(PARTITION BY Customercity)) AS TotalOrderAmount FROM orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver2() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       OrderAmount, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       AVG(orderamount) OVER(PARTITION BY Customercity \n"
                + "       ORDER BY OrderAmount DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS "
                + "CumulativeAVG "
                + " FROM orders";
        String expected = "SELECT Customercity, CustomerName, OrderAmount, (ROW_NUMBER() OVER"
                + "(PARTITION BY Customercity ORDER BY OrderAmount DESC)) AS row_num, (AVG"
                + "(orderamount) OVER(PARTITION BY Customercity ORDER BY OrderAmount DESC ROWS "
                + "BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS CumulativeAVG FROM orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver3() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       OrderAmount, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       AVG(orderamount) OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC ROWS UNBOUNDED PRECEDING) AS CumulativeAvg\n"
                + "FROM Orders";
        String expected = "SELECT Customercity, CustomerName, OrderAmount, (ROW_NUMBER() OVER"
                + "(PARTITION BY Customercity ORDER BY OrderAmount DESC)) AS row_num, (AVG"
                + "(orderamount) OVER(PARTITION BY Customercity ORDER BY OrderAmount DESC ROWS "
                + "UNBOUNDED PRECEDING)) AS CumulativeAvg FROM Orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver4() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       OrderAmount, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       AVG(orderamount) OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC ROWS CURRENT ROW) AS CumulativeAvg\n"
                + "FROM Orders";
        String expected = "SELECT Customercity, CustomerName, OrderAmount, (ROW_NUMBER() OVER"
                + "(PARTITION BY Customercity ORDER BY OrderAmount DESC)) AS row_num, (AVG"
                + "(orderamount) OVER(PARTITION BY Customercity ORDER BY OrderAmount DESC ROWS "
                + "CURRENT ROW)) AS CumulativeAvg FROM Orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver5() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       OrderAmount, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       AVG(orderamount) OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC ROWS 1 PRECEDING) AS CumulativeAvg\n"
                + "FROM Orders";
        String expected = "SELECT Customercity, CustomerName, OrderAmount, (ROW_NUMBER() OVER"
                + "(PARTITION BY Customercity ORDER BY OrderAmount DESC)) AS row_num, (AVG"
                + "(orderamount) OVER(PARTITION BY Customercity ORDER BY OrderAmount DESC ROWS 1 "
                + "PRECEDING)) AS CumulativeAvg FROM Orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testOver6() throws Exception {
        String sql = "SELECT Customercity, \n"
                + "       CustomerName, \n"
                + "       OrderAmount, \n"
                + "       ROW_NUMBER() OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC) AS row_num, \n"
                + "       AVG(orderamount) OVER(PARTITION BY Customercity\n"
                + "       ORDER BY OrderAmount DESC RANGE 1 PRECEDING) AS CumulativeAvg\n"
                + "FROM Orders";
        String expected = "SELECT Customercity, CustomerName, OrderAmount, (ROW_NUMBER() OVER"
                + "(PARTITION BY Customercity ORDER BY OrderAmount DESC)) AS row_num, (AVG"
                + "(orderamount) OVER(PARTITION BY Customercity ORDER BY OrderAmount DESC RANGE 1"
                + " PRECEDING)) AS CumulativeAvg FROM Orders";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    @Ignore
    public void testJdbcParams() throws Exception {
        String sql = "SELECT col,null,true,-(1+2) FROM tab where id = ?";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    @Ignore
    public void testNamedJdbcParams() throws Exception {
        String sql = "SELECT col,null,true,-(1+2) FROM tab where id =:id";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    @Ignore
    public void testParenthesis() throws Exception {
        String sql = "SELECT (1+2)*(1/3) FROM tab where id =1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(sql));
    }

    @Test
    public void testOpPrior() throws Exception {
        String sql = "SELECT 1+2*1/3 FROM tspider_pizz.tab where id =1";
        String expected = "SELECT 1 + ((2 * 1) / 3) FROM tspider_pizz.tab WHERE id = 1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByCube1() throws Exception {
        String sql = "SELECT id,name,age from tab group by cube(id,name,age)";
        String expected = "SELECT id, name, age FROM tab GROUP BY CUBE(id, name, age)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByCube2() throws Exception {
        String sql = "SELECT `dm`, `pttplat`, `pttproject`, `pttpagetype`, `pttsitetype2`, "
                + "`pttpagename`, `pttrefer`,\n"
                + "    `sid`, COUNT(`uid`) AS `spv`, COUNT(DISTINCT `uid`) AS `suv`, COUNT"
                + "(DISTINCT `sid`) AS `sidnum`\n"
                + "    , SUM(`staytime`) AS `total_staytime_millsecond`\n"
                + "FROM `tab`.`hdfs`\n"
                + "WHERE CAST(`staytime` AS BIGINT) >= 0 AND CAST(`staytime` AS BIGINT) <= "
                + "1800000 AND `pttsitetype2`\n"
                + "        <> '_NONE_'\n"
                + "GROUP BY cube(`dm`, `pttplat`, `pttproject`, `pttpagetype`, `pttsitetype2`, "
                + "`pttpagename`, `pttrefer`, `sid`)";
        String expected = "SELECT dm, pttplat, pttproject, pttpagetype, pttsitetype2, "
                + "pttpagename, pttrefer, sid, COUNT(uid) AS spv, COUNT(DISTINCT uid) AS suv, "
                + "COUNT(DISTINCT sid) AS sidnum, SUM(staytime) AS total_staytime_millsecond FROM"
                + " tab.hdfs WHERE (((CAST(staytime AS BIGINT))"
                + " >= 0) AND ((CAST(staytime AS BIGINT)) <= 1800000)) AND (pttsitetype2 <> "
                + "'_NONE_') GROUP BY CUBE(dm, pttplat, pttproject, pttpagetype, pttsitetype2, "
                + "pttpagename, pttrefer, sid)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByCube3() throws Exception {
        String sql = "SELECT id,name,age from tab group by cube(id,(name,age))";
        String expected = "SELECT id, name, age FROM tab GROUP BY CUBE(id, (name, age))";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByRollUp() throws Exception {
        String sql = "SELECT id,name,age from tab group by rollup(id,name,age)";
        String expected = "SELECT id, name, age FROM tab GROUP BY ROLLUP(id, name, age)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testGroupByGroupingSets() throws Exception {
        String sql = "SELECT id,name,age from tab group by grouping sets(id,(name,age))";
        String expected = "SELECT id, name, age FROM tab GROUP BY GROUPING SETS(id, (name, age))";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIntervalWithNumber() throws Exception {
        String sql = "SELECT user AS user\n"
                + "FROM tab\n"
                + "WHERE DateJoined >=\n"
                + "date_sub(now(), interval 30 day)\n"
                + "LIMIT 50000;";
        String expected = "SELECT user AS user FROM tab WHERE "
                + "DateJoined >= (date_sub(now(), INTERVAL '30' DAY)) LIMIT 50000";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIntervalWithCharacter() throws Exception {
        String sql = "SELECT *\n"
                + "FROM tab\n"
                + "WHERE DateJoined >=\n"
                + "date_sub(now(), interval '30' day)\n"
                + "LIMIT 50000;";
        String expected = "SELECT * FROM tab WHERE DateJoined >= "
                + "(date_sub(now(), INTERVAL '30' DAY)) LIMIT 50000";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testRlike() throws Exception {
        String sql = "SELECT *\n"
                + "FROM tab\n"
                + "WHERE name rlike '^tt'";
        String expected = "SELECT * "
                + "FROM tab "
                + "WHERE name RLIKE '^tt'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNotRlike() throws Exception {
        String sql = "SELECT *\n"
                + "FROM tab\n"
                + "WHERE name not rlike '^tt'";
        String expected = "SELECT * "
                + "FROM tab "
                + "WHERE name NOT RLIKE '^tt'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testRegExp() throws Exception {
        String sql = "SELECT *\n"
                + "FROM tab\n"
                + "WHERE name regexp '^tt'";
        String expected = "SELECT * "
                + "FROM tab "
                + "WHERE name REGEXP '^tt'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNotRegExp() throws Exception {
        String sql = "SELECT *\n"
                + "FROM tab\n"
                + "WHERE name NOT REGEXP '^tt'";
        String expected = "SELECT * "
                + "FROM tab "
                + "WHERE name NOT REGEXP '^tt'";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray1() throws Exception {
        String sql = "SELECT * FROM tab WHERE thedate='20200803' and SPLIT(hard_cfg, '#')[2]"
                + " = '1047173'";
        String expected = "SELECT * FROM tab WHERE (thedate = '20200803') AND ((SPLIT"
                + "(hard_cfg, '#')[2]) = '1047173')";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray2() throws Exception {
        String sql =
                "SELECT combinations(ARRAY[1, 2, 2], 2) FROM tab WHERE thedate='20200803' "
                        + "and SPLIT(hard_cfg, '#')[2]"
                        + " = '1047173'";
        String expected = "SELECT combinations(ARRAY[1, 2, 2], 2) FROM tab WHERE (thedate = "
                + "'20200803') AND ((SPLIT(hard_cfg, '#')[2]) = '1047173')";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray3() throws Exception {
        String sql =
                "SELECT ARRAY[1] || ARRAY[2] FROM tab WHERE thedate='20200803' and SPLIT"
                        + "(hard_cfg, '#')[2]"
                        + " = '1047173'";
        String expected = "SELECT (ARRAY[1]) || (ARRAY[2]) FROM tab WHERE (thedate = "
                + "'20200803') AND ((SPLIT(hard_cfg, '#')[2]) = '1047173')";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testMap1() throws Exception {
        String sql = "SELECT * from tab where name_to_age_map['bob']=13";
        String expected = "SELECT * FROM tab WHERE (name_to_age_map['bob']) = 13";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testMap2() throws Exception {
        String sql = "SELECT map[ARRAY[1,3], ARRAY[2,4]] from tab where "
                + "name_to_age_map['bob']=13";
        String expected = "SELECT MAP[ARRAY[1, 3], ARRAY[2, 4]] FROM tab WHERE "
                + "(name_to_age_map['bob']) = 13";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testRow1() throws Exception {
        String sql = "SELECT (1, 2.0)[1] from tab where (1, 2.0)[1]>0";
        String expected = "SELECT (1, 2.0)[1] FROM tab WHERE ((1, 2.0)[1]) > 0";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTrim1() throws Exception {
        String sql = "SELECT trim(id) from tab";
        String expected = "SELECT TRIM(id) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTrim2() throws Exception {
        String sql = "SELECT trim(both '%' from id) from tab";
        String expected = "SELECT TRIM(BOTH '%' FROM id) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTrim3() throws Exception {
        String sql = "SELECT trim(LEADING '' from id) from tab";
        String expected = "SELECT TRIM(LEADING '' FROM id) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTrim4() throws Exception {
        String sql = "SELECT trim(TRAILING '' from id) from tab";
        String expected = "SELECT TRIM(TRAILING '' FROM id) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDelete1() throws Exception {
        String sql = "delete from tab where thedate=20200901";
        String expected = "DELETE FROM tab WHERE thedate = 20200901";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testUpdate1() throws Exception {
        String sql = "update tab set flag=1,result=false where thedate=20200901";
        String expected = "UPDATE tab SET flag = 1, result = false WHERE thedate = 20200901";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTryCast() throws Exception {
        String sql = "select try_cast(id as varchar) from tab";
        String expected = "SELECT TRY_CAST(id AS VARCHAR) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDropTable1() throws Exception {
        String sql = "drop table if exists tab";
        String expected = "DROP TABLE IF EXISTS tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDropTable2() throws Exception {
        String sql = "drop table tab";
        String expected = "DROP TABLE tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDropModel1() throws Exception {
        String sql = "drop model if exists tab";
        String expected = "DROP MODEL IF EXISTS tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDropModel2() throws Exception {
        String sql = "drop model tab";
        String expected = "DROP MODEL tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInsert01() throws Exception {
        String sql = "insert into t1 select * from tab";
        String expected = "INSERT INTO t1 SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInsert02() throws Exception {
        String sql = "insert into t1(c1,c2) select c1,c2 from tab";
        String expected = "INSERT INTO t1(c1, c2) SELECT c1, c2 FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInsert03() throws Exception {
        String sql = "insert overwrite t1 select c1,c2 from tab";
        String expected = "INSERT OVERWRITE t1 SELECT c1, c2 FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInsert04() throws Exception {
        String sql = "insert overwrite t1(c1,c2) select c1,c2 from tab";
        String expected = "INSERT OVERWRITE t1(c1, c2) SELECT c1, c2 FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testAlterTable() throws Exception {
        String sql = "alter table tab rename to tab_1";
        String expected = "ALTER TABLE tab RENAME TO tab_1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "alter table tab add column c1 varchar, add column c2 int";
        expected = "ALTER TABLE tab ADD COLUMN c1 VARCHAR, ADD COLUMN c2 INTEGER";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "alter table tab add column c1 varchar not null, add column c2 varchar";
        expected = "ALTER TABLE tab ADD COLUMN c1 VARCHAR NOT NULL, ADD COLUMN c2 VARCHAR";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "alter table tab drop column c1";
        expected = "ALTER TABLE tab DROP COLUMN c1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "alter table tab drop column c1,drop column c2";
        expected = "ALTER TABLE tab DROP COLUMN c1, DROP COLUMN c2";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "alter table tab rename column c1 to c1_1";
        expected = "ALTER TABLE tab RENAME COLUMN c1 TO c1_1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain1() throws Exception {
        String sql = "explain plan for select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE DISTRIBUTED) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain2() throws Exception {
        String sql = "explain plan as xml for select * from tab";
        String expected = "EXPLAIN (FORMAT XML, TYPE DISTRIBUTED) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain3() throws Exception {
        String sql = "explain plan as json for select * from tab";
        String expected = "EXPLAIN (FORMAT JSON, TYPE DISTRIBUTED) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain4() throws Exception {
        String sql = "explain select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE DISTRIBUTED) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain5() throws Exception {
        String sql = "explain (FORMAT TEXT) select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE DISTRIBUTED) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain6() throws Exception {
        String sql = "explain (FORMAT TEXT,TYPE LOGICAL) select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE LOGICAL) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain7() throws Exception {
        String sql = "explain (TYPE LOGICAL) select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE LOGICAL) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testExplain8() throws Exception {
        String sql = "explain (TYPE IO) select * from tab";
        String expected = "EXPLAIN (FORMAT TEXT, TYPE IO) SELECT * FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testNonReservedKeyWords() throws Exception {
        String sql = "select per, percent, date, position, model, language, function from tab";
        String expected = "SELECT per, percent, date, position, model, language, function FROM "
                + "tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDoubleEquals() throws Exception {
        String sql = "select * from tab where id == 1";
        String expected = "SELECT * FROM tab WHERE id = 1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testMySqlIndexHint01() throws Exception {
        String sql = "select id from tab FORCE INDEX (idx_id) where id>0";
        String expected = "SELECT id FROM tab FORCE INDEX (idx_id) WHERE id > 0";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "select id from tab a FORCE INDEX (idx_id,idx_name) where id>0";
        expected = "SELECT id FROM tab AS a FORCE INDEX (idx_id, idx_name) WHERE id > 0";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "select id from tab IGNORE INDEX (idx_id) where id>0";
        expected = "SELECT id FROM tab IGNORE INDEX (idx_id) WHERE id > 0";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));

        sql = "select id from tab USE INDEX (idx_id) where id>0";
        expected = "SELECT id FROM tab USE INDEX (idx_id) WHERE id > 0";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }
}
