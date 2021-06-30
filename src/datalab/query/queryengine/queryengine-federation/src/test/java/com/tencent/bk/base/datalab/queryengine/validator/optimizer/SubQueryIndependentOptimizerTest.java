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
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import com.tencent.bk.base.datalab.bksql.validator.optimizer.BlueKingTableNameOptimizer;
import java.util.Locale;
import java.util.Properties;
import org.junit.Ignore;
import org.junit.Test;

public class SubQueryIndependentOptimizerTest extends OptimizerTestSupport {

    private static final String URL_PATTERN = "http://localhost:8080/v3/meta/result_tables/{0}/?erp={\"~StorageResultTable.result_table\":{\"data_type\":\"true\",\"physical_table_name\":\"true\",\"active\":\"true\",\"storage_cluster_config_id\": \"true\",\"storage_cluster\":{\"cluster_name\":\"true\",\"cluster_type\": \"true\",\"connection_info\":\"true\",\"expires\":\"true\",\"version\":\"true\"},\"storage_channel\":{\"cluster_type\":\"true\"}}}&result_format=classic";
    private static final String STORAGE = "federation";
    private static final String FEDERATION_CONF;

    static {
        String currentDir = System.getProperty("user.dir");
        String configDir = currentDir.substring(0, currentDir.lastIndexOf("/"));
        FEDERATION_CONF =
                configDir + "/queryengine-distribution/src/main/etc-files/dev-ie/conf/federation";
    }


    @Test
    public void testSubQuery1() throws Exception {
        String sql = "select * "
                + "from (SELECT * "
                + "from tab.tspider "
                + "where thedate=20210411)";
        String expected = "SELECT * "
                + "FROM (SELECT * "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210411) "
                + "AND ((dtEventTimeStamp >= 1618070400000) "
                + "AND (dtEventTimeStamp < 1618156800000)))";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testSubQuery2() throws Exception {
        String sql = "select * "
                + "from (SELECT * "
                + "from tab.hdfs "
                + "where thedate=20210411) "
                + "limit 100";
        String expected = "SELECT * "
                + "FROM (SELECT * "
                + "FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP)))) LIMIT 100";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testJoin1() throws Exception {
        String sql = "select a.* "
                + "from (SELECT * "
                + "from tab.hdfs "
                + "where thedate=20210411)a "
                + "join (SELECT * "
                + "from tab.tspider "
                + "where thedate=20210411) b "
                + "on(a.thedate=b.thedate) "
                + "limit 100";
        String expected = "SELECT a.* "
                + "FROM (SELECT * "
                + "FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP)))) AS a "
                + "INNER JOIN (SELECT * "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210411) AND ((dtEventTimeStamp >= 1618070400000) "
                + "AND (dtEventTimeStamp < 1618156800000))) AS b "
                + "ON (a.thedate = b.thedate) "
                + "LIMIT 100";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testJoin2() throws Exception {
        String sql = "select a.* "
                + "from (SELECT * "
                + "from tab.hdfs "
                + "where thedate=20210411 order by id limit 1000)a "
                + "join (SELECT * "
                + "from tab.tspider "
                + "where thedate=20210411 order by id limit 2000) b "
                + "on(a.thedate=b.thedate) "
                + "limit 100";
        String expected = "SELECT a.* "
                + "FROM (SELECT * "
                + "FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP))) "
                + "ORDER BY id LIMIT 1000) AS a "
                + "INNER JOIN (SELECT * "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210411) "
                + "AND ((dtEventTimeStamp >= 1618070400000) "
                + "AND (dtEventTimeStamp < 1618156800000)) ORDER BY id LIMIT 2000) AS b "
                + "ON (a.thedate = b.thedate) "
                + "LIMIT 100";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testJoin3() throws Exception {
        String sql = "select a.* "
                + "from tab.hdfs a "
                + "join tab.tspider b "
                + "on(a.thedate=b.thedate) "
                + "where a.thedate=20210414 "
                + "limit 100";
        String expected = "SELECT a.* "
                + "FROM iceberg.test_schema.tab AS a "
                + "INNER JOIN \"tspider_tspider-test\".test_schema.tab AS b "
                + "ON (a.thedate = b.thedate) "
                + "WHERE a.thedate = 20210414 "
                + "LIMIT 100";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testUnion1() throws Exception {
        String sql = "select name,id,age "
                + "from tab.hdfs a "
                + "where a.thedate=20210411 "
                + "union "
                + "select name,id,age "
                + "from tab.tspider b "
                + "where b.thedate=20210412 "
                + "union "
                + "select name,id,age "
                + "from tab.tspider c "
                + "where c.thedate=20210413";
        String expected = "((SELECT name, id, age "
                + "FROM iceberg.test_schema.tab AS a "
                + "WHERE (a.____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (a.____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP)))) "
                + "UNION "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS b "
                + "WHERE (b.thedate = 20210412) AND ((b.dtEventTimeStamp >= 1618156800000) "
                + "AND (b.dtEventTimeStamp < 1618243200000)))) "
                + "UNION "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS c "
                + "WHERE (c.thedate = 20210413) AND ((c.dtEventTimeStamp >= 1618243200000) "
                + "AND (c.dtEventTimeStamp < 1618329600000)))";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testUnion2() throws Exception {
        String sql = "(select name,id,age "
                + "from tab.hdfs a "
                + "where a.thedate=20210411 order by name limit 100) "
                + "union "
                + "(select name,id,age "
                + "from tab.tspider b "
                + "where b.thedate=20210412 order by name limit 100) "
                + "union "
                + "(select name,id,age "
                + "from tab.tspider c "
                + "where c.thedate=20210413 order by name limit 100) ";
        String expected = "((SELECT name, id, age "
                + "FROM iceberg.test_schema.tab AS a "
                + "WHERE (a.____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (a.____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP))) "
                + "ORDER BY name LIMIT 100) "
                + "UNION "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS b "
                + "WHERE (b.thedate = 20210412) AND ((b.dtEventTimeStamp >= 1618156800000) "
                + "AND (b.dtEventTimeStamp < 1618243200000)) "
                + "ORDER BY name LIMIT 100)) "
                + "UNION "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS c "
                + "WHERE (c.thedate = 20210413) AND ((c.dtEventTimeStamp >= 1618243200000) "
                + "AND (c.dtEventTimeStamp < 1618329600000)) ORDER BY name LIMIT 100)";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testIntersect() throws Exception {
        String sql = "(select name,id,age "
                + "from tab.hdfs a "
                + "where a.thedate=20210411 order by name limit 100) "
                + "INTERSECT "
                + "(select name,id,age "
                + "from tab.tspider b "
                + "where b.thedate=20210412 order by name limit 100) "
                + "INTERSECT "
                + "(select name,id,age "
                + "from tab.tspider c "
                + "where c.thedate=20210413 order by name limit 100) ";
        String expected = "((SELECT name, id, age "
                + "FROM iceberg.test_schema.tab AS a "
                + "WHERE (a.____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (a.____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP))) "
                + "ORDER BY name LIMIT 100) "
                + "INTERSECT "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS b "
                + "WHERE (b.thedate = 20210412) AND ((b.dtEventTimeStamp >= 1618156800000) "
                + "AND (b.dtEventTimeStamp < 1618243200000)) "
                + "ORDER BY name LIMIT 100)) "
                + "INTERSECT "
                + "(SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab AS c "
                + "WHERE (c.thedate = 20210413) AND ((c.dtEventTimeStamp >= 1618243200000) "
                + "AND (c.dtEventTimeStamp < 1618329600000)) ORDER BY name LIMIT 100)";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testUnionJoin1() throws Exception {
        String sql = "SELECT name, id, age "
                + "FROM tab.hdfs a "
                + "WHERE a.thedate = 20210411 "
                + "UNION "
                + "(SELECT b.name, b.id, b.age "
                + "FROM "
                + "( SELECT name, id, age "
                + "FROM tab.tspider "
                + "WHERE thedate = 20210411 ) b "
                + "JOIN ( SELECT name, id, age "
                + "FROM tab.hdfs "
                + "WHERE thedate = 20210411 ) c ON b.thedate = c.thedate)";
        String expected = "(SELECT name, id, age "
                + "FROM iceberg.test_schema.tab AS a "
                + "WHERE (a.____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (a.____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP)))) "
                + "UNION "
                + "(SELECT b.name, b.id, b.age "
                + "FROM (SELECT name, id, age "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210411) AND ((dtEventTimeStamp >= 1618070400000) "
                + "AND (dtEventTimeStamp < 1618156800000))) AS b "
                + "INNER JOIN "
                + "(SELECT name, id, age "
                + "FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-04-10 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-04-11 15:59:59 UTC' AS TIMESTAMP)))) AS c "
                + "ON (b.thedate = c.thedate))";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testUnionJoin2() throws Exception {
        LocaleHolder.instance()
                .set(Locale.US);
        String sql =
                "SELECT SUM(if(t1.pay_amount IS NULL, 0, t1.pay_amount)) AS xx\n"
                        + "FROM (SELECT oid, pid, SUM(pay_amount) AS pay_amount\n"
                        + "FROM (SELECT DISTINCT statis_date, oid, pay_amount, appid, pid,"
                        + " acctype FROM tab.hdfs\n"
                        + "WHERE thedate >= 20210116 AND thedate <= 20210415) tt\n"
                        + "GROUP BY oid, pid\n"
                        + ") t1\n"
                        + "JOIN (\n"
                        + "SELECT acctype, oid, pid, channelid, time_interval_s, "
                        + "row_number() "
                        + "OVER (PARTITION BY pid, oid ORDER BY time_interval_s ASC) "
                        + "AS idx\n"
                        + "FROM (SELECT DISTINCT ttt1.acctype, ttt1.oid, ttt1.pid, "
                        + "ttt2.channelid,"
                        + "ttt1.login_timestamp - ttt2.request_timestamp AS time_interval_s\n"
                        + "FROM (SELECT acctype, oid, pid, max(login_timestamp) "
                        + "AS login_timestamp\n"
                        + "FROM (SELECT DISTINCT acctype, oid, pid, login_timestamp, 1 "
                        + "AS xValue\n"
                        + "FROM tab.tspider\n"
                        + "WHERE thedate = 20210116\n"
                        + "UNION ALL\n"
                        + "SELECT DISTINCT acctype, oid, pid, 0 AS logint_timestamp, 2 "
                        + "AS xValue\n"
                        + "FROM tab.tspider\n"
                        + "WHERE thedate = 20210116\n"
                        + "UNION ALL\n"
                        + "SELECT DISTINCT acctype, oid, pid, 0 AS login_timestamp, 4 "
                        + "AS xValue\n"
                        + "FROM tab.tspider\n"
                        + "WHERE thedate >= 20201217\n"
                        + "AND thedate < 20210116) tttt\n"
                        + "GROUP BY acctype, oid, pid\n"
                        + "HAVING sum(xValue) = 1) ttt1\n"
                        + "JOIN (\n"
                        + "SELECT oid, pid, request_timestamp\n"
                        + ", if(pid = 0, concat('tgl', channelid, '-', channelname, channelid),"
                        + "channelid) "
                        + "AS channelid\n"
                        + "FROM tab.tspider\n"
                        + "WHERE thedate >= 20210113\n"
                        + "AND thedate <= 20210116\n"
                        + "UNION ALL\n"
                        + "SELECT goid AS oid, pid, request_timestamp, channelid\n"
                        + "FROM (\n"
                        + "SELECT oid, goid, appid\n"
                        + "FROM tab.tspider\n"
                        + "WHERE thedate >= 20201217\n"
                        + "AND thedate <= 20210116) tttt1\n"
                        + "JOIN (\n"
                        + "SELECT oid, pid, request_timestamp, appid\n"
                        + ", if(pid = 0, concat('tgl', channelid, '-', channelname, channelid),"
                        + " channelid) "
                        + "AS channelid\n"
                        + "FROM tab.hdfs\n"
                        + "WHERE thedate >= 20210113\n"
                        + "AND thedate <= 20210116) tttt2\n"
                        + "ON tttt1.oid = tttt2.oid) ttt2\n"
                        + "ON ttt1.oid = ttt2.oid) tt\n"
                        + "WHERE time_interval_s BETWEEN 0 AND 72 * 60 * 60) t2\n"
                        + "ON t1.oid = t2.oid";
        String expected = "SELECT SUM(if(t1.pay_amount IS NULL, 0, t1.pay_amount)) AS xx "
                + "FROM (SELECT oid, pid, SUM(pay_amount) AS pay_amount "
                + "FROM (SELECT DISTINCT statis_date, oid, pay_amount, appid, pid, acctype "
                + "FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-01-15 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-04-15 15:59:59 UTC' AS TIMESTAMP)))) "
                + "AS tt GROUP BY oid, pid) AS t1 "
                + "INNER JOIN (SELECT acctype, oid, pid, channelid, time_interval_s, "
                + "(row_number() OVER(PARTITION BY pid, oid ORDER BY time_interval_s)) "
                + "AS idx FROM (SELECT DISTINCT ttt1.acctype, ttt1.oid, ttt1.pid, "
                + "ttt2.channelid, (ttt1.login_timestamp - ttt2.request_timestamp) AS "
                + "time_interval_s FROM (SELECT acctype, oid, pid, max(login_timestamp) "
                + "AS login_timestamp FROM (((SELECT DISTINCT acctype, oid, pid, "
                + "login_timestamp, 1 AS xValue FROM "
                + "\"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210116) AND ((dtEventTimeStamp >= 1610726400000) "
                + "AND (dtEventTimeStamp < 1610812800000))) UNION ALL "
                + "(SELECT DISTINCT acctype, oid, pid, 0 AS logint_timestamp, 2 AS xValue "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE (thedate = 20210116) AND ((dtEventTimeStamp >= 1610726400000) "
                + "AND (dtEventTimeStamp < 1610812800000)))) UNION ALL "
                + "(SELECT DISTINCT acctype, oid, pid, 0 AS login_timestamp, 4 AS xValue "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE ((thedate >= 20201217) AND (dtEventTimeStamp >= 1608134400000)) "
                + "AND ((thedate < 20210116) AND (dtEventTimeStamp < 1610726400000)))) AS tttt"
                + " GROUP BY acctype, oid, pid HAVING (sum(xValue)) = 1) AS ttt1 "
                + "INNER JOIN ((SELECT oid, pid, request_timestamp, if(pid = 0, "
                + "concat('tgl', channelid, '-', channelname, channelid), channelid) "
                + "AS channelid FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE ((thedate >= 20210113) AND (dtEventTimeStamp >= 1610467200000)) "
                + "AND ((thedate <= 20210116) AND (dtEventTimeStamp <= 1610812799999))) "
                + "UNION ALL (SELECT goid AS oid, pid, request_timestamp, channelid "
                + "FROM (SELECT oid, goid, appid "
                + "FROM \"tspider_tspider-test\".test_schema.tab "
                + "WHERE ((thedate >= 20201217) AND (dtEventTimeStamp >= 1608134400000)) "
                + "AND ((thedate <= 20210116) AND (dtEventTimeStamp <= 1610812799999))) "
                + "AS tttt1 INNER JOIN (SELECT oid, pid, request_timestamp, appid, "
                + "if(pid = 0, concat('tgl', channelid, '-', channelname, channelid), "
                + "channelid) AS channelid FROM iceberg.test_schema.tab "
                + "WHERE (____et >= (CAST('2021-01-12 16:00:00 UTC' AS TIMESTAMP))) "
                + "AND (____et <= (CAST('2021-01-16 15:59:59 UTC' AS TIMESTAMP)))) AS tttt2 "
                + "ON (tttt1.oid = tttt2.oid))) AS ttt2 ON (ttt1.oid = ttt2.oid)) "
                + "AS tt WHERE time_interval_s BETWEEN 0 AND (72 * 60) * 60) AS t2 "
                + "ON (t1.oid = t2.oid)";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF),
                        new BlueKingTableNameOptimizer(URL_PATTERN, STORAGE)),
                sql,
                new DeParsingMatcher(expected));
    }

    @Test
    @Ignore
    public void testPreferStorage() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("prefer_storage", "tspider");
        String sql = "select * "
                + "from (SELECT * "
                + "from tab.tspider "
                + "where thedate=20210411)";
        String expected = "SELECT * "
                + "FROM (SELECT * "
                + "FROM tab.tspider "
                + "WHERE (thedate = 20210411) "
                + "AND ((dtEventTimeStamp >= 1618070400000) "
                + "AND (dtEventTimeStamp < 1618156800000)))";
        assertThat(
                Lists.newArrayList(new SubQueryIndependentOptimizer(URL_PATTERN, FEDERATION_CONF)),
                sql,
                new DeParsingMatcher(expected));
    }
}
