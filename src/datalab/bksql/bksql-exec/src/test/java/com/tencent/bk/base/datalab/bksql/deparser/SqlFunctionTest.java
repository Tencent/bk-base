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

import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.typesafe.config.ConfigFactory;
import java.util.Locale;
import org.hamcrest.core.IsEqual;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlFunctionTest extends DeParserTestSupport {

    @BeforeClass
    public static void beforeClass() {
        LocaleHolder.instance().set(Locale.US);
    }

    @Test
    public void testSql1() throws Exception {
        String sql = "SELECT (CAST(1 AS DOUBLE)) / 1 FROM tab WHERE (id IS NULL) AND (ID1 = ID2)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<Object>(sql));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "SELECT FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp % 420000)) / "
                + "1000, '%Y%m%d%H%i') AS minute7 FROM tab GROUP BY FROM_UNIXTIME("
                + "(dtEventTimestamp - (dtEventTimestamp % 420000)) / 1000, '%Y%m%d%H%i') ORDER "
                + "BY FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp % 420000)) / 1000, "
                + "'%Y%m%d%H%i')";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<Object>(sql));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "SELECT province, operator, hardware_os, COUNT(*) AS CNT FROM "
                + "tab.hdfs WHERE (dtEventTimestamp >= ((to_unixtime((NOW()) - "
                + "INTERVAL '1' HOUR)) * 1000)) AND (thedate = 20191215) GROUP BY province, "
                + "operator, hardware_os LIMIT 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<Object>(sql));
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "SELECT AVG((CAST(to_unixtime(CAST(localtime AS TIMESTAMP)) AS BIGINT)) - "
                + "(CAST(to_unixtime(CAST(dtEventTime AS TIMESTAMP)) AS BIGINT))) AS avg_delta "
                + "FROM tab.hdfs WHERE thedate = '20191211' LIMIT 10";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<Object>(sql));
    }

    @Test
    public void testSql5() throws Exception {
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
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<Object>(
                        "SELECT port1, count(*) AS cnt FROM (SELECT port1, filename FROM "
                                + "tab1.hdfs WHERE (thedate >= '20191203') "
                                + "AND (thedate <= '20191203')) AS a INNER JOIN (SELECT filename "
                                + "FROM tab2.hdfs WHERE (((thedate >= "
                                + "'20191203') AND (thedate <= '20191203')) AND ((replace(replace"
                                + "(replace(disconnect_extension_reason, 'u''ClientAltF4''', ''),"
                                + " 'u''AFKDetected''', ''), ', ', '')) <> '[]')) AND "
                                + "(reconnect_extension_stage NOT LIKE '%try:%')) AS b ON (a"
                                + ".filename = b.filename) GROUP BY port1 LIMIT 1000"));
    }

    @Test
    public void testSql6() throws Exception {
        String sql =
                "SELECT timestampdiff(week, LEFT(dteventtime, 10), '2020-03-10')  as "
                        + "week_sub_num, thedate, `request_user`\n"
                        + "FROM tab\n"
                        + "WHERE `thedate` >= '20191210' \n"
                        + "LIMIT 1000";
        String expected = "SELECT TIMESTAMPDIFF(WEEK, LEFT(dteventtime, 10), '2020-03-10') AS "
                + "week_sub_num, thedate, request_user FROM tab WHERE "
                + "thedate >= '20191210' LIMIT 1000";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIfFunc1() throws Exception {
        String sql = "select IF(a > 1,'1','2') from tab";
        String expected = "SELECT IF(a > 1, '1', '2') FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIfFunc2() throws Exception {
        String sql = "select IF(a > 1, 1, 2) from tab";
        String expected = "SELECT IF(a > 1, 1, 2) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIfFunc3() throws Exception {
        String sql = "select if(a > 1, 1, '2') from tab";
        String expected = "SELECT if(a > 1, 1, '2') FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testIntervalFunc() throws Exception {
        String sql = "select interval(6,1,2,3,4,5,6,7) from tab";
        String expected = "SELECT interval(6, 1, 2, 3, 4, 5, 6, 7) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }
}
