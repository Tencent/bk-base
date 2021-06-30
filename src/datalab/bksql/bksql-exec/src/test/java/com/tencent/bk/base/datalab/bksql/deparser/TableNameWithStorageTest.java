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

import com.google.common.collect.ImmutableMap;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class TableNameWithStorageTest extends DeParserTestSupport {

    @Test
    public void testTableNames1() throws Exception {
        String sql = "SELECT id,ip,thedate,data from tab1.tspider";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab1", "tspider")));
    }

    @Test
    public void testTableNames2() throws Exception {
        String sql = "SELECT t2.name FROM tab1.tspider t1 join tab2.tspider t2 on t1.id = t2.id";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab1", "tspider", "tab2", "tspider")));
    }

    @Test
    public void testTableNames3() throws Exception {
        String sql = "SELECT t2.name FROM tab1.tspider t1 left outer join tab2.hdfs t2 on t1.id ="
                + " t2.id";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab1", "tspider", "tab2", "hdfs")));
    }

    @Test
    public void testTableNames4() throws Exception {
        String sql = "SELECT t2.name FROM tab1.tspider t1 right outer join tab2.hdfs t2 on t1.id "
                + "= t2.id";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab1", "tspider", "tab2", "hdfs")));
    }

    @Test
    public void testTableNames5() throws Exception {
        String sql = "(SELECT a.uid as auid FROM tab1.tspider a limit 10) union (select b.uid as "
                + "buid from tab2.hdfs b LIMIT 10)";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab1", "tspider", "tab2", "hdfs")));
    }

    @Test
    public void testTableNames6() throws Exception {
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
        assertThat(new TableNamesWithStorage(), sql, new IsEqual<>(ImmutableMap
                .of("tab1", "hdfs", "tab2", "hdfs")));
    }

    @Test
    public void testTableNames7() throws Exception {
        String sql = "select cc from(SELECT cc FROM tab.hdfs  WHERE "
                + "thedate=20200216) LIMIT 100";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames8() throws Exception {
        String sql = "select cc from(SELECT cc FROM tab.hdfs  WHERE "
                + "thedate=20200216 limit 10) LIMIT 100";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames9() throws Exception {
        String sql = "select cc from(SELECT cc FROM tab.hdfs  WHERE "
                + "thedate=20200216 order by dteventtime desc) LIMIT 100";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames10() throws Exception {
        String sql = "select cc from(SELECT cc FROM(select cc from "
                + "tab.hdfs WHERE thedate=20200216 order by "
                + "dteventtime desc)) LIMIT 100";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames11() throws Exception {
        String sql =
                "select thedate from(SELECT thedate, dtEventTime, dtEventTimeStamp, localTime, "
                        + "`module`, code, message, result_detail, cnt\n"
                        + " FROM tab.tspider\n"
                        + " WHERE thedate>='20200214' AND thedate<='20200214'\n"
                        + " ORDER BY dtEventTime DESC) a LIMIT 10";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "tspider")));
    }

    @Test
    public void testTableNames12() throws Exception {
        String sql = "select b.cc from(SELECT a.cc FROM(select cc from "
                + "tab.hdfs WHERE thedate=20200216 order by "
                + "dteventtime desc)a )b LIMIT 100";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames13() throws Exception {
        String sql = "with a as(SELECT cast(`if`(wrong_field%2=0,1,2) as varchar) as cc FROM "
                + "tab.hdfs WHERE thedate=20200224 limit 10) select "
                + "cc from a ";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "hdfs")));
    }

    @Test
    public void testTableNames14() throws Exception {
        String sql = "SELECT minute5 as xx, count(*) c\n"
                + "FROM tab.tspider\n"
                + "WHERE thedate>='20200312' AND thedate<='20200312'\n"
                + "GROUP BY minute5 union(SELECT minute5 as xx, count(*) c\n"
                + "FROM tab.tspider\n"
                + "WHERE thedate>='20200312' AND thedate<='20200312'\n"
                + "GROUP BY minute5 ) limit 10";
        assertThat(new TableNamesWithStorage(), sql,
                new IsEqual<>(ImmutableMap.of("tab", "tspider")));
    }
}
