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

public class BlueKingTableNameOptimizerTest extends OptimizerTestSupport {

    String storageUrl = "http://xxx"
            + ".com/v3/meta/result_tables/{0}/?erp={\"~StorageResultTable"
            + ".result_table\":{\"data_type\":\"true\",\"physical_table_name\":\"true\",\"active\":\"true\","
            + "\"storage_cluster_config_id\": \"true\","
            + "\"storage_cluster\":{\"cluster_name\":\"true\",\"cluster_type\": \"true\","
            + "\"connection_info\":\"true\",\"expires\":\"true\",\"version\":\"true\"},"
            + "\"storage_channel\":{\"cluster_type\":\"true\"}}}&result_format=classic";

    @Test
    public void testGetPhysicalTableName1() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "tspider");
        assertThat(optimizer, "SELECT dataid, dteventtime, localtime FROM tab",
                new DeParsingMatcher(
                        "SELECT dataid, dteventtime, localtime FROM test_schema"
                                + ".tab"));
    }

    @Test
    public void testGetPhysicalTableName2() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "tspider");
        assertThat(optimizer,
                "SELECT dataid, dteventtime, localtime FROM tab.tspider",
                new DeParsingMatcher(
                        "SELECT dataid, dteventtime, localtime FROM test_schema"
                                + ".tab"));
    }

    @Test
    public void testGetPhysicalTableName3() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "hdfs");
        assertThat(optimizer, "SELECT a1.log,a2.log\n"
                        + "FROM tab.hdfs a1 join tab.hdfs a2\n"
                        + "on(a1.ip=a2.ip)\n"
                        + "WHERE a1.thedate='20191211'\n"
                        + "ORDER BY a1.dtEventTime DESC\n"
                        + "LIMIT 10",
                new DeParsingMatcher(
                        "SELECT a1.log, a2.log FROM iceberg.test_schema.tab AS a1 "
                                + "INNER JOIN iceberg.test_schema.tab AS a2 ON (a1"
                                + ".ip = a2"
                                + ".ip) WHERE a1.thedate = '20191211' ORDER BY a1.dtEventTime "
                                + "DESC LIMIT 10"));
    }

    @Test
    public void testGetPhysicalTableName4() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "tspider");
        assertThat(optimizer,
                "select count(distinct t2.oid) as cnt from (SELECT oid FROM "
                        + "tab.hdfs WHERE thedate=20191203 ) t1 join ( SELECT "
                        + "oid from tab.hdfs where thedate=20191204) t2 on t1"
                        + ".oid=t2.oid LIMIT 10000",
                new DeParsingMatcher(
                        "SELECT count(DISTINCT t2.oid) AS cnt "
                                + "FROM (SELECT oid "
                                + "FROM iceberg.test_schema.tab "
                                + "WHERE thedate = 20191203) AS t1 "
                                + "INNER JOIN "
                                + "(SELECT oid "
                                + "FROM iceberg.test_schema"
                                + ".tab "
                                + "WHERE thedate = 20191204) AS t2 "
                                + "ON (t1.oid = t2.oid) LIMIT 10000"));
    }

    @Test
    public void testGetPhysicalTableName5() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "federation");
        assertThat(optimizer,
                "select count(distinct t2.oid) as cnt from (SELECT oid FROM "
                        + "tab.hdfs WHERE thedate=20191203 ) t1 join ( SELECT "
                        + "oid from tab.tspider where thedate=20191204) t2 on "
                        + "t1.oid=t2.oid LIMIT 10000",
                new DeParsingMatcher(
                        "SELECT count(DISTINCT t2.oid) AS cnt "
                                + "FROM (SELECT oid "
                                + "FROM iceberg.test_schema.tab "
                                + "WHERE thedate = 20191203) AS t1 "
                                + "INNER JOIN "
                                + "(SELECT oid "
                                + "FROM \"tspider_tspider-test\".test_schema.tab "
                                + "WHERE thedate = 20191204) AS t2 "
                                + "ON (t1.oid = t2.oid) LIMIT 10000"));
    }

    @Test
    public void testGetPhysicalTableName6() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "federation");
        assertThat(optimizer,
                "(SELECT a.dataid as auid FROM tab.tspider a limit 10) union "
                        + "(select b.uid as buid from tab.hdfs b LIMIT 10)",
                new DeParsingMatcher(
                        "(SELECT a.dataid AS auid FROM \"tspider_tspider-test\".test_schema"
                                + ".tab AS a LIMIT 10) UNION (SELECT b.uid AS buid"
                                + " FROM iceberg.test_schema.tab AS b LIMIT 10)"));
    }

    @Test
    public void testGetPhysicalTableName7() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl,
                "tspider");
        assertThat(optimizer,
                "select thedate from(SELECT thedate, dtEventTime, dtEventTimeStamp, localTime, "
                        + "`module`, code, message, result_detail, cnt FROM tab"
                        + ".tspider WHERE thedate>='20200214' AND thedate<='20200214'  ORDER BY "
                        + "dtEventTime DESC) a LIMIT 10",
                new DeParsingMatcher(
                        "SELECT thedate FROM (SELECT thedate, dtEventTime, dtEventTimeStamp, "
                                + "localTime, module, code, message, result_detail, cnt FROM "
                                + "test_schema.tab WHERE (thedate >= '20200214')"
                                + " AND (thedate <= '20200214') ORDER BY dtEventTime DESC) AS a "
                                + "LIMIT 10"));
    }

    @Test
    public void testGetPhysicalTableName8() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl, "hdfs");
        assertThat(optimizer,
                "select a.cc from(SELECT cast(if(wrong_field%2=0,1,2) as varchar) as cc FROM "
                        + "tab.hdfs WHERE thedate=20200225) a limit 10",
                new DeParsingMatcher(
                        "SELECT a.cc "
                                + "FROM (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                                + "FROM iceberg.test_schema.tab "
                                + "WHERE thedate = 20200225) AS a LIMIT 10"));
    }

    @Test
    public void testGetPhysicalTableName9() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl, "hdfs");
        assertThat(optimizer,
                "explain select a.cc from(SELECT cast(if(wrong_field%2=0,1,2) as varchar) as cc FROM "
                        + "tab.hdfs WHERE thedate=20200225) a limit 10",
                new DeParsingMatcher(
                        "EXPLAIN (FORMAT TEXT, TYPE DISTRIBUTED) "
                                + "SELECT a.cc "
                                + "FROM (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                                + "FROM iceberg.test_schema.tab "
                                + "WHERE thedate = 20200225) AS a LIMIT 10"));
    }

    @Test
    public void testGetPhysicalTableName10() throws Exception {
        BlueKingTableNameOptimizer optimizer = new BlueKingTableNameOptimizer(storageUrl, "hdfs");
        assertThat(optimizer,
                "select username "
                        + "from tab.hdfs "
                        + "where username "
                        + "not in(select username "
                        + "from tab.hdfs limit 100)",
                new DeParsingMatcher(
                        "SELECT username "
                                + "FROM iceberg.test_schema.tab "
                                + "WHERE username "
                                + "NOT IN (SELECT username "
                                + "FROM iceberg.test_schema.tab LIMIT 100)"));
    }
}
