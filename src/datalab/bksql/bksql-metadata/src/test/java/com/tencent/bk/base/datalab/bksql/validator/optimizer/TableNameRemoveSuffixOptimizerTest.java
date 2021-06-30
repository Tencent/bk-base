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

public class TableNameRemoveSuffixOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testRemoveSuffix1() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer, "SELECT dataid, dteventtime, localtime FROM tab.hdfs",
                new DeParsingMatcher("SELECT dataid, dteventtime, localtime FROM tab"));
    }

    @Test
    public void testRemoveSuffix2() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer, "SELECT dataid, dteventtime, localtime FROM tab.tspider",
                new DeParsingMatcher("SELECT dataid, dteventtime, localtime FROM tab"));
    }

    @Test
    public void testRemoveSuffix3() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer, "SELECT a1.log,a2.log\n"
                        + "FROM tab.hdfs a1 join tab.hdfs a2\n"
                        + "on(a1.ip=a2.ip)\n"
                        + "WHERE a1.thedate='20191211'\n"
                        + "ORDER BY a1.dtEventTime DESC\n"
                        + "LIMIT 10",
                new DeParsingMatcher(
                        "SELECT a1.log, a2.log "
                                + "FROM tab AS a1 "
                                + "INNER JOIN "
                                + "tab AS a2 ON (a1.ip = a2.ip) "
                                + "WHERE a1.thedate = '20191211' "
                                + "ORDER BY a1.dtEventTime DESC LIMIT 10"));
    }

    @Test
    public void testRemoveSuffix4() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "select count(distinct t2.oid) as cnt "
                        + "from (SELECT oid "
                        + "FROM tab.hdfs "
                        + "WHERE thedate=20191203 ) t1 "
                        + "join "
                        + "( SELECT oid "
                        + "from tab.hdfs "
                        + "where thedate=20191204) t2 "
                        + "on t1.oid=t2.oid LIMIT 10000",
                new DeParsingMatcher(
                        "SELECT count(DISTINCT t2.oid) AS cnt "
                                + "FROM (SELECT oid "
                                + "FROM tab "
                                + "WHERE thedate = 20191203) AS t1 "
                                + "INNER JOIN "
                                + "(SELECT oid "
                                + "FROM tab "
                                + "WHERE thedate = 20191204) AS t2 "
                                + "ON (t1.oid = t2.oid) LIMIT 10000"));
    }

    @Test
    public void testRemoveSuffix5() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "select count(distinct t2.oid) as cnt "
                        + "from (SELECT oid "
                        + "FROM tab.hdfs "
                        + "WHERE thedate=20191203 ) t1 "
                        + "join "
                        + "( SELECT oid "
                        + "from tab.tspider "
                        + "where thedate=20191204) t2 "
                        + "on t1.oid=t2.oid LIMIT 10000",
                new DeParsingMatcher(
                        "SELECT count(DISTINCT t2.oid) AS cnt "
                                + "FROM (SELECT oid "
                                + "FROM tab "
                                + "WHERE thedate = 20191203) AS t1 "
                                + "INNER JOIN "
                                + "(SELECT oid "
                                + "FROM tab "
                                + "WHERE thedate = 20191204) AS t2 "
                                + "ON (t1.oid = t2.oid) LIMIT 10000"));
    }

    @Test
    public void testRemoveSuffix6() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "(SELECT a.dataid as auid "
                        + "FROM tab.tspider a limit 10) "
                        + "union "
                        + "(select b.uid as buid "
                        + "from tab.hdfs b LIMIT 10)",
                new DeParsingMatcher(
                        "(SELECT a.dataid AS auid "
                                + "FROM tab AS a LIMIT 10) "
                                + "UNION "
                                + "(SELECT b.uid AS buid FROM tab AS b LIMIT 10)"));
    }

    @Test
    public void testRemoveSuffix7() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "SELECT cc FROM (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                        + "FROM tab.hdfs "
                        + "WHERE thedate = 20200224) LIMIT 100",
                new DeParsingMatcher(
                        "SELECT cc FROM (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                                + "FROM tab "
                                + "WHERE thedate = 20200224) LIMIT 100"));
    }

    @Test
    public void testRemoveSuffix8() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "with a as (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                        + "FROM tab.hdfs "
                        + "WHERE thedate = 20200224) select * from a",
                new DeParsingMatcher(
                        "WITH a AS (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                                + "FROM tab "
                                + "WHERE thedate = 20200224) SELECT * FROM a"));
    }

    @Test
    public void testRemoveSuffix9() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "with a as (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                        + "FROM tab.hdfs "
                        + "WHERE thedate = 20200224 order by cc) select * from a",
                new DeParsingMatcher(
                        "WITH a AS (SELECT CAST(if((wrong_field % 2) = 0, 1, 2) AS VARCHAR) AS cc "
                                + "FROM tab "
                                + "WHERE thedate = 20200224 ORDER BY cc) SELECT * FROM a"));
    }

    @Test
    public void testRemoveSuffix10() throws Exception {
        TableNameRemoveSuffixOptimizer optimizer = new TableNameRemoveSuffixOptimizer();
        assertThat(optimizer,
                "SELECT task.topic, connector_task_name, topic_diff, consumer_diff "
                        + "FROM ( SELECT topic, connector_task_name "
                        + "FROM tab_1.hdfs "
                        + "WHERE thedate = '20200227' AND status = 'running' ) task "
                        + "INNER JOIN "
                        + "( SELECT topic, SUM(diff) AS topic_diff "
                        + "FROM ( SELECT topic, `partition`, kafka_cluster , MAX(endoffset) - MIN(endoffset) "
                        + "AS diff "
                        + "FROM tab_2.hdfs "
                        + "WHERE (endoffset > 0 "
                        + "AND dtEventTime >= '2020-02-27 22:20:01' "
                        + "AND dtEventTime < '2020-02-27 22:35:01') "
                        + "GROUP BY topic, `partition`, kafka_cluster ) GROUP BY topic ) topic_op "
                        + "ON task.topic = topic_op.topic "
                        + "LEFT JOIN "
                        + "( SELECT connector, SUM(input_cnt) AS consumer_diff "
                        + "FROM tab_3.hdfs "
                        + "WHERE dtEventTime >= '2020-02-27 22:20:01' "
                        + "AND dtEventTime < '2020-02-27 22:37:01' GROUP BY connector ) consumer "
                        + "ON consumer.connector = task.connector_task_name "
                        + "WHERE topic_diff > 0 AND consumer_diff IS NULL LIMIT 100000",
                new DeParsingMatcher(
                        "SELECT task.topic, connector_task_name, topic_diff, consumer_diff "
                                + "FROM (SELECT topic, connector_task_name "
                                + "FROM tab_1 "
                                + "WHERE (thedate = '20200227') AND (status = 'running')) AS task "
                                + "INNER JOIN "
                                + "(SELECT topic, SUM(diff) AS topic_diff "
                                + "FROM (SELECT topic, partition, kafka_cluster, "
                                + "((MAX(endoffset)) - (MIN(endoffset))) AS diff "
                                + "FROM tab_2 WHERE ((endoffset > 0) "
                                + "AND (dtEventTime >= '2020-02-27 22:20:01')) "
                                + "AND (dtEventTime < '2020-02-27 22:35:01') "
                                + "GROUP BY topic, partition, kafka_cluster) GROUP BY topic) "
                                + "AS topic_op "
                                + "ON (task.topic = topic_op.topic) "
                                + "LEFT OUTER JOIN "
                                + "(SELECT connector, SUM(input_cnt) AS consumer_diff "
                                + "FROM tab_3 "
                                + "WHERE (dtEventTime >= '2020-02-27 22:20:01') "
                                + "AND (dtEventTime < '2020-02-27 22:37:01') GROUP BY connector) "
                                + "AS consumer "
                                + "ON (consumer.connector = task.connector_task_name) "
                                + "WHERE (topic_diff > 0) AND (consumer_diff IS NULL) LIMIT 100000"));
    }


}