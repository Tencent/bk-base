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

public class PrestoDtEventTimePartitionOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testSql1() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime = '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit = 2019112123) AND (dteventtime = "
                                + "'2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql2() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime != '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime <> '2019-11-21 23:59:59'"));
    }

    @Test
    public void testSql3() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime > '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2019112123) AND (dteventtime >"
                                + " '2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql4() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime >= '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit >= 2019112123) AND (dteventtime "
                                + ">= '2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql5() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime < '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2019112123) AND (dteventtime <"
                                + " '2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql6() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT col FROM tab WHERE dteventtime <= '2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE (dt_par_unit <= 2019112123) AND (dteventtime "
                                + "<= '2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql7() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT a.col FROM tab a WHERE a.dteventtime between '2019-11-21 \n00:00:00' and "
                        + "'2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.col FROM tab AS a WHERE ((a.dt_par_unit >= 2019112100) AND (a"
                                + ".dteventtime >= '2019-11-21 \n00:00:00')) AND ((a.dt_par_unit <="
                                + " 2019112123) AND (a.dteventtime <= '2019-11-21 23:59:59'))"));
    }

    @Test
    public void testSql8() throws Exception {
        assertThat(Lists.newArrayList(
                new PrestoDtEventTimePartitionOptimizer(), new PrestoGroupByOptimizer(),
                new ReservedKeyOptimizer(PrestoTestConstants.KEY_WORDS, "\"", null)),
                "SELECT task.topic, connector_task_name, topic_diff, consumer_diff FROM ( SELECT "
                        + "topic, connector_task_name FROM table_1 WHERE "
                        + "thedate = '20200227' AND status = 'running' ) task INNER JOIN ( SELECT"
                        + " topic, SUM(diff) AS topic_diff FROM ( SELECT topic, `partition`, "
                        + "kafka_cluster , MAX(endoffset) - MIN(endoffset) AS diff FROM "
                        + "table_2 WHERE (endoffset > 0 AND dtEventTime >= "
                        + "'2020-02-27 22:20:01' AND dtEventTime < '2020-02-27 22:35:01') GROUP "
                        + "BY topic, `partition`, kafka_cluster ) GROUP BY topic ) topic_op ON "
                        + "task.topic = topic_op.topic LEFT JOIN ( SELECT connector, SUM"
                        + "(input_cnt) AS consumer_diff FROM table_3 WHERE "
                        + "dtEventTime >= '2020-02-27 22:20:01' AND dtEventTime < '2020-02-27 "
                        + "22:37:01' GROUP BY connector ) consumer ON consumer.connector = task"
                        + ".connector_task_name WHERE topic_diff > 0 AND consumer_diff IS NULL "
                        + "LIMIT 100000",
                new DeParsingMatcher(
                        "SELECT task.topic, connector_task_name, topic_diff, consumer_diff FROM "
                                + "(SELECT topic, connector_task_name FROM "
                                + "table_1 WHERE (\"thedate\" = '20200227') "
                                + "AND (status = 'running')) AS task INNER JOIN (SELECT topic, "
                                + "SUM(diff) AS topic_diff FROM (SELECT topic, \"partition\", "
                                + "kafka_cluster, ((MAX(endoffset)) - (MIN(endoffset))) AS diff "
                                + "FROM table_2 WHERE ((endoffset > 0) AND ("
                                + "(dt_par_unit >= 2020022722) AND (\"dtEventTime\" >= "
                                + "'2020-02-27 22:20:01'))) AND ((dt_par_unit <= 2020022722) AND "
                                + "(\"dtEventTime\" < '2020-02-27 22:35:01')) GROUP BY topic, "
                                + "\"partition\", kafka_cluster) GROUP BY topic) AS topic_op ON "
                                + "(task.topic = topic_op.topic) LEFT OUTER JOIN (SELECT "
                                + "connector, SUM(input_cnt) AS consumer_diff FROM "
                                + "table_3 WHERE ((dt_par_unit >= 2020022722) AND "
                                + "(\"dtEventTime\" >= '2020-02-27 22:20:01')) AND ((dt_par_unit "
                                + "<= 2020022722) AND (\"dtEventTime\" < '2020-02-27 22:37:01')) "
                                + "GROUP BY connector) AS consumer ON (consumer.connector = task"
                                + ".connector_task_name) WHERE (topic_diff > 0) AND "
                                + "(consumer_diff IS NULL) LIMIT 100000"));
    }

    @Test
    public void testSql9() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.dteventtime=b"
                        + ".dteventtime) where a.dteventtime='2019-11-21 23:59:59' and b"
                        + ".dteventtime='2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".dteventtime = b.dteventtime)) WHERE ((a.dt_par_unit = "
                                + "2019112123) AND (a.dteventtime = '2019-11-21 23:59:59')) AND ("
                                + "(b.dt_par_unit = 2019112123) AND (b.dteventtime = '2019-11-21 "
                                + "23:59:59'))"));
    }

    @Test
    public void testSql10() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT a.id FROM tab a join "
                        + "tab b on(a.id=b.id and a.dteventtime=b"
                        + ".thedate2) where a.dteventtime>'2019-11-21 23:59:59'",
                new DeParsingMatcher(
                        "SELECT a.id FROM tab AS a INNER JOIN "
                                + "tab AS b ON ((a.id = b.id) AND (a"
                                + ".dteventtime = b.thedate2)) WHERE (a.dt_par_unit >= "
                                + "2019112123) AND (a.dteventtime > '2019-11-21 23:59:59')"));
    }

    @Test
    public void testSql11() throws Exception {
        assertThat(
                new PrestoDtEventTimePartitionOptimizer(),
                "SELECT if(dteventtime>='2020-05-01 00:00:00','after05') as c1 FROM "
                        + "tab",
                new DeParsingMatcher(
                        "SELECT if((dt_par_unit >= 2020050100) AND (dteventtime >= '2020-05-01 "
                                + "00:00:00'), 'after05') AS c1 FROM "
                                + "tab"));
    }
}