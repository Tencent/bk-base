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

import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import java.util.Arrays;
import org.junit.Test;

public class DruidAllOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testLowVersion() throws Exception {

        assertThat(
                Arrays.asList(
                        new DruidMinutexOptimizer("yyyy-MM-dd HH:mm:ss", "TIME_FORMAT"),
                        new DruidTimeIntervalOptimizer(),
                        new DruidOrderByOptimizer(),
                        new DruidTimeColumnOptimizer(true),
                        new DruidBuiltinColumnCaseOptimizer(),
                        new DruidIdentifierOptimizer()
                ),
                "SELECT localTime, user, ip, report_time, gseindex, path, log, `time` "
                        + "FROM tab.druid "
                        + "ORDER BY `time` LIMIT 10",
                new DeParsingMatcher(
                        "SELECT \"__localtime\" AS \"localTime\", \"user\" AS \"user\", "
                                + "\"ip\" AS \"ip\", \"report_time\" AS \"report_time\", "
                                + "\"gseindex\" AS \"gseindex\", \"path\" AS \"path\", \"log\" AS"
                                + " \"log\", MILLIS_TO_TIMESTAMP(\"dtEventTimeStamp\") AS "
                                + "\"time\" FROM \"tab\".\"druid\" ORDER BY "
                                + "\"__time\" LIMIT 10")
        );
    }

    @Test
    public void testHighVersion() throws Exception {

        assertThat(
                Arrays.asList(
                        new DruidMinutexOptimizer("yyyy-MM-dd HH:mm:ss", "TIME_FORMAT"),
                        new DruidTimeIntervalOptimizer(),
                        new DruidOrderByOptimizer(),
                        new DruidTimeColumnOptimizer(false),
                        new DruidBuiltinColumnCaseOptimizer(),
                        new DruidIdentifierOptimizer()
                ),
                "SELECT ip, report_time, gseindex, path, log, `time` FROM tab.druid "
                        + "ORDER BY `time` LIMIT 10",
                new DeParsingMatcher(
                        "SELECT \"ip\" AS \"ip\", \"report_time\" AS \"report_time\", "
                                + "\"gseindex\" AS \"gseindex\", \"path\" AS \"path\", \"log\" AS"
                                + " \"log\", \"__time\" AS \"time\" FROM \"tab\""
                                + ".\"druid\" ORDER BY \"__time\" LIMIT 10")
        );
    }

    @Test
    public void testLowVersionChildTable() throws Exception {
        assertThat(
                Arrays.asList(
                        new DruidMinutexOptimizer("yyyy-MM-dd HH:mm:ss", "TIME_FORMAT"),
                        new DruidTimeIntervalOptimizer(),
                        new DruidOrderByOptimizer(),
                        new DruidTimeColumnOptimizer(true),
                        new DruidBuiltinColumnCaseOptimizer(),
                        new DruidIdentifierOptimizer()
                ),
                "SELECT `time`, c FROM (SELECT `time`, count(*) c FROM tab.druid "
                        + "GROUP BY `time` ORDER BY `time` LIMIT 10)",
                new DeParsingMatcher(
                        "SELECT MILLIS_TO_TIMESTAMP(\"dtEventTimeStamp\") AS \"time\", \"c\" AS "
                                + "\"c\" FROM (SELECT MILLIS_TO_TIMESTAMP(\"dtEventTimeStamp\") "
                                + "AS \"time\", count(*) AS \"c\" FROM \"tab\""
                                + ".\"druid\" GROUP BY MILLIS_TO_TIMESTAMP(\"dtEventTimeStamp\") "
                                + "ORDER BY MILLIS_TO_TIMESTAMP(\"dtEventTimeStamp\") LIMIT 10)")
        );
    }

    @Test
    public void testHighVersionChildTable() throws Exception {
        assertThat(
                Arrays.asList(
                        new DruidMinutexOptimizer("yyyy-MM-dd HH:mm:ss", "TIME_FORMAT"),
                        new DruidTimeIntervalOptimizer(),
                        new DruidOrderByOptimizer(),
                        new DruidTimeColumnOptimizer(false),
                        new DruidBuiltinColumnCaseOptimizer(),
                        new DruidIdentifierOptimizer()
                ),
                "SELECT `time`, c FROM (SELECT `time`, count(*) c FROM tab.druid "
                        + "GROUP BY `time` ORDER BY `time` LIMIT 10)",
                new DeParsingMatcher(
                        "SELECT \"__time\" AS \"time\", \"c\" AS \"c\" FROM (SELECT \"__time\" AS"
                                + " \"time\", count(*) AS \"c\" FROM \"tab\""
                                + ".\"druid\" GROUP BY \"__time\" ORDER BY \"__time\" LIMIT 10)")
        );
    }
}