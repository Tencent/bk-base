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
import org.junit.Test;

public class DruidAggFunctionOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testAgg1() throws Exception {
        assertThat(
                new DruidAggFunctionOptimizer("localTime"),
                "SELECT max(localTime), substr(max(localTime)) FROM tab ",
                new DeParsingMatcher(
                        "SELECT TIME_FORMAT(max(TIME_PARSE(localTime)), 'yyyy-MM-dd HH:mm:ss'), "
                                + "substr(TIME_FORMAT(max(TIME_PARSE(localTime)), 'yyyy-MM-dd "
                                + "HH:mm:ss')) FROM tab"));
    }

    @Test
    public void testAgg2() throws Exception {
        assertThat(
                new DruidAggFunctionOptimizer("thedate"),
                "SELECT min(`time`), min(thedate) FROM tab ORDER BY `time` DESC",
                new DeParsingMatcher(
                        "SELECT min(time), TIME_FORMAT(min(TIME_PARSE(thedate)), 'yyyy-MM-dd "
                                + "HH:mm:ss') FROM tab ORDER BY time DESC"));
    }

    @Test
    public void testAgg3() throws Exception {
        assertThat(
                new DruidAggFunctionOptimizer("thedate"),
                "select * from(SELECT min(`time`), min(thedate) FROM tab ORDER BY `time` DESC)",
                new DeParsingMatcher(
                        "SELECT * FROM (SELECT min(time), TIME_FORMAT(min(TIME_PARSE(thedate)), "
                                + "'yyyy-MM-dd "
                                + "HH:mm:ss') FROM tab ORDER BY time DESC)"));
    }

    @Test
    public void testAgg4() throws Exception {
        assertThat(
                new DruidAggFunctionOptimizer("thedate"),
                "select a.tm from(select min(thedate) as tm  FROM tab) a join (select min"
                        + "(thedate) as tm  FROM tab)b on(a.tm=b.tm)",
                new DeParsingMatcher(
                        "SELECT a.tm FROM (SELECT TIME_FORMAT(min(TIME_PARSE(thedate)), "
                                + "'yyyy-MM-dd HH:mm:ss') AS tm FROM tab) AS a INNER JOIN (SELECT"
                                + " TIME_FORMAT(min(TIME_PARSE(thedate)), 'yyyy-MM-dd HH:mm:ss') "
                                + "AS tm FROM tab) AS b ON (a.tm = b.tm)"));
    }
}
