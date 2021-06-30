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

public class PrestoMinutexOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testMinuteX1() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "SELECT minute7 FROM tab GROUP BY minute7 ORDER BY minute7",
                new DeParsingMatcher(
                        "SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp %"
                                + " 420000)) / 1000), '%Y%m%d%H%i') AS minute7 FROM tab GROUP BY "
                                + "DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - "
                                + "(dtEventTimestamp % 420000)) / 1000), '%Y%m%d%H%i') ORDER BY "
                                + "DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - "
                                + "(dtEventTimestamp % 420000)) / 1000), '%Y%m%d%H%i')"));
    }

    @Test
    public void testMinuteX2() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "SELECT minute7 FROM tab",
                new DeParsingMatcher(
                        "SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp %"
                                + " 420000)) / 1000), '%Y%m%d%H%i') AS minute7 FROM tab"));
    }

    @Test
    public void testMinuteX3() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "SELECT minute7 as xx FROM tab",
                new DeParsingMatcher(
                        "SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp %"
                                + " 420000)) / 1000), '%Y%m%d%H%i') AS xx FROM tab"));
    }

    @Test
    public void testMinuteX4() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "SELECT minute7 FROM tab WHERE minute7 > 'xx'",
                new DeParsingMatcher(
                        "SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp %"
                                + " 420000)) / 1000), '%Y%m%d%H%i') AS minute7 FROM tab WHERE "
                                + "(DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - "
                                + "(dtEventTimestamp % 420000)) / 1000), '%Y%m%d%H%i')) > 'xx'"));
    }

    @Test
    public void testMinuteX5() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "select * from(SELECT minute7 FROM tab WHERE minute7 > 'xx')",
                new DeParsingMatcher(
                        "SELECT * FROM (SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - "
                                + "(dtEventTimestamp % 420000)) / 1000), '%Y%m%d%H%i') AS minute7"
                                + " FROM tab WHERE (DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp -"
                                + " (dtEventTimestamp % 420000)) / 1000), '%Y%m%d%H%i')) > 'xx')"));
    }

    @Test
    public void testMinuteX6() throws Exception {
        assertThat(
                new PrestoMinutexOptimizer("%Y%m%d%H%i", "DATE_FORMAT"),
                "SELECT minute7 FROM tab t1 join tab t2 on(t1.id=t2.id)  WHERE t1.minute7 > 'xx'",
                new DeParsingMatcher(
                        "SELECT DATE_FORMAT(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp %"
                                + " 420000)) / 1000), '%Y%m%d%H%i') AS minute7 FROM tab AS t1 "
                                + "INNER JOIN tab AS t2 ON (t1.id = t2.id) WHERE (DATE_FORMAT"
                                + "(FROM_UNIXTIME((dtEventTimestamp - (dtEventTimestamp % 420000)"
                                + ") / 1000), '%Y%m%d%H%i')) > 'xx'"));
    }

}
