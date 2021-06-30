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

public class WhereMinutexOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testEqualsTo() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 = '20190704104500'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime = '2019-07-04 10:45:00'")
        );
    }

    @Test
    public void testNotEqualsTo() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 != '20190704104500'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime <> '2019-07-04 10:45:00'")
        );
    }

    @Test
    public void testGreaterThan() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 > '201907041045'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime > '2019-07-04 10:45:00'")
        );
    }

    @Test
    public void testGreaterThanEquals() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 >= '2019070410'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime >= '2019-07-04 10:00:00'")
        );
    }

    @Test
    public void testMinorThan() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 < '20190704104500'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime < '2019-07-04 10:45:00'")
        );
    }

    @Test
    public void testMinorThanEquals() throws Exception {
        assertThat(
                new WhereMinutexOptimizer(),
                "SELECT col FROM tab WHERE minute5 <= '20190704'",
                new DeParsingMatcher(
                        "SELECT col FROM tab WHERE dteventtime <= '2019-07-04 00:00:00'")
        );
    }
}