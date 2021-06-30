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

public class ColumnNameOptimizerTest extends OptimizerTestSupport {

    @Test
    public void test1() throws Exception {
        assertThat(
                new ColumnNameOptimizer("lower"),
                "SELECT report_time AS Report_Time, Happen_Time, manufacturer FROM tab",
                new DeParsingMatcher(
                        "SELECT report_time AS \"Report_Time\", happen_time AS \"Happen_Time\", "
                                + "manufacturer AS \"manufacturer\" FROM tab")
        );
    }

    @Test
    public void test2() throws Exception {
        assertThat(
                new ColumnNameOptimizer("upper"),
                "SELECT report_time AS Report_Time, Happen_Time, manufacturer FROM tab",
                new DeParsingMatcher(
                        "SELECT REPORT_TIME AS \"Report_Time\", HAPPEN_TIME AS \"Happen_Time\", "
                                + "MANUFACTURER AS \"manufacturer\" FROM tab")
        );
    }

    @Test
    public void test3() throws Exception {
        assertThat(
                new ColumnNameOptimizer(""),
                "SELECT report_time AS Report_Time, Happen_Time, manufacturer FROM tab",
                new DeParsingMatcher(
                        "SELECT report_time AS \"Report_Time\", Happen_Time AS \"Happen_Time\", "
                                + "manufacturer AS \"manufacturer\" FROM tab")
        );
    }
}