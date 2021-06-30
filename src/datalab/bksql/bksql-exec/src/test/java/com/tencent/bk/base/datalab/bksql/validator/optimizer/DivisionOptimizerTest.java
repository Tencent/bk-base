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

public class DivisionOptimizerTest extends OptimizerTestSupport {

    @Test
    public void test1() throws Exception {
        assertThat(
                new DivisionOptimizer(),
                "SELECT 1/2,max(1/2) "
                        + "FROM `tab` "
                        + "where id > 1/2 "
                        + "and age > max(1/2) "
                        + "group by abs(1/2) "
                        + "having 1/2>0 "
                        + "order by abs(1/2)",
                new DeParsingMatcher(
                        "SELECT (CAST(1 AS DOUBLE)) / 2, max((CAST(1 AS DOUBLE)) / 2) "
                                + "FROM tab "
                                + "WHERE (id > ((CAST(1 AS DOUBLE)) / 2)) "
                                + "AND (age > (max((CAST(1 AS DOUBLE)) / 2))) "
                                + "GROUP BY abs((CAST(1 AS DOUBLE)) / 2) "
                                + "HAVING ((CAST(1 AS DOUBLE)) / 2) > 0 "
                                + "ORDER BY abs((CAST(1 AS DOUBLE)) / 2)"));
    }

    @Test
    public void test2() throws Exception {
        assertThat(
                new DivisionOptimizer(),
                "SELECT case "
                        + "when 1/2>0 then 1/2 "
                        + "else 1/2 "
                        + "end as xx  "
                        + "FROM `tab` "
                        + "where id> 1/2",
                new DeParsingMatcher(
                        "SELECT CASE "
                                + "WHEN ((CAST(1 AS DOUBLE)) / 2) > 0 THEN ((CAST(1 AS DOUBLE)) / 2) "
                                + "ELSE ((CAST(1 AS DOUBLE)) / 2) "
                                + "END AS xx "
                                + "FROM tab "
                                + "WHERE id > ((CAST(1 AS DOUBLE)) / 2)"));
    }
}
