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

public class DruidBuiltinColumnCaseOptimizerTest extends OptimizerTestSupport {

    @Test
    public void testConvert01() throws Exception {
        assertThat(
                new DruidBuiltinColumnCaseOptimizer(),
                "SELECT dteventtimestamp AS xx, dtEventTimeStamp AS dtEventTimeStamp, "
                        + "`localtime`, `localTime` AS `LOCALTIME`, dtEventTime AS DTEVENTTIME "
                        + "FROM tab GROUP BY `localTime` ORDER BY localtime",
                new DeParsingMatcher(
                        "SELECT dtEventTimeStamp AS xx, dtEventTimeStamp AS dtEventTimeStamp, "
                                + "__localtime AS localtime, __localtime AS LOCALTIME, dtEventTime"
                                + " AS"
                                + " DTEVENTTIME FROM tab GROUP BY __localtime ORDER BY __localtime")
        );
    }

    @Test
    public void testConvert02() throws Exception {
        assertThat(
                new DruidBuiltinColumnCaseOptimizer(),
                "select test(substring(localtime,1,5)) as lt from tab group by localtime order "
                        + "by "
                        + "localtime",
                new DeParsingMatcher(
                        "SELECT test(SUBSTRING(__localtime, 1, 5)) AS lt FROM tab GROUP BY "
                                + "__localtime ORDER BY __localtime")
        );
    }
}