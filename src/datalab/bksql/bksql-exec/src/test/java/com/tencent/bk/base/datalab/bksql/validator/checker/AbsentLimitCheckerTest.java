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

package com.tencent.bk.base.datalab.bksql.validator.checker;

import com.tencent.bk.base.datalab.bksql.exception.FailedOnCheckException;
import com.tencent.bk.base.datalab.bksql.validator.CheckerTestSupport;
import org.junit.Test;

public class AbsentLimitCheckerTest extends CheckerTestSupport {

    @Test
    public void testAbsentLimit1() {
        assertFailed(new AbsentLimitChecker(),
                "SELECT col FROM tab GROUP BY col",
                FailedOnCheckException.class);

        assertFailed(new AbsentLimitChecker(),
                "SELECT c1*c2, c1/2, 2-2, 2+c2, c2%c1 FROM tab where true",
                FailedOnCheckException.class);

        assertFailed(new AbsentLimitChecker(),
                "SELECT c1*c2, c1/2, 2-2, 2+c2, c2%c1 FROM tab where not true",
                FailedOnCheckException.class);

        assertFailed(new AbsentLimitChecker(),
                "SELECT col FROM tab OFFSET 10",
                FailedOnCheckException.class);

        assertPassed(new AbsentLimitChecker(),
                "SELECT col FROM tab LIMIT 10");

        assertPassed(new AbsentLimitChecker(),
                "SELECT * FROM (SELECT col FROM tab where col>0 and col2<0) limit 10");

        assertPassed(new AbsentLimitChecker(),
                "SELECT * FROM (SELECT col,count(*) FROM tab where col<0 and col2>0 group by col "
                        + "having col<0 order by col) limit 10");
    }

    @Test
    public void testAbsentLimit2() {
        assertFailed(new AbsentLimitChecker(),
                "SELECT c1*c2, c1/2, 2-2, 2+c2, c2%c1 FROM tab where thedate>'20191114' and "
                        + "thedate<='20191115'",
                FailedOnCheckException.class);
    }

    @Test
    public void testAbsentLimit3() {
        assertPassed(new AbsentLimitChecker(),
                "select count(distinct t2.oid) as cnt from (SELECT oid FROM "
                        + "tab.hdfs WHERE thedate=20191203 ) t1 join ( "
                        + "SELECT oid from tab.hdfs where "
                        + "thedate=20191204) t2 on t1.oid=t2.oid LIMIT 10000");
    }

    @Test
    public void testAbsentLimit4() {
        assertPassed(new AbsentLimitChecker(),
                "with a as(SELECT cast(`if`(wrong_field%2=0,1,2) as varchar) as cc FROM "
                        + "tab.hdfs WHERE thedate=20200225 limit 1) "
                        + "select * from a limit 1");
    }
}
