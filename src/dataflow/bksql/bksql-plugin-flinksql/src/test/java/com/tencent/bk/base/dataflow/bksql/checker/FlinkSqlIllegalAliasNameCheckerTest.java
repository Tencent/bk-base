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

package com.tencent.bk.base.dataflow.bksql.checker;

import com.tencent.bk.base.dataflow.bksql.validator.checker.FlinkSqlIllegalAliasNameChecker;
import com.tencent.blueking.bksql.exception.FailedOnCheckException;
import com.tencent.blueking.bksql.validator.CheckerTestSupport;
import org.junit.Test;

public class FlinkSqlIllegalAliasNameCheckerTest extends CheckerTestSupport {

    @Test
    public void test() {
        FlinkSqlIllegalAliasNameChecker checker = new FlinkSqlIllegalAliasNameChecker();

        assertFailed(checker,
                "SELECT result, result1 as x12_3 "
                        + "FROM T1, "
                        + "LATERAL TABLE(ZIP('*','11_21','\\_','12_22','\\_','13_23','\\_')) AS T(1_result)",
                FailedOnCheckException.class);

        assertFailed(checker,
                "SELECT result as 1_xx FROM tab",
                FailedOnCheckException.class);

        assertPassed(checker,
                "SELECT result as xx_1 FROM tab");

        assertPassed(checker,
                "SELECT result, result1 as x12_3, result2 "
                        + "FROM T1, "
                        + "LATERAL TABLE(ZIP('*','11_21','\\_','12_22','\\_','13_23','\\_')) AS T(result, result2)");

        assertPassed(checker,
                "select log, ip, report_time, gseindex,  count(if(path='1', path, null)) as cc "
                        + "from 591_xxxxx group by  log, ip, report_time, gseindex");
    }

    @Test
    public void test1() {
        FlinkSqlIllegalAliasNameChecker checker = new FlinkSqlIllegalAliasNameChecker();
        assertPassed(checker,
                "SELECT result, result1 as x12_3, result2 "
                        + "FROM T1, "
                        + "LATERAL TABLE(ZIP('*','11_21','\\_','12_22','\\_','13_23','\\_')) "
                        + "AS T(`result`,   result2)");

        assertFailed(checker,
                "SELECT result, result1 as x12_3, result2 "
                        + "FROM T1, "
                        + "LATERAL TABLE(ZIP('*','11_21','\\_','12_22','\\_','13_23','\\_')) "
                        + "AS T(`result`,  1result2)",
                FailedOnCheckException.class);
    }
}
