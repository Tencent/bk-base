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

import com.tencent.bk.base.dataflow.bksql.validator.checker.FlinkStartEndTimeChecker;
import com.tencent.blueking.bksql.exception.FailedOnCheckException;
import com.tencent.blueking.bksql.validator.CheckerTestSupport;
import org.junit.Test;

public class FlinkStartEndTimeCheckerTest extends CheckerTestSupport {
    @Test
    public void test() {
        FlinkStartEndTimeChecker checker = new FlinkStartEndTimeChecker();

        assertFailed(checker,
                "SELECT _startTime_ FROM tab",
                FailedOnCheckException.class);

        assertFailed(checker,
                "SELECT _endTime_ FROM tab",
                FailedOnCheckException.class);

        assertFailed(checker,
                "SELECT result AS _startTime_ FROM tab",
                FailedOnCheckException.class);

        assertFailed(checker,
                "SELECT result AS _endTime_ FROM tab",
                FailedOnCheckException.class);

        assertPassed(checker,
                "SELECT _startTime_ AS startField FROM tab");

        assertPassed(checker,
                "SELECT _endTime_ AS endField FROM tab");

        assertPassed(checker,
                "SELECT result AS xx_1 FROM tab");

    }
}