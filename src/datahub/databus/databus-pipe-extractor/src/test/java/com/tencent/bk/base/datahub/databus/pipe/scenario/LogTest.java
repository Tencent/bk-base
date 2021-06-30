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

package com.tencent.bk.base.datahub.databus.pipe.scenario;

import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;

public class LogTest {


    /**
     * 骏鹰日志
     */
    @Test
    public void testJunying() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/log_junying.conf");
        ETL etl = new ETLImpl(confStr);
        String[] lines = TestUtils.readlines("/scenario/log_junying.data", "UTF8");

        for (String line : lines) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testETL2() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/log_sgame2.conf");
        ETL etl = new ETLImpl(confStr);
        String[] lines = TestUtils.readlines("/scenario/log_sgame2.data", "UTF8");

        for (String line : lines) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }
}
