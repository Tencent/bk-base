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

public class OnlineTest {

    @Test
    public void testETL() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/online_1.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/online_1.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testDerive() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/online_2_derive.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/online_2_derive.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
            Assert.assertEquals(2, result.getSucceedMsgNum());

        }
    }

    @Test
    public void testDerive2() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/derive.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/derive.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
            Assert.assertEquals(2, result.getSucceedMsgNum());
        }
    }

}
