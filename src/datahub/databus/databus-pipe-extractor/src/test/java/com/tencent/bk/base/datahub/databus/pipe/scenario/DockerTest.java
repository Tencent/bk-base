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

public class DockerTest {

    @Test
    public void testCPU() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_cpu.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testDisk() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_disk.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testFS() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_fs.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testNetwork() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_network.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testMemory() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_memory.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testTaskStats() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_task_stats.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker2.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testESLog() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/docker_eslog.conf");
        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/docker_eslog.data", "UTF8")) {
            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }
}
