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

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class JaCommunityTest {

    @Test
    public void testCpu() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_cpu.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/cpu.data", "UTF-8")) {
            ETLResult result = etl.handle(line);
            List<Object> data = result.getValues().get(0).get(0);

            Assert.assertEquals(0, result.getFailedMsgNum());
            Assert.assertEquals(1479729466000L, result.getValByName(data, "timestamp"));
        }
    }

    @Test
    public void testCpuCpuUsage() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_cpu_cpuusage.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/cpu.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            List<Object> data = result.getValues().get(0).get(0);

            Assert.assertEquals(0, result.getFailedMsgNum());
            Assert.assertEquals(3.7259010896898577, result.getValByName(data, "cpuusage"));

        }
    }

    @Test
    public void testCpuMaxUsage() throws Exception {
        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_cpu_core_max_cpuusage.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/cpu.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            List<Object> data = result.getValues().get(0).get(0);

            Assert.assertEquals(0, result.getFailedMsgNum());
            Assert.assertEquals(4.246071547977265, result.getValByName(data, "cpuusage"));
        }
    }

    @Test
    public void testCpuCoreUsage() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_cpu_core_cpuusage.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/cpu.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            List<List<Object>> datas = result.getValues().get(0);

            for (List<Object> data : datas) {
                int coreNo = (int) result.getValByName(data, "_iteration_idx");
                switch (coreNo) {
                    case 1:
                        Assert.assertEquals(3.3801874163319945, result.getValByName(data, "cpuusage"));
                        break;
                    case 2:
                        Assert.assertEquals(4.246071547977265, result.getValByName(data, "cpuusage"));
                        break;
                    case 3:
                        Assert.assertEquals(3.988603988603989, result.getValByName(data, "cpuusage"));
                        break;
                    case 4:
                        Assert.assertEquals(3.2692955847657568, result.getValByName(data, "cpuusage"));
                        break;
                }
            }

            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testJAGseDisk() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_disk.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/disk.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testJAGseDiskIostats() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_disk_iostats.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/disk.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testJAGseDiskUsed() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_disk_used.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/disk.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testJAGseNet() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_net.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/net.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testGseNetDetail() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_net_detail.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/net.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            Assert.assertEquals(0, result.getFailedMsgNum());
        }
    }

    @Test
    public void testGseMem() throws Exception {

        String confStr = TestUtils.getFileContent("/scenario/ja_community/ja_gse_mem.conf");

        ETL etl = new ETLImpl(confStr);

        for (String line : TestUtils.readlines("/scenario/ja_community/mem.data", "UTF-8")) {

            ETLResult result = etl.handle(line);
            List<Object> data = result.getValues().get(0).get(0);

            Assert.assertEquals(0, result.getFailedMsgNum());

            int memtotal = (int) result.getValByName(data, "memtotal");
            int memused = (int) result.getValByName(data, "memused");
            int memfree = (int) result.getValByName(data, "memfree");

            Assert.assertEquals(memtotal - memfree, memused);
            Assert.assertEquals(14.634373982747395, result.getValByName(data, "processmemusage"));
        }
    }
}
