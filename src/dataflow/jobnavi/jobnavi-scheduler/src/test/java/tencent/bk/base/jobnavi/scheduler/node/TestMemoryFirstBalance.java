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

package tencent.bk.base.jobnavi.scheduler.node;

import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.balance.MemoryFirstBalance;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestMemoryFirstBalance {

    @Test
    public void getHighLevelRunners() {
        RunnerInfo runnerInfo1 = new RunnerInfo();
        runnerInfo1.setHost("runnerInfo1");
        runnerInfo1.setMemoryUsage(1.0);

        RunnerInfo runnerInfo2 = new RunnerInfo();
        runnerInfo2.setHost("runnerInfo2");
        runnerInfo2.setMemoryUsage(2.0);

        RunnerInfo runnerInfo3 = new RunnerInfo();
        runnerInfo3.setHost("runnerInfo3");
        runnerInfo3.setMemoryUsage(3.0);

        List<RunnerInfo> runnerInfos = new ArrayList<>();
        runnerInfos.add(runnerInfo1);
        runnerInfos.add(runnerInfo2);
        runnerInfos.add(runnerInfo3);

        try {
            MemoryFirstBalance balance = new MemoryFirstBalance(new Configuration(false));
            RunnerInfo info = balance.getBestRunner(runnerInfos);
            Assert.assertEquals(info.getHost(), runnerInfo1.getHost());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
