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

package com.tencent.bk.base.dataflow.jobnavi.yarnservice;

import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.yarnservice.ConstantVar.EventName;
import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RMHAUtils.class, YarnClient.class})
@PowerMockIgnore({"javax.net.ssl.*"})
public class YarnServiceTest {

    YarnService yarnService = null;
    EventListener stateListener;
    EventListener appListener;

    @Before
    public void setUp() throws Exception {
        yarnService = new YarnService();
        stateListener = yarnService.getEventListener(EventName.yarn_state.name());
        appListener = yarnService.getEventListener(EventName.yarn_apps.name());
    }

    @Test
    public void testStateListener() {
        String activeRM = "rm1";
        PowerMockito.mockStatic(RMHAUtils.class);
        PowerMockito.when(RMHAUtils.findActiveRMHAId(any())).thenReturn(activeRM);
        TaskEventResult result = stateListener.doEvent(null);
        Assert.assertTrue(result.isSuccess());
        Assert.assertEquals(activeRM, result.getProcessInfo());

        PowerMockito.when(RMHAUtils.findActiveRMHAId(any())).thenThrow(new RuntimeException());
        TaskEventResult errorResult = stateListener.doEvent(null);
        Assert.assertFalse(errorResult.isSuccess());
    }

    @Test
    public void testAppListener() throws Exception {
        YarnClient yarnClient = PowerMockito.mock(YarnClient.class);
        List<ApplicationReport> runningApplications = Lists.newArrayList();
        runningApplications.add(buildApplicationReport());
        PowerMockito.when(yarnClient.getApplications(any(), any())).thenReturn(runningApplications);

        PowerMockito.mockStatic(YarnClient.class);
        PowerMockito.when(YarnClient.class, "createYarnClient").thenReturn(yarnClient);
        TaskEventResult result = appListener.doEvent(null);
        Assert.assertTrue(result.isSuccess());
        Assert.assertTrue(result.getProcessInfo().contains("application_1622131200000_0001"));
    }

    public ApplicationReport buildApplicationReport() {
        return ApplicationReport.newInstance(ApplicationId.newInstance(1622131200000L, 1),
                null, "", "queue1", "stream app", "",
                80, null, YarnApplicationState.RUNNING, "", "", 0,
                0, null, null, "", 0, "Apache Flink", null);
    }
}