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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparkstreaming;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({YarnUtil.class})
public class SparkStreamingSubmitTaskTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testCheckReourcePass() throws Exception {
        PowerMockito.mockStatic(YarnUtil.class);
        String queue = "root.more.default";
        PowerMockito.when(YarnUtil.isValidSubmitApplication(queue, 3072L)).thenReturn(true);
        testValidateYarnModeResource(queue);
    }

    @Test
    public void testCheckReourceFail() throws Exception {
        PowerMockito.mockStatic(YarnUtil.class);
        exception.expect(NaviException.class);
        String queue = "root.less.default";
        PowerMockito.when(YarnUtil.isValidSubmitApplication(queue, 3072L)).thenReturn(false);
        testValidateYarnModeResource(queue);
    }

    private void testValidateYarnModeResource(String queue) throws NaviException {
        SparkStreamingSubmitTask sparkStreamingSubmitTask = new SparkStreamingSubmitTask();
        String extraInfo = "{\"numExecutors\":\"3\",\"executorMemory\":\"1024M\"}";
        SparkStreamingOptions options = new SparkStreamingOptions("591_xxx", extraInfo, new Properties(),
                "sparkstreaming");
        options.getQueue().setValue(queue);
        sparkStreamingSubmitTask.validateYarnModeResource(options);
    }
}
