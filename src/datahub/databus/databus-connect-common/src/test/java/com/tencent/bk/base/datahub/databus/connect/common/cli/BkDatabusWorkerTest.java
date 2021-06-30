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

package com.tencent.bk.base.datahub.databus.connect.common.cli;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static junit.framework.TestCase.assertNotNull;

/**
 * BkDatabusWorker Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/13/2018</pre>
 */
@RunWith(PowerMockRunner.class)
public class BkDatabusWorkerTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }


    @Test
    public void testConstructor() {
        BkDatabusWorker obj = new BkDatabusWorker();
        assertNotNull(obj);

    }

    @Test(expected = Exception.class)
    @PrepareForTest(BkDatabusWorker.class)
    public void testMainCase0() throws Exception {
        PowerMockito.spy(System.class);
        PowerMockito.doNothing().when(System.class);
        System.exit(1);
        Whitebox.invokeMethod(BkDatabusWorker.class, "main", (Object) new String[]{});
    }



    @Test(expected = Exception.class)
    @PrepareForTest(BkDatabusWorker.class)
    public void testMainCase3() throws Exception {
        PowerMockito.spy(System.class);
        PowerMockito.doNothing().when(System.class);
        System.exit(1);
        Whitebox.invokeMethod(BkDatabusWorker.class, "main", (Object) new String[]{""});
    }

}
