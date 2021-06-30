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

package com.tencent.bk.base.datahub.databus.connect.common.tools;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
public class ConvertAvroToJsonTest {

    @Test
    public void testMainCase0() throws Exception {
        String configPath = ConvertAvroToJsonTest.class.getClassLoader().getResource("abc.avro").getPath();
        Whitebox.invokeMethod(ConvertAvroToJson.class, "main", (Object) new String[]{configPath});
    }

    @Test
    @PrepareForTest(ConvertAvroToJson.class)
    public void testMainCase1() throws Exception {
        PowerMockito.spy(System.class);
        PowerMockito.doNothing().when(System.class);
        System.exit(1);
        Whitebox.invokeMethod(ConvertAvroToJson.class, "main", (Object) new String[]{});
    }

    @Test
    @PrepareForTest(ConvertAvroToJson.class)
    public void testMainCase2() throws Exception {
        PowerMockito.whenNew(ByteArrayInputStream.class).withAnyArguments().thenThrow(IOException.class);
        String configPath = ConvertAvroToJsonTest.class.getClassLoader().getResource("abc.avro").getPath();
        Whitebox.invokeMethod(ConvertAvroToJson.class, "main", (Object) new String[]{configPath});
    }

    @Test
    public void testConstructor() {
        ConvertAvroToJson obj = new ConvertAvroToJson();
        assertNotNull(obj);

    }


}