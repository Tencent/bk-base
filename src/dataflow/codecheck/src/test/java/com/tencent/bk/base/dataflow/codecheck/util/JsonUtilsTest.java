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

package com.tencent.bk.base.dataflow.codecheck.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JsonUtils.class)
public class JsonUtilsTest {

    private AutoCloseable closeable;

    @Before
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testReadList() {
        List result = JsonUtils.readList("[\"a\", \"b\", \"c\"]");
        String[] arr = {"a", "b", "c"};
        Assert.assertEquals(Arrays.asList(arr), result);
    }

    @Test
    public void testReadMap() {
        Map<String, Object> result = JsonUtils.readMap("{\"bizid\": 0, \"cloudid\": 0}");
        Assert.assertEquals(new HashMap<String, Object>() {{
            put("bizid", 0);
            put("cloudid", 0);
        }}, result);
    }

    @Test
    public void testReadEmptyMap() {
        Map<String, Object> result = JsonUtils.readMap("");
        Assert.assertEquals(new HashMap<String, Object>(), result);
    }

    @Test
    public void testReadMap2() {
        String initialString = "{\"bizid\": 0, \"cloudid\": 0}";
        InputStream targetStream = new ByteArrayInputStream(initialString.getBytes(StandardCharsets.UTF_8));
        Map<String, Object> result = JsonUtils.readMap(targetStream);
        Assert.assertEquals(new HashMap<String, Object>() {{
            put("bizid", 0);
            put("cloudid", 0);
        }}, result);
    }

    @Test
    public void testWriteValueAsString() {
        Map map = new HashMap() {
            {
                put("cloudid", 0);
                put("bizid", 0);
            }
        };
        String result = JsonUtils.writeValueAsString(map);
        Assert.assertEquals("{\"cloudid\":0,\"bizid\":0}", result);
    }
}
