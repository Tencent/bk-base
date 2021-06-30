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

package com.tencent.bk.base.dataflow.core.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TestTools {

    @Test
    public void testWriteValueAsString() {
        Map<String, Object> map = ImmutableMap.of("result", true, "data", "a");
        String actual = Tools.writeValueAsString(map);
        assertEquals("{\"result\":true,\"data\":\"a\"}", actual);
    }

    @Test
    public void testReadMap() {
        String mapString = "{\"result\": \"true\", \"data\": \"abc\"}";
        Map<String, Object> actual = Tools.readMap(mapString);
        assertEquals("true", actual.get("result"));
        assertEquals("abc", actual.get("data"));
    }

    @Test
    public void testReadList() {
        String listString = "[1, 2, 3]";
        List<Integer> actual = Tools.readList(listString);
        assertEquals(listString, actual.toString());
    }

    @Test
    public void testPause() {
        long start = System.currentTimeMillis();
        Tools.pause(3);
        long end = System.currentTimeMillis();
        assertTrue(end - start >= 3000);
    }
}
