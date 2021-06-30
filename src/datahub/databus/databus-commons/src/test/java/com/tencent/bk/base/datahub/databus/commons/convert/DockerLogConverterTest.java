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

package com.tencent.bk.base.datahub.databus.commons.convert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class DockerLogConverterTest {

    @Test
    public void getListObjects() {
        DockerLogConverter obj = new DockerLogConverter();
        assertNotNull(obj.getListObjects("k1", "v1", null));
    }

    @Test
    public void getListObjects1() {
        DockerLogConverter obj = new DockerLogConverter();
        assertNotNull(obj.getListObjects("k1", "v1".getBytes(), null));
    }

    @Test
    public void testGetJsonListCase0() throws Exception {
        DockerLogConverter obj = new DockerLogConverter();
        String msgKey = "collector-ds=14500000000&k1=v1";
        String msgValue = "{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}";
        ConvertResult result = obj.getJsonList(msgKey, msgValue);
        assertEquals(14500000000l, result.getTagTime());
        assertEquals("docker-namespace", result.getDockerNameSpace());
        assertEquals(
                Lists.newArrayList("{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}"),
                result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase1() throws Exception {
        DockerLogConverter obj = new DockerLogConverter();
        String msgKey = "collector-ds=abc&k1=v1";
        String msgValue = "{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}";
        ConvertResult result = obj.getJsonList(msgKey, msgValue);
        assertEquals(0l, result.getTagTime());
        assertEquals("docker-namespace", result.getDockerNameSpace());
        assertEquals(
                Lists.newArrayList("{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}"),
                result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase2() throws Exception {
        DockerLogConverter obj = new DockerLogConverter();
        String msgKey = "collector-ds=14500000000&k1=v1";
        String msgValue = "{\"container\":{\"labels\"{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}";
        ConvertResult result = obj.getJsonList(msgKey, msgValue);
        assertEquals(14500000000l, result.getTagTime());
        assertEquals(Lists.newArrayList("JsonParseException"), result.getErrors());
    }

    @Test
    public void testGetJsonListCase3() throws Exception {
        DockerLogConverter obj = new DockerLogConverter();
        String msgKey = "collector-ds=14500000000&k1=";
        String msgValue = "{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}";
        ConvertResult result = obj.getJsonList(msgKey, msgValue);
        assertEquals(14500000000l, result.getTagTime());
        assertEquals("docker-namespace", result.getDockerNameSpace());
        assertEquals(
                Lists.newArrayList("{\"container\":{\"labels\":{\"io.tencent.bcs.namespace\":\"docker-namespace\"}}}"),
                result.getJsonResult());
    }

    @Test
    public void testConstructor() {
        DockerLogConverter obj = new DockerLogConverter();
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(DockerLogConverter.class)
    public void testReadDataServerTagsCase0() throws Exception {
        String data = "k1=v1";
        PowerMockito.mockStatic(URLDecoder.class);
        PowerMockito.when(URLDecoder.decode(data, StandardCharsets.UTF_8.toString()))
                .thenThrow(UnsupportedEncodingException.class);
        DockerLogConverter obj = new DockerLogConverter();
        Map<String, String> result = Whitebox.invokeMethod(obj, "readDataServerTags", data);
        assertEquals(Maps.newHashMap(), result);
    }
}