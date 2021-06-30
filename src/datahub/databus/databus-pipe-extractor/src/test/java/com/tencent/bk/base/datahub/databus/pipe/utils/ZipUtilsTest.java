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

package com.tencent.bk.base.datahub.databus.pipe.utils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.powermock.api.mockito.PowerMockito;

/**
 * ZipUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/04/2018</pre>
 */
public class ZipUtilsTest {

    /**
     * Method: unGzip(byte[] msgGzip)
     */
    @Test
    public void testUnGzip() throws Exception {
        String orgMsg = "test ungzip";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(bos);
        gos.write(orgMsg.getBytes(Charset.forName("utf-8")));
        gos.close();
        byte[] zipArray = bos.toByteArray();
        byte[] unzip = ZipUtils.unGzip(zipArray);
        Assert.assertEquals(orgMsg, new String(unzip, "utf-8"));
    }

    /**
     * Method: unGzip(byte[] msgGzip)
     */
    @Test(expected = NullPointerException.class)
    public void testUnGzipCase0() throws Exception {
        byte[] unzip = ZipUtils.unGzip(null);
    }

    /**
     * Method: unGzip(byte[] msgGzip)
     */
    @Test(expected = ZipException.class)
    public void testUnGzipCase1() throws Exception {
        byte[] zipArray = "test ungzip".getBytes(Charset.forName("utf-8"));
        byte[] unzip = ZipUtils.unGzip(zipArray);
    }

    /**
     * 测试构造函数
     */
    @Test
    public void testNew() throws Exception {
        ZipUtils zipUtils = new ZipUtils();
        Assert.assertNotNull(zipUtils);
    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase0() throws Exception {
        ZipUtils zipUtils = new ZipUtils();
        Optional<Method> method = ReflectionUtils.findMethod(zipUtils.getClass(), "closeResources", Closeable[].class);
        Closeable[] closeables = new Closeable[]{};
        ReflectionUtils.invokeMethod(method.get(), zipUtils, (Object) closeables);
        Assert.assertNotNull(zipUtils);
    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase1() throws Exception {
        ZipUtils zipUtils = new ZipUtils();
        Optional<Method> method = ReflectionUtils.findMethod(zipUtils.getClass(), "closeResources", Closeable[].class);
        ReflectionUtils.invokeMethod(method.get(), zipUtils, (Object) null);
        Assert.assertNotNull(zipUtils);
    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase2() throws Exception {
        ZipUtils zipUtils = new ZipUtils();
        Closeable resource = PowerMockito.mock(Closeable.class);
        PowerMockito.when(resource, "close").thenThrow(IOException.class);
        Optional<Method> method = ReflectionUtils.findMethod(zipUtils.getClass(), "closeResources", Closeable[].class);
        Closeable[] closeables = new Closeable[]{resource};
        ReflectionUtils.invokeMethod(method.get(), zipUtils, (Object) closeables);
        Assert.assertNotNull(zipUtils);
    }

} 
