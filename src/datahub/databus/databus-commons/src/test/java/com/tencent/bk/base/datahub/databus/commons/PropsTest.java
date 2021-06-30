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

package com.tencent.bk.base.datahub.databus.commons;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class PropsTest {

    @Test
    public void testEmptyFile() {
        DatabusProps props = new DatabusProps();
        Assert.assertNull(props.getProperty("a"));
        Assert.assertEquals("testaaa", props.getOrDefault("a", "testaaa"));
    }

    @Test
    public void testPropsSetAndClear() {
        DatabusProps props = new DatabusProps();
        Assert.assertNull(props.getProperty("a"));
        props.setProperty("a", "abc");
        Assert.assertNotNull(props.getProperty("a"));
        Assert.assertEquals(12, props.getOrDefault("a", 12));
        Assert.assertEquals(12121212L, props.getOrDefault("a", 12121212L));

        props.setProperty("a", "123");
        Assert.assertEquals(123, props.getOrDefault("a", 12));
        Assert.assertEquals(123L, props.getOrDefault("a", 12));

        props.clearProperty();
        Assert.assertNull(props.getProperty("a"));

    }

    @Test
    public void testPropsFile() {
        File file = new File(Consts.DATABUS_PROPERTIES);
        try (OutputStream os = new FileOutputStream(file)) {
            os.write("#test config \n a=test,value\nb=test \n".getBytes(StandardCharsets.UTF_8));
            os.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        DatabusProps props = new DatabusProps();
        Assert.assertNull(props.getProperty("c"));
        Assert.assertEquals("test,value", props.getProperty("a"));
        Assert.assertEquals("test ", props.getProperty("b"));
        Assert.assertEquals("abc", props.getOrDefault("c", "abc"));
        Assert.assertEquals("test", props.getArrayProperty("a", ",")[0]);

        // 清理资源
        Assert.assertTrue(file.delete());
    }

    @Test
    public void testSystemPropsFile() {
        String filename = "abc.properties";
        System.setProperty(Consts.DATABUS_PROPERTIES_FILE_CONFIG, filename);
        File file = new File(filename);
        try (OutputStream os = new FileOutputStream(file)) {
            os.write("#test config \n a=going...to...have...fun\nb= just a fly \n".getBytes(StandardCharsets.UTF_8));
            os.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        Assert.assertNull(DatabusProps.getInstance().getProperty("c"));
        Assert.assertEquals("going...to...have...fun", DatabusProps.getInstance().getProperty("a"));
        Assert.assertEquals("just a fly ", DatabusProps.getInstance().getProperty("b"));
        Assert.assertEquals("default", DatabusProps.getInstance().getOrDefault("c", "default"));
        Assert.assertEquals("have", DatabusProps.getInstance().getArrayProperty("a", "...")[2]);
        Assert.assertEquals(4, DatabusProps.getInstance().getArrayProperty("a", "...").length);

        // 清理资源
        Assert.assertTrue(file.delete());
        System.setProperty(Consts.DATABUS_PROPERTIES_FILE_CONFIG, "");
    }

}
