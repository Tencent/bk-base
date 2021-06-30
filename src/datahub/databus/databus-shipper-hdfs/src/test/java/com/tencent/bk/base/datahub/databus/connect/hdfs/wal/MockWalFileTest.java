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


package com.tencent.bk.base.datahub.databus.connect.hdfs.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.MessageDigest;

@RunWith(PowerMockRunner.class)
public class MockWalFileTest {

    /**
     * 测试初始化sync时抛出异常的情况
     *
     * @throws IOException
     */
    @Test(expected = RuntimeException.class)
    @PrepareForTest({WalFile.Writer.class})
    public void testWriterInitKeySerializerIsNull() throws Exception {
        PowerMockito.mockStatic(MessageDigest.class);
        PowerMockito.when(MessageDigest.getInstance("MD5")).thenThrow(Exception.class);
        WalFile.createWriter(new Configuration(), WalFile.Writer.file(new Path("xx")));
    }

    /**
     * 测试执行init方法时keySerializer == null的情况：Could not find a serializer for the Key class
     *
     * @throws Exception
     */
    @Test(expected = IOException.class)
    @PrepareForTest({WalFile.Writer.class})
    public void testWriterInitKeyIOException() throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, JavaSerialization.class.getName());
        WalFile.Writer writer = PowerMockito.mock(WalFile.Writer.class);
        FSDataOutputStream outputStream = PowerMockito.mock(FSDataOutputStream.class);
        PowerMockito.doCallRealMethod().when(writer).init(conf, outputStream, true);
        writer.init(conf, outputStream, true);
    }

    /**
     * 测试执行append方法时keyLength < 0的情况：negative length keys not allowed
     *
     * @throws Exception
     */
    @Test(expected = IOException.class)
    public void testWriterAppendKeyLengthIsNegative() throws Exception {
        DataOutputBuffer buffer = PowerMockito.mock(DataOutputBuffer.class);
        PowerMockito.when(buffer.getLength()).thenReturn(-1);
        WalFile.Writer writer = PowerMockito.mock(WalFile.Writer.class);
        Field field1 = PowerMockito.field(writer.getClass(), "buffer");
        field1.set(writer, buffer);
        Serializer keySerializer = PowerMockito.mock(Serializer.class);
        PowerMockito.doNothing().when(keySerializer).serialize(Matchers.anyObject());
        Field field2 = PowerMockito.field(writer.getClass(), "keySerializer");
        field2.set(writer, keySerializer);
        Writable key = new WalEntry("1");
        Writable val = new WalEntry("2");
        PowerMockito.doCallRealMethod().when(writer).append(key, val);
        writer.append(key, val);
    }
}
