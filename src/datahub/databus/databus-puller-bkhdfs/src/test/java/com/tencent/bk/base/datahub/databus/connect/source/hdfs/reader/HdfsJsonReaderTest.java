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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class HdfsJsonReaderTest {

    @Test
    public void testConstruction() throws Exception {
        Path path = mock(Path.class);
        FileSystem fs = mock(FileSystem.class);
        String json = "{\"dfs.nameservices\": \"testHdfs\"}";
        StringReader stringReader = new StringReader(json);
        ReaderInputStream inputStream = new ReaderInputStream(stringReader);
        TestInputStream testInputStream = new TestInputStream(inputStream);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(testInputStream);
        when(fs.open(path)).thenReturn(fsDataInputStream);
        String[] columns = {"dfs.nameservices"};
        HdfsJsonReader reader = new HdfsJsonReader(fs, path, columns);
        Field field = reader.getClass().getDeclaredField("br");
        field.setAccessible(true);
        BufferedReader bufferedReader = (BufferedReader) field.get(reader);
        Assert.assertNotNull(bufferedReader);
    }

    @Test
    public void testSeekTo() throws Exception {
        Path path = mock(Path.class);
        FileSystem fs = mock(FileSystem.class);
        String json = "{\"dfs.nameservices\": \"testHdfs\"}\n{\"dfs.nameservices\": \"testHdfs1\"}\n{\"dfs"
                + ".nameservices\": \"testHdfs2\"}";
        StringReader stringReader = new StringReader(json);
        BufferedReader bufferedReader = new BufferedReader(stringReader);
        ReaderInputStream inputStream = new ReaderInputStream(bufferedReader);
        TestInputStream testInputStream = new TestInputStream(inputStream);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(testInputStream);
        when(fs.open(path)).thenReturn(fsDataInputStream);
        String[] columns = {"dfs.nameservices"};
        HdfsJsonReader reader = new HdfsJsonReader(fs, path, columns);
        reader.seekTo(1);
        Assert.assertEquals(1, reader.processedCnt());
    }

    @Test
    public void testReadRecords() throws Exception {
        Path path = mock(Path.class);
        FileSystem fs = mock(FileSystem.class);
        String json = "{\"dfs.nameservices\": \"testHdfs\"}\n{\"dfs.nameservices\": \"testHdfs1\"}\n{\"dfs"
                + ".nameservices\": \"testHdfs2\"}";
        StringReader stringReader = new StringReader(json);
        BufferedReader bufferedReader = new BufferedReader(stringReader);
        ReaderInputStream inputStream = new ReaderInputStream(bufferedReader);
        TestInputStream testInputStream = new TestInputStream(inputStream);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(testInputStream);
        when(fs.open(path)).thenReturn(fsDataInputStream);
        String[] columns = {"dfs.nameservices"};
        HdfsJsonReader reader = new HdfsJsonReader(fs, path, columns);
        List<List<Object>> records = reader.readRecords(3);
        Assert.assertEquals(3, records.size());
        Assert.assertEquals("testHdfs2", records.get(2).get(0));
    }

    public static class TestInputStream extends InputStream implements Seekable, PositionedReadable {


        private ReaderInputStream readerInputStream;

        public TestInputStream(ReaderInputStream readerInputStream) {
            this.readerInputStream = readerInputStream;
        }

        @Override
        public void seek(long l) throws IOException {

        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public boolean seekToNewSource(long l) throws IOException {
            return false;
        }

        @Override
        public int read(long l, byte[] bytes, int i, int i1) throws IOException {
            return this.readerInputStream.read(bytes);
        }

        @Override
        public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {
            this.readerInputStream.read(bytes, i, i1);
        }

        @Override
        public void readFully(long l, byte[] bytes) throws IOException {
            this.readerInputStream.read(bytes);
        }

        @Override
        public int read() throws IOException {
            return this.readerInputStream.read();
        }
    }
}
