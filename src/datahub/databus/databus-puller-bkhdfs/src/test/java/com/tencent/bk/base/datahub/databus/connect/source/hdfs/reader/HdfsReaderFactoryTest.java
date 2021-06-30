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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader.HdfsJsonReaderTest.TestInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class HdfsReaderFactoryTest {

    @Test
    public void testCreateRecordsReader() throws IOException {
        Path path = mock(Path.class);
        when(path.getName()).thenReturn("part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.snappy");
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
        Configuration conf = new Configuration();
        RecordsReader recordsReader = HdfsReaderFactory.createRecordsReader(fs, conf, path, columns);
        Assert.assertNull(recordsReader);
    }

    @Test
    public void testCreateJsonRecordsReader() throws IOException {
        Path path = mock(Path.class);
        when(path.getName()).thenReturn("part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.json");
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
        Configuration conf = new Configuration();
        RecordsReader recordsReader = HdfsReaderFactory.createRecordsReader(fs, conf, path, columns);
        Assert.assertEquals(recordsReader.getClass(), HdfsJsonReader.class);
    }

    @Test
    @PrepareForTest({ParquetReader.class})
    public void testCreateParquetRecordsReader() throws IOException {
        Path path = mock(Path.class);
        Path parent = mock(Path.class);
        when(path.getParent()).thenReturn(parent);
        when(path.getName()).thenReturn("part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.parquet");
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
        Configuration conf = new Configuration();
        when(path.getFileSystem(conf)).thenReturn(fs);
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getPath()).thenReturn(path);
        FileStatus[] fileStatuses = new FileStatus[1];
        fileStatuses[0] = fileStatus;
        when(fs.listStatus(path, HiddenFileFilter.INSTANCE)).thenReturn(fileStatuses);

        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        ParquetReader parquetReader = mock(ParquetReader.class);
        when(builder.build()).thenReturn(parquetReader);
        RecordsReader recordsReader = HdfsReaderFactory.createRecordsReader(fs, conf, path, columns);
        Assert.assertEquals(recordsReader.getClass(), HdfsParqueReader.class);
    }
}
