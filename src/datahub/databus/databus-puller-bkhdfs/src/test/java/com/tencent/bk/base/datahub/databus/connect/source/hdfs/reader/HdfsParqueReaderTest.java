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

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
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
public class HdfsParqueReaderTest {

    @Test
    @PrepareForTest({ParquetReader.class})
    public void testConstruction() throws Exception {
        String[] columns = {"ts", "balance", "salary", "info"};
        Configuration conf = new Configuration();
        Path path = mock(Path.class);

        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        ParquetReader parquetReader = mock(ParquetReader.class);
        when(builder.build()).thenReturn(parquetReader);

        HdfsParqueReader reader = new HdfsParqueReader(conf, path, columns);
        Field field = reader.getClass().getDeclaredField("reader");
        field.setAccessible(true);
        ParquetReader<GenericRecord> innerParquetReader = (ParquetReader) field.get(reader);
        Assert.assertNotNull(innerParquetReader);
    }

    @Test
    @PrepareForTest({ParquetReader.class})
    public void testSeekTo() throws Exception {
        String[] columns = {"ts", "balance", "salary", "info"};
        Configuration conf = new Configuration();
        conf.set("fs.file.impl.disable.cache", "true");
        Path path = new Path("file:///9a1f4abd-f48a-4640-8828-6d31729ca5bf.parquet");
        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        AvroParquetReader<GenericRecord> avroParquetReader = new AvroParquetReader<>(conf, path);
        when(builder.build()).thenReturn(avroParquetReader);

        Path mockPath = mock(Path.class);
        HdfsParqueReader reader = new HdfsParqueReader(conf, mockPath, columns);
        reader.seekTo(1);
        Assert.assertEquals(1, reader.processedCnt());
    }

    @Test
    @PrepareForTest({ParquetReader.class})
    public void testReadRecords() throws Exception {
        String[] columns = {"ts", "balance", "salary", "info"};
        Configuration conf = new Configuration();
        conf.set("fs.file.impl.disable.cache", "true");
        Path path = new Path("file:///9a1f4abd-f48a-4640-8828-6d31729ca5bf.parquet");
        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        AvroParquetReader<GenericRecord> avroParquetReader = new AvroParquetReader<>(conf, path);
        when(builder.build()).thenReturn(avroParquetReader);

        Path mockPath = mock(Path.class);
        HdfsParqueReader reader = new HdfsParqueReader(conf, mockPath, columns);

        List<List<Object>> records = reader.readRecords(3);
        Assert.assertEquals(3, records.size());
    }

}
