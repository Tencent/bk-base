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
package com.tencent.bk.base.datahub.databus.connect.source.hdfs;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tencent.bk.base.datahub.databus.connect.source.hdfs.bean.BkhdfsPullerTask;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
public class BkHdfsSourceTaskTest {


    private static Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkHdfsSourceConfig.WORKER_ID, "1");
        props.put(BkHdfsSourceConfig.MSG_BATCH_RECORDS, "100");
        props.put("worker.num", "50");
        return props;
    }

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS,
                "data=string,dataid=string,datetime=string,timezone=string,type=string,utctime=string");
        rtProps.put(BkConfig.RT_ID, "591_test_batch");
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        return rtProps;
    }

    @Test
    public void testStartTask() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> props = getProps();
        BasicProps.getInstance().getClusterProps().put("read.max.records", "100000");
        BasicProps.getInstance().getClusterProps().put("read.parquet.max.records", "200000");
        BasicProps.getInstance().getClusterProps().put("geog.area", "inland");

        BkHdfsSourceTask task = new BkHdfsSourceTask();
        task.start(props);
        Field configField = task.getClass().getDeclaredField("config");
        configField.setAccessible(true);
        BkHdfsSourceConfig config = (BkHdfsSourceConfig) configField.get(task);
        Assert.assertEquals(props.get(BkConfig.GROUP_ID), config.cluster);
        Assert.assertEquals(Integer.parseInt(props.get(BkHdfsSourceConfig.MSG_BATCH_RECORDS)), config.msgBatchRecords);
        Assert.assertEquals(Integer.parseInt(props.get(BkHdfsSourceConfig.WORKER_ID)), config.workerId);
        Assert.assertEquals(props.get(BkConfig.CONNECTOR_NAME), config.connector);

        Field workerNumField = task.getClass().getDeclaredField("workerNum");
        workerNumField.setAccessible(true);
        int workerNum = (int) workerNumField.get(task);
        Assert.assertEquals(50, workerNum);

        Field jsonReadMaxRecordsField = task.getClass().getDeclaredField("jsonReadMaxRecords");
        jsonReadMaxRecordsField.setAccessible(true);
        long jsonReadMaxRecords = (long) jsonReadMaxRecordsField.get(task);
        Assert.assertEquals(100000, jsonReadMaxRecords);

        Field parquetReadMaxRecordsField = task.getClass().getDeclaredField("parquetReadMaxRecords");
        parquetReadMaxRecordsField.setAccessible(true);
        long parquetReadMaxRecords = (long) parquetReadMaxRecordsField.get(task);
        Assert.assertEquals(200000, parquetReadMaxRecords);

        Field geogAreaField = task.getClass().getDeclaredField("geogArea");
        geogAreaField.setAccessible(true);
        String geogAreaRecords = (String) geogAreaField.get(task);
        Assert.assertEquals("inland", geogAreaRecords);
    }

    @Test
    @PrepareForTest({HttpUtils.class, ParquetReader.class})
    public void testReadFile() throws IOException {
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("591_test_batch")).thenReturn(rtProps);

        BasicProps.getInstance().getClusterProps().put("read.max.records", "100000");
        BasicProps.getInstance().getClusterProps().put("read.parquet.max.records", "200000");
        BasicProps.getInstance().getClusterProps().put("geog.area", "inland");

        Map<String, Object> taskMap = new HashMap<>();
        taskMap.put("id", "123456");
        taskMap.put("hdfs_custom_property", "{\"dfs.nameservices\": \"testHdfs\"}");
        taskMap.put("hdfs_conf_dir", "");
        taskMap.put("kafka_bs", "kafka.bootstrap.url:9092");
        taskMap.put("rt_id", "591_test_batch");
        taskMap.put("data_dir", "file:///part-00000-eafb8aef-816e-4014-ad1b-3253519a39a8-c000.snappy.parquet");
        taskMap.put("status", "");
        BkhdfsPullerTask pullerTask = new BkhdfsPullerTask(taskMap, 1);

        Configuration conf = new Configuration();
        conf.set("fs.file.impl.disable.cache", "true");
        Path path = new Path("file:///part-00000-eafb8aef-816e-4014-ad1b-3253519a39a8-c000.snappy.parquet");
        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        AvroParquetReader<GenericRecord> avroParquetReader = new AvroParquetReader<>(conf, path);
        when(builder.build()).thenReturn(avroParquetReader);

        BkHdfsSourceTask task = new BkHdfsSourceTask();
        task.start(props);

        List<List<Object>> buffer = new ArrayList<>();
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getPath()).thenReturn(path);

        Producer<String, String> producer = mock(Producer.class);

        long lineNum = task.readFile(pullerTask, fileStatus, null, buffer, producer);
        Assert.assertEquals(5, buffer.size());
        Assert.assertEquals(5, lineNum);
    }

    @Test
    @PrepareForTest({HttpUtils.class, ParquetReader.class})
    public void testExecuteTask() throws IOException {
        BasicProps.getInstance().getProducerProps().put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "4194304");
        Map<String, String> props = getProps();
        Map<String, String> rtProps = getRtProps();
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("591_test_batch")).thenReturn(rtProps);

        BasicProps.getInstance().getClusterProps().put("read.max.records", "100000");
        BasicProps.getInstance().getClusterProps().put("read.parquet.max.records", "200000");
        BasicProps.getInstance().getClusterProps().put("geog.area", "inland");

        Map<String, Object> taskMap = new HashMap<>();
        taskMap.put("id", "123456");
        taskMap.put("hdfs_custom_property", "{\"fs.defaultFS\": \"file:///\",\"fs.file.impl.disable.cache\":\"true\"}");
        taskMap.put("hdfs_conf_dir", "");
        taskMap.put("kafka_bs", "127.0.0.1:9092");
        taskMap.put("rt_id", "591_test_batch");
        taskMap.put("data_dir", "file:///part-00000-eafb8aef-816e-4014-ad1b-3253519a39a8-c000.snappy.parquet");
        taskMap.put("status", "");
        BkhdfsPullerTask pullerTask = new BkhdfsPullerTask(taskMap, 1);

        Configuration conf = new Configuration();
        conf.set("fs.file.impl.disable.cache", "true");
        Path path = new Path("file:///part-00000-eafb8aef-816e-4014-ad1b-3253519a39a8-c000.snappy.parquet");
        PowerMockito.mockStatic(ParquetReader.class);
        ParquetReader.Builder builder = mock(ParquetReader.Builder.class);
        PowerMockito.when(ParquetReader.builder(anyObject(), anyObject())).thenReturn(builder);
        AvroParquetReader<GenericRecord> avroParquetReader = new AvroParquetReader<>(conf, path);
        when(builder.build()).thenReturn(avroParquetReader);

        BkHdfsSourceTask task = spy(new BkHdfsSourceTask());
        task.start(props);
        task.executeTask(pullerTask);

        verify(task, atLeast(1)).readFile(anyObject(), anyObject(), anyObject(), anyObject(), anyObject());
        verify(task, atLeast(1)).composeRecordAndSend(anyObject(), anyObject(), anyObject());
        verify(task, atLeast(1))
                .sendRecordsToKafka(anyObject(), anyString(), anyString(), anyString());
    }


}
