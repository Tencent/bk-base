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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.HdfsStorage;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockserver.integration.ClientAndServer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * BizHdfsSinkTask Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.hadoop.*", "javax.xml.*", "org.apache.xerces.*"})
public class BizHdfsSinkTask01Test {

    private static ClientAndServer mockServer;


    @BeforeClass
    public static void beforeClass() {
        BasicProps basicProps = BasicProps.getInstance();
        Map<String, String> propsConfig = Maps.newHashMap();
        propsConfig.put(Consts.API_DNS, "127.0.0.1:9999");
        basicProps.addProps(propsConfig);
        mockServer = ClientAndServer.startClientAndServer(9999);
    }

    @AfterClass
    public static void afterClass() {
        mockServer.stop(true);
    }

    @After
    public void after() throws Exception {
        mockServer.reset();
    }


    /**
     * Method: start(Map<String, String> props)
     */
    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testInitHdfsCase0() throws Exception {

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");
        BizHdfsSinkTask task = new BizHdfsSinkTask();

        BizHdfsSinkConfig sinkConfig = new BizHdfsSinkConfig(props);

        Whitebox.setInternalState(task, "config", sinkConfig);

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        PowerMockito.when(mockStorage.exists(Matchers.anyString())).thenThrow(IOException.class);

        PowerMockito.whenNew(HdfsStorage.class).withAnyArguments().thenReturn(mockStorage);

        Whitebox.invokeMethod(task, "initHdfs");
    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testCreateStorage() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        BizHdfsSinkConfig sinkConfig = new BizHdfsSinkConfig(props);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "config", sinkConfig);

        PowerMockito.whenNew(HdfsStorage.class).withAnyArguments().thenThrow(IOException.class);

        Whitebox.invokeMethod(task, "createStorage");

    }


    @Test
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testCreateDir() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        BizHdfsSinkConfig sinkConfig = new BizHdfsSinkConfig(props);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "config", sinkConfig);

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        PowerMockito.when(mockStorage.exists(Matchers.anyString())).thenReturn(false);

        PowerMockito.when(mockStorage.mkdirs(Matchers.anyString())).thenReturn(true);

        Whitebox.setInternalState(task, "storage", mockStorage);

        Whitebox.invokeMethod(task, "createDir", "testDir");
    }


    @Test
    public void testGetMetricTag() throws Exception {
        BizHdfsSinkTask task = new BizHdfsSinkTask();
        String first = Whitebox.invokeMethod(task, "getMetricTag", 20190101111111l);
        assertEquals("null|1546311600", first);

        String second = Whitebox.invokeMethod(task, "getMetricTag", 20190101111111l);
        assertEquals("null|1546311600", second);

        String third = Whitebox.invokeMethod(task, "getMetricTag", 0l);
        assertEquals("null|0", third);
    }


    @Test
    public void testCloseWriters() throws Exception {
        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doThrow(new ConnectException("")).when(topicPartitionWriter).close();

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);

        SinkTaskContext sinkTaskContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.when(sinkTaskContext.assignment()).thenReturn(Sets.newHashSet(topicPartition));

        task.initialize(sinkTaskContext);

        Whitebox.invokeMethod(task, "closeWriters");

        assertNotNull(task);


    }

    @Test
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testSendMonitor() throws Exception {
        Metric mockMetric = PowerMockito.mock(Metric.class);
        PowerMockito.doNothing().when(mockMetric)
                .updateStat(Matchers.anyString(), Matchers.anyString(), Matchers.anyString(),
                        Matchers.anyString(), Matchers.anyInt(), Matchers.anyInt(), Matchers.anyString(),
                        Matchers.anyString());
        PowerMockito.doNothing().when(mockMetric).updateTopicErrInfo(Matchers.anyString(), Matchers.anyList());
        PowerMockito.doNothing().when(mockMetric)
                .updateDelayInfo(Matchers.anyString(), Matchers.anyLong(), Matchers.anyLong(), Matchers.anyLong());

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "");
        props.put("topics.dir", "/topic_dir");
        props.put("logs.dir", "/log_dir");
        props.put("biz.id", "100");
        props.put("table.name", "test_tb");
        props.put("flush.size", "1000");
        props.put("rotate.interval.ms", "10000");
        props.put("multi.paths.rotate.interval.ms", "10000");
        props.put("retry.backoff.ms", "10000");
        props.put("shutdown.timeout.ms", "10000");
        props.put("filename.offset.zero.pad.width", "100");

        BizHdfsSinkConfig sinkConfig = new BizHdfsSinkConfig(props);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "config", sinkConfig);

        Whitebox.setInternalState(task, "metric", mockMetric);

        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(100l);

        BizTopicPartitionWriter mockWriter = PowerMockito.mock(BizTopicPartitionWriter.class);
        PowerMockito.doNothing().when(mockWriter).buffer(Matchers.any());

        ParsedMsg mockMsg = PowerMockito.mock(ParsedMsg.class);
        PowerMockito.when(mockMsg.getMsgSize()).thenReturn(1);
        PowerMockito.when(mockMsg.getErrors()).thenReturn(Lists.newArrayList("ee"));

        PowerMockito.whenNew(ParsedMsg.class).withAnyArguments().thenReturn(mockMsg);

        Whitebox.invokeMethod(task, "sendMonitor", mockRecord, mockWriter, 15800000000l, 15800000000l);

        assertNotNull(task);
    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testWriteHdfsAndWalCase0() throws Exception {
        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doThrow(new ConnectException("")).when(topicPartitionWriter).write(false);

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);

        SinkTaskContext sinkTaskContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.when(sinkTaskContext.assignment()).thenReturn(Sets.newHashSet(topicPartition));

        task.initialize(sinkTaskContext);

        Whitebox.invokeMethod(task, "writeHdfsAndWal", Lists.newArrayList(), -30000l);
        assertNotNull(task);

    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testWriteHdfsAndWalCase1() throws Exception {
        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doThrow(new ConnectException("")).when(topicPartitionWriter).write(false);

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);

        SinkTaskContext sinkTaskContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.when(sinkTaskContext.assignment()).thenReturn(Sets.newHashSet(topicPartition));

        task.initialize(sinkTaskContext);

        Whitebox.invokeMethod(task, "writeHdfsAndWal", Lists.newArrayList(), System.currentTimeMillis());

        assertNotNull(task);
    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testWriteHdfsAndWalCase2() throws Exception {
        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doThrow(new ConnectException("")).when(topicPartitionWriter).write(false);

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);

        SinkTaskContext sinkTaskContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.when(sinkTaskContext.assignment()).thenReturn(Sets.newHashSet(topicPartition));

        task.initialize(sinkTaskContext);

        Whitebox.invokeMethod(task, "writeHdfsAndWal", Lists.newArrayList(), System.currentTimeMillis() + 30000l);

        assertNotNull(task);
    }

    @Test(expected = ConnectException.class)
    @PrepareForTest(BizHdfsSinkTask.class)
    public void testWriteHdfsAndWalCase3() throws Exception {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis()).thenReturn(30000l);

        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new HashMap<>();

        BizTopicPartitionWriter topicPartitionWriter = PowerMockito.mock(BizTopicPartitionWriter.class);

        PowerMockito.doThrow(new ConnectException("")).when(topicPartitionWriter).write(false);

        topicPartitionWriters.put(topicPartition, topicPartitionWriter);

        BizHdfsSinkTask task = new BizHdfsSinkTask();

        Whitebox.setInternalState(task, "topicPartitionWriters", topicPartitionWriters);

        SinkTaskContext sinkTaskContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.when(sinkTaskContext.assignment()).thenReturn(Sets.newHashSet(topicPartition));

        task.initialize(sinkTaskContext);

        Whitebox.invokeMethod(task, "writeHdfsAndWal", Lists.newArrayList(), 0l);

        assertNotNull(task);
    }


}
