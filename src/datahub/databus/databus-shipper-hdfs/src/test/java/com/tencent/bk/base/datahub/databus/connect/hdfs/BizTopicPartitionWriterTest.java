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

import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.HdfsStorage;
import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.Wal;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * BizTopicPartitionWriter Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
public class BizTopicPartitionWriterTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }


    @Test(expected = ConnectException.class)
    public void testRecoverWithRetryCase01() throws Exception {
        Wal mockWal = PowerMockito.mock(Wal.class);

        PowerMockito.doNothing().when(mockWal).deleteBadLogFile();

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        BizDataPartitioner bizDataPartitioner = new BizDataPartitioner();

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
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

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        BizTopicPartitionWriter writer = new BizTopicPartitionWriter(topicPartition, mockStorage, bizDataPartitioner,
                sinkConfig, mockContext);

        Whitebox.setInternalState(writer, "retryLimit", 0);
        Whitebox.setInternalState(writer, "wal", mockWal);

        writer.recoverWithRetry();

    }

    @Test
    public void testRecoverWithRetryCase02() throws Exception {
        Wal mockWal = PowerMockito.mock(Wal.class);

        PowerMockito.doNothing().when(mockWal).deleteBadLogFile();

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        BizDataPartitioner bizDataPartitioner = new BizDataPartitioner();

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
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

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        BizTopicPartitionWriter writer = new BizTopicPartitionWriter(topicPartition, mockStorage, bizDataPartitioner,
                sinkConfig, mockContext);

        Whitebox.setInternalState(writer, "retryLimit", 2);
        Whitebox.setInternalState(writer, "sleepIntervalMs", 2000L);
        Whitebox.setInternalState(writer, "wal", mockWal);

        writer.recoverWithRetry();

        Object state = Whitebox.getInternalState(writer, "state");
        System.out.println("currnt inner State:" + state);

        assertEquals("WRITE_STARTED", state.toString());

    }

    @Test(expected = ConnectException.class)
    public void testRecoverWithRetryCase03() throws Exception {
        Wal mockWal = PowerMockito.mock(Wal.class);

        PowerMockito.doNothing().when(mockWal).deleteBadLogFile();

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        TopicPartition topicPartition = new TopicPartition("t_topic", 1);

        BizDataPartitioner bizDataPartitioner = new BizDataPartitioner();

        Map<String, String> props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "100_xxx_test");
        props.put(BkConfig.GROUP_ID, "test_group_id");
        props.put("hdfs.url", "t_url");
        props.put("hadoop.conf.dir", "/2019");
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

        SinkTaskContext mockContext = PowerMockito.mock(SinkTaskContext.class);

        PowerMockito.doThrow(new RuntimeException()).when(mockContext).pause(Matchers.any());

        BizTopicPartitionWriter writer = new BizTopicPartitionWriter(topicPartition, mockStorage, bizDataPartitioner,
                sinkConfig, mockContext);

        Whitebox.setInternalState(writer, "retryLimit", 2);
        Whitebox.setInternalState(writer, "sleepIntervalMs", 2000L);
        Whitebox.setInternalState(writer, "wal", mockWal);

        writer.recoverWithRetry();

        Object state = Whitebox.getInternalState(writer, "state");
        System.out.println("currnt inner State:" + state);

        assertEquals("WRITE_STARTED", state.toString());

    }

} 
