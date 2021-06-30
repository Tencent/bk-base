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

import com.tencent.bk.base.datahub.databus.connect.hdfs.BizHdfsSinkConfig;
import com.tencent.bk.base.datahub.databus.connect.hdfs.FileUtils;
import com.tencent.bk.base.datahub.databus.connect.hdfs.TestWithMiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Options;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class WalFileTest extends TestWithMiniDFSCluster {

    private static final String extension = ".avro";
    private static final String ZERO_PAD_FMT = "%010d";

    /**
     * 测试append方法
     *
     * @throws Exception
     */
    @Test
    public void testAppend() throws Exception {
        Map<String, String> props = createProps();
        BizHdfsSinkConfig connectorConfig = new BizHdfsSinkConfig(props);
        String topicsDir = connectorConfig.getString(BizHdfsSinkConfig.TOPICS_DIR_CONFIG);
        String topic = "topic";
        int partition = 0;
        TopicPartition topicPart = new TopicPartition(topic, partition);
        Path file = new Path(FileUtils.logFileName(url, topicsDir, topicPart));
        WalFile.Writer writer = WalFile.createWriter(conf, WalFile.Writer.file(file));
        WalEntry key1 = new WalEntry("key1");
        WalEntry val1 = new WalEntry("val1");
        WalEntry key2 = new WalEntry("key2");
        WalEntry val2 = new WalEntry("val2");
        writer.append(key1, val1);
        writer.append(key2, val2);
        writer.close();
        verify2Values(file);

        writer = WalFile.createWriter(conf, WalFile.Writer.file(file), WalFile.Writer.appendIfExists(true));
        WalEntry key3 = new WalEntry("key3");
        WalEntry val3 = new WalEntry("val3");
        WalEntry key4 = new WalEntry("key4");
        WalEntry val4 = new WalEntry("val4");
        writer.append(key3, val3);
        writer.append(key4, val4);
        writer.hsync();
        writer.close();

        verifyAll4Values(file);
        fs.deleteOnExit(file);
    }

    /**
     * 测试createWriter触发IllegalArgumentException的情况：file must be specified
     *
     * @throws IOException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWriterIllegalArgumentException() throws IOException {
        WalFile.createWriter(conf, WalFile.Writer.appendIfExists(true));
    }


    /**
     * 测试Reader的sync方法
     *
     * @throws Exception
     */
    @Test
    public void testReaderSync1() throws Exception {
        String logFile = FileUtils.logFileName(url, logsDir, TOPIC_PARTITION);
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String tempFile = FileUtils.tempFileName(url, topicsDir, directory, extension);
        final String committedFile = FileUtils.committedFileName(url, topicsDir, directory,
                TOPIC_PARTITION, 0, 10, extension,
                ZERO_PAD_FMT);
        fs.createNewFile(new Path(tempFile));
        WalFile.Writer writer = WalFile
                .createWriter(conf, WalFile.Writer.file(new Path(logFile)), WalFile.Writer.appendIfExists(true));
        WalEntry key = new WalEntry(tempFile);
        WalEntry value = new WalEntry(committedFile);
        writer.append(key, value);
        writer.hsync();
        writer.hflush();
        long end = writer.getLength();
        writer.close();
        WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(new Path(logFile)));
        reader.sync(end);
        assertEquals(end, reader.getPosition());
        assertEquals("hdfs://127.0.0.1:9001/logs/topic/12/log", reader.toString());
    }

    /**
     * 测试Reader的sync方法
     *
     * @throws Exception
     */
    @Test
    public void testReaderSync2() throws Exception {
        String logFile = FileUtils.logFileName(url, logsDir, TOPIC_PARTITION);
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String tempFile = FileUtils.tempFileName(url, topicsDir, directory, extension);
        final String committedFile = FileUtils.committedFileName(url, topicsDir, directory,
                TOPIC_PARTITION, 0, 10, extension,
                ZERO_PAD_FMT);
        fs.createNewFile(new Path(tempFile));
        WalFile.Writer writer = WalFile
                .createWriter(conf, WalFile.Writer.file(new Path(logFile)), WalFile.Writer.appendIfExists(true));
        WalEntry key = new WalEntry(tempFile);
        WalEntry value = new WalEntry(committedFile);
        writer.append(key, value);
        writer.close();
        WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(new Path(logFile)));
        assertFalse(reader.syncSeen());
        reader.sync(1);
        assertTrue(reader.syncSeen());
    }

    /**
     * 测试Reader的sync方法
     *
     * @throws Exception
     */
    @Test
    public void testReaderSync3() throws Exception {
        String logFile = FileUtils.logFileName(url, logsDir, TOPIC_PARTITION);
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String tempFile = FileUtils.tempFileName(url, topicsDir, directory, extension);
        final String committedFile = FileUtils.committedFileName(url, topicsDir, directory,
                TOPIC_PARTITION, 0, 10, extension,
                ZERO_PAD_FMT);
        fs.createNewFile(new Path(tempFile));
        WalFile.Writer writer = WalFile
                .createWriter(conf, WalFile.Writer.file(new Path(logFile)), WalFile.Writer.appendIfExists(true));
        WalEntry key = new WalEntry(tempFile);
        WalEntry value = new WalEntry(committedFile);
        writer.append(key, value);
        long end = writer.getLength();
        writer.close();
        WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(new Path(logFile)));
        reader.sync(100);
        System.out.println(reader);
        assertEquals(end, reader.getPosition());
    }

    /**
     * 测试Reader触发IllegalArgumentException的情况：file must be specified
     *
     * @throws IOException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testReaderIllegalArgumentException() throws IOException {
        class mockOption extends Options.PathOption
                implements WalFile.Reader.Option {

            private mockOption(Path value) {
                super(value);
            }
        }
        new WalFile.Reader(conf, new mockOption(new Path("xx")));
    }

    private void verify2Values(Path file) throws IOException {
        WalEntry key1 = new WalEntry("key1");
        WalEntry val1 = new WalEntry("val1");
        WalEntry key2 = new WalEntry("key2");
        WalEntry val2 = new WalEntry("val2");

        WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(file));
        assertEquals(key1.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val1.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());
        assertEquals(key2.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val2.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());
        assertNull(reader.next((Object) null));
        reader.close();
    }

    private void verifyAll4Values(Path file) throws IOException {
        WalEntry key1 = new WalEntry("key1");
        WalEntry val1 = new WalEntry("val1");
        WalEntry key2 = new WalEntry("key2");
        WalEntry val2 = new WalEntry("val2");
        WalEntry key3 = new WalEntry("key3");
        WalEntry val3 = new WalEntry("val3");
        WalEntry key4 = new WalEntry("key4");
        WalEntry val4 = new WalEntry("val4");

        WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(file));
        assertEquals(key1.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val1.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());
        assertEquals(key2.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val2.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());

        assertEquals(key3.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val3.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());
        assertEquals(key4.getName(), ((WalEntry) reader.next((Object) null)).getName());
        assertEquals(val4.getName(), ((WalEntry) reader.getCurrentValue((Object) null)).getName());
        assertNull(reader.next((Object) null));
        reader.close();
    }
}

