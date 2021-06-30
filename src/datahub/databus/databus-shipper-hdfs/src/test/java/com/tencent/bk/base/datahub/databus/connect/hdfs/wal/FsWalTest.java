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

import com.tencent.bk.base.datahub.databus.connect.hdfs.FileUtils;
import com.tencent.bk.base.datahub.databus.connect.hdfs.TestWithMiniDFSCluster;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.HdfsStorage;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.Storage;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FsWalTest extends TestWithMiniDFSCluster {

    private static final String ZERO_PAD_FMT = "%010d";

    private boolean closed;
    private static final String extension = ".avro";

    /**
     * 测试append方法和apply方法正常执行流程
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testWalMultiClient() throws Exception {
        fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC + "/" + PARTITION)), true);
        Storage storage = new HdfsStorage(conf, url);

        final Wal wal1 = storage.wal(topicsDir, TOPIC_PARTITION);
        final Wal wal2 = storage.wal(topicsDir, TOPIC_PARTITION);

        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String tempfile = FileUtils.tempFileName(url, topicsDir, directory, extension);
        final String tempfile2 = FileUtils.tempFileName(url, topicsDir, directory, extension);
        final String commitedFile = FileUtils.committedFileName(url, topicsDir, directory,
                TOPIC_PARTITION, 0, 10, extension,
                ZERO_PAD_FMT);
        final String commitedFile2 = FileUtils.committedFileName(url, topicsDir, directory,
                TOPIC_PARTITION, 11, 20, extension,
                ZERO_PAD_FMT);

        fs.createNewFile(new Path(tempfile));
        fs.createNewFile(new Path(tempfile2));
        fs.createNewFile(new Path("temp2"));

        wal1.acquireLease();
        wal1.append(Wal.beginMarker, "");
        wal1.append(tempfile, commitedFile);
        wal1.append("temp1", commitedFile);
        wal1.append("temp2", "/11");
        wal1.append(Wal.offsetMaker, "hdfs://127.0.0.1:9001/1");
        wal1.append(tempfile2, "hdfs://127.0.0.1:9001/topics/topic/12/_READ");
        wal1.append(Wal.endMarker, "");
        wal1.append(Wal.offsetMaker, "hdfs://127.0.0.1:9001/2");
        wal1.append(Wal.offsetMaker, "11");
        wal1.append(Wal.hdfsMetaMarker, commitedFile + "##0##0");
        wal1.append(Wal.hdfsMetaMarker, commitedFile2 + "##0##0");
        wal1.append(Wal.hdfsMetaMarker, commitedFile2 + "/_READ" + "##0##0");
        wal1.append(Wal.hdfsMetaMarker, "hdfs://127.0.0.1:9001/topics/topic/12/_READ##0##0");
        wal1.append(Wal.endMarker, "");

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // holding the lease for awhile
                    Thread.sleep(3000);
                    closed = true;
                    wal1.close();
                } catch (ConnectException | InterruptedException e) {
                    // Ignored
                }
            }
        });
        thread.start();

        Thread.sleep(5000);
        wal2.acquireLease();
        assertTrue(closed);
        wal2.apply();
        wal2.close();

        assertTrue(fs.exists(new Path(commitedFile)));
        assertFalse(fs.exists(new Path(tempfile)));
        assertTrue(fs.exists(new Path("hdfs://127.0.0.1:9001/topics/topic/12/_READ")));
        assertFalse(fs.exists(new Path(commitedFile2)));
        assertTrue(fs.exists(new Path("/11")));
        assertFalse(fs.exists(new Path("temp2")));
        storage.close();
    }

    /**
     * 测试acquireLease时租借锁被其他线程占用触发异常 Lease is hold by another thread!
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testAppendAnotherThreadHoldLease() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal1 = storage.wal(topicsDir, TOPIC_PARTITION);
        final Wal wal2 = storage.wal(topicsDir, TOPIC_PARTITION);
        wal1.append("temp1", "commit1");
        wal2.append("temp2", "commit2");
    }

    /**
     * 测试acquireLease时 WalFile.createWriter 触发IOException的情况 Cannot acquire lease on Wal
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testAcquireLease() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal1 = storage.wal(topicsDir, TOPIC_PARTITION);
        final Wal wal2 = storage.wal(topicsDir, TOPIC_PARTITION);
        wal1.acquireLease();
        wal2.acquireLease();
    }

    /**
     * 测试truncate方法
     *
     * @throws Exception
     */
    @Test
    public void testTruncate() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        FsWal wal = new FsWal("/logs", tp, storage);
        wal.append("a", "b");
        assertTrue("WAL file should exist after append",
                storage.exists("/logs/mytopic/123/log"));
        wal.truncate();
        assertFalse("WAL file should not exist after truncate",
                storage.exists("/logs/mytopic/123/log"));
        assertTrue("Rotated WAL file should exist after truncate",
                storage.exists("/logs/mytopic/123/log.1"));
        wal.append("c", "d");
        assertTrue("WAL file should be recreated after truncate + append",
                storage.exists("/logs/mytopic/123/log"));
        assertTrue("Rotated WAL file should exist after truncate + append",
                storage.exists("/logs/mytopic/123/log.1"));
    }

    /**
     * 测试truncate方法和delete方法
     *
     * @throws IOException
     */
    @Test
    public void testTruncateAndDelete() throws IOException {
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        String logFile = wal.getLogFile();
        wal.acquireLease();
        wal.truncate();
        assertTrue(fs.exists(new Path(logFile + ".1")));
        assertFalse(fs.exists(new Path(logFile)));

        wal.deleteBadLogFile();
        assertFalse(fs.exists(new Path(logFile + ".1")));
    }

    /**
     * 测试apply方法中 !storage.exists(logFile) 为true时直接 return; 的情况
     *
     * @throws Exception
     */
    @Test
    public void testApplyStorageNotExistsLogFile() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        String logFile = wal.getLogFile();
        wal.apply();
        assertFalse(fs.exists(new Path(logFile)));
        wal.close();
    }

    /**
     * 测试append方法中触发IOException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataException.class)
    public void testAppendMockException() throws Exception {
        WalFile.Writer writer = PowerMockito.mock(WalFile.Writer.class);
        PowerMockito.doThrow(new IOException()).when(writer).append(Matchers.anyObject(), Matchers.anyObject());
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        Field field = wal.getClass().asSubclass(FsWal.class).getDeclaredField("writer");
        field.setAccessible(true);
        field.set(wal, writer);
        wal.append("xx", "xx");
    }

    /**
     * 测试apply方法中触发AssertionError的情况
     *
     * @throws Exception
     */
    @Test(expected = AssertionError.class)
    public void testApplyAssertionError() throws Exception {
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String file1 = FileUtils.fileName(url, topicsDir, directory, "log/file1");
        final String file2 = FileUtils.fileName(url, topicsDir, directory, "log/file2");
        fs.createNewFile(new Path(file1));
        fs.createNewFile(new Path(file2));
        fs.createNewFile(new Path("hdfs://localhost:9001/" + topicsDir + "/" + directory + "/log"));
        Storage storage = new HdfsStorage(conf, "hdfs://localhost:9001/");
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        wal.apply();
    }

    /**
     * 测试apply方法中Found empty log file at path的情况
     *
     * @throws Exception
     */
    @Test
    public void testApplyEmptyLogFileAtPath() throws Exception {
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String file1 = FileUtils.fileName(url, topicsDir, directory, "file1");
        final String file2 = FileUtils.fileName(url, topicsDir, directory, "file2");
        fs.createNewFile(new Path(file1));
        fs.createNewFile(new Path(file2));
        fs.createNewFile(new Path("hdfs://localhost:9001/" + topicsDir + "/" + directory + "/log"));
        Storage storage = new HdfsStorage(conf, "hdfs://localhost:9001/");
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        wal.apply();
        Field field = wal.getClass().asSubclass(FsWal.class).getDeclaredField("reader");
        field.setAccessible(true);
        assertNull(field.get(wal));
    }

    /**
     * 测试apply方法中触发IOException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataException.class)
    public void testApplyCatchIOException() throws Exception {
        HdfsStorage hdfsStorage = PowerMockito.mock(HdfsStorage.class);
        PowerMockito.when(hdfsStorage.exists(Matchers.anyObject())).thenThrow(IOException.class);
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        Field field = wal.getClass().asSubclass(FsWal.class).getDeclaredField("storage");
        field.setAccessible(true);
        field.set(wal, hdfsStorage);
        wal.apply();
    }

    /**
     * 测试close方法中触发IOException的情况
     *
     * @throws Exception
     */
    @Test(expected = DataException.class)
    public void testClose() throws Exception {
        WalFile.Writer writer = PowerMockito.mock(WalFile.Writer.class);
        PowerMockito.doThrow(new IOException()).when(writer).close();
        WalFile.Reader reader = PowerMockito.mock(WalFile.Reader.class);
        PowerMockito.doThrow(new IOException()).when(reader).close();
        Storage storage = new HdfsStorage(conf, url);
        final Wal wal = storage.wal(topicsDir, TOPIC_PARTITION);
        Field field1 = wal.getClass().asSubclass(FsWal.class).getDeclaredField("writer");
        field1.setAccessible(true);
        field1.set(wal, writer);
        Field field2 = wal.getClass().asSubclass(FsWal.class).getDeclaredField("reader");
        field2.setAccessible(true);
        field2.set(wal, reader);
        wal.close();
    }
}

