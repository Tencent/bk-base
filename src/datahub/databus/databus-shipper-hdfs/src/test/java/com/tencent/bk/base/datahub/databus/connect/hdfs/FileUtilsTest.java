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

import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.HdfsStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.*;

/**
 * FileUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
public class FileUtilsTest {

    private static Configuration conf;
    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void before() {
        conf = new Configuration();
        try {
            cluster = new MiniDFSCluster.Builder(conf).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
        cluster.shutdown();
    }


    @Test
    public void testConstructor() {
        FileUtils fileUtils = new FileUtils();
        assertNotNull(fileUtils);
    }

    @Test
    public void testFileName() {
        String fileName = FileUtils.fileName("url", "topic", "dir", "name");
        assertEquals("url/topic/dir/name", fileName);
    }

    @Test
    public void testDirName() {
        String dirName = FileUtils.directoryName("url", "topic", "dir");
        assertEquals("url/topic/dir", dirName);
    }

    @Test
    public void testTempFileName() {
        String fileName = FileUtils.tempFileName("url", "topic", "dir", "ext");
        assertNotNull(fileName);
    }

    @Test
    public void testCommittedFileName() {
        TopicPartition topicPartition = new TopicPartition("topic", 1);
        String fileName = FileUtils.committedFileName("url", "topicDir", "dir", topicPartition, 0l, 100l, "ext", "x");
        System.out.println(fileName);
    }


    @Test
    public void testCleanOffsetRecordFilesCase0() throws IOException {
        final byte[] data = "foo".getBytes();

        FileSystem fs = FileSystem.get(conf);

        OutputStream out = fs.create(new Path("/100"));

        out.write(data);

        out.close();

        fs.close();

        HdfsStorage hdfsStorage = new HdfsStorage(conf, "/100");

        assertTrue(hdfsStorage.exists("/100"));

        FileUtils.cleanOffsetRecordFiles(hdfsStorage, 101, "/100");

        assertFalse(hdfsStorage.exists("/100"));

    }

    @Test
    public void testCleanOffsetRecordFilesCase1() throws IOException {

        final byte[] data = "foo".getBytes();

        FileSystem fs = FileSystem.get(conf);

        OutputStream out = fs.create(new Path("/102"));

        out.write(data);

        out.close();

        fs.close();

        HdfsStorage hdfsStorage = new HdfsStorage(conf, "/102");

        assertTrue(hdfsStorage.exists("/102"));

        FileUtils.cleanOffsetRecordFiles(hdfsStorage, 101, "/102");

        assertTrue(hdfsStorage.exists("/102"));

    }


    @Test
    public void testGetMaxOffsetFromRecordDirCase0() throws IOException {
        final byte[] data = "foo".getBytes();

        FileSystem fs = FileSystem.get(conf);

        OutputStream out = fs.create(new Path("/102"));

        out.write(data);

        out.close();

        fs.close();

        HdfsStorage hdfsStorage = new HdfsStorage(conf, "/102");

        long maxOffSet = FileUtils.getMaxOffsetFromRecordDir(hdfsStorage, "/102/");

        assertEquals(103, maxOffSet);

    }

    @Test
    public void testGetMaxOffsetFromRecordDirCase1() throws IOException {
        final byte[] data = "foo".getBytes();

        FileSystem fs = FileSystem.get(conf);

        OutputStream out = fs.create(new Path("/-2"));

        out.write(data);

        out.close();

        fs.close();

        HdfsStorage hdfsStorage = new HdfsStorage(conf, "/-2");

        long maxOffSet = FileUtils.getMaxOffsetFromRecordDir(hdfsStorage, "/-2/");

        assertEquals(-1, maxOffSet);

    }

    @Test
    public void testGetMaxOffsetFromRecordDirCase2() throws IOException {

        HdfsStorage mockStorage = PowerMockito.mock(HdfsStorage.class);

        PowerMockito.when(mockStorage.listStatus("/101/")).thenReturn(new FileStatus[]{});

        long maxOffSet = FileUtils.getMaxOffsetFromRecordDir(mockStorage, "/101/");

        assertEquals(-1, maxOffSet);

    }

    @Test
    public void testGetOffsetRecordDirCase0() {
        String offSetDir = FileUtils
                .getOffsetRecordDir("t_url", "topicDir", "hdfs-inner2-M", "c_name", new TopicPartition("tt", 1));
        assertEquals("t_url/topicDir/inner2/__offset__/c_name/1", offSetDir);
    }

    @Test
    public void testGetOffsetRecordDirCase1() {
        String offSetDir = FileUtils
                .getOffsetRecordDir("t_url", "topicDir", "hdfs", "c_name", new TopicPartition("tt", 1));
        assertEquals("t_url/topicDir/hdfs/__offset__/c_name/1", offSetDir);

    }

    @Test
    public void testGetOldOffsetRecordDir() {
        String offSetDir = FileUtils
                .getOldOffsetRecordDir("t_url", "topicDir", "hdfs", "c_name", new TopicPartition("tt", 1));
        assertEquals("t_url/topicDir/__offset__/hdfs/c_name/tt/1", offSetDir);
    }


}
