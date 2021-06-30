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


package com.tencent.bk.base.datahub.databus.connect.hdfs.storage;

import com.tencent.bk.base.datahub.databus.connect.hdfs.FileUtils;
import com.tencent.bk.base.datahub.databus.connect.hdfs.TestWithMiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.lang.reflect.Field;

public class HdfsStorageTest extends TestWithMiniDFSCluster {

    /**
     * 测试listStatus方法正常情况
     *
     * @throws IOException
     */
    @Test
    public void testListStatus() throws IOException {
        Storage storage = new HdfsStorage(conf, url);
        String directory = TOPIC + "/" + String.valueOf(PARTITION);
        final String file1 = FileUtils.fileName(url, topicsDir, directory, "file1");
        final String file2 = FileUtils.fileName(url, topicsDir, directory, "file2");
        fs.createNewFile(new Path(file1));
        fs.createNewFile(new Path(file2));
        FileStatus[] statuses = storage
                .listStatus("hdfs://localhost:9001/" + topicsDir + "/" + directory + "/", new GlobFilter("file\\d"));
        Assert.assertEquals(2, statuses.length);
        storage.append("", "");
        storage.close();
    }

    /**
     * 测试commit方法
     *
     * @throws IOException
     */
    @Test
    public void testCommit() throws IOException {
        Storage storage = new HdfsStorage(conf, url);
        storage.commit("source", "source");
        Assert.assertFalse(fs.exists(new Path("source")));

        fs.createNewFile(new Path("source"));
        storage.commit("source", "target");
        Assert.assertTrue(fs.exists(new Path("target")));

        storage.commit("source", "target");
        Assert.assertTrue(fs.exists(new Path("target")));
    }

    /**
     * 测试commit方法触发ConnectException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testCommitFailed() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        FileSystem fs = PowerMockito.mock(FileSystem.class);
        PowerMockito.doThrow(new IOException("")).when(fs).exists(new Path("xx"));
        Field field = storage.getClass().asSubclass(HdfsStorage.class).getDeclaredField("fs");
        field.setAccessible(true);
        field.set(storage, fs);
        storage.commit("xx", "xxx");
    }

    /**
     * 测试commit方法触发ConnectException的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    public void testDelete() throws Exception {
        Storage storage = new HdfsStorage(conf, url);
        FileSystem fs = PowerMockito.mock(FileSystem.class);
        PowerMockito.doThrow(new IOException("")).when(fs).delete(new Path("xx"), true);
        Field field = storage.getClass().asSubclass(HdfsStorage.class).getDeclaredField("fs");
        field.setAccessible(true);
        field.set(storage, fs);
        storage.delete("xx");
    }
}
