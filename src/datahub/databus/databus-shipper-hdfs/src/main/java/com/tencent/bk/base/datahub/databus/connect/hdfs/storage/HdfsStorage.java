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

import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.FsWal;
import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.Wal;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class HdfsStorage implements Storage {

    private static final Logger log = LoggerFactory.getLogger(HdfsStorage.class);

    private final FileSystem fs;
    private final Configuration conf;
    private final String url;

    public HdfsStorage(Configuration conf, String url) throws IOException {
        fs = FileSystem.newInstance(URI.create(url), conf);
        this.conf = conf;
        this.url = url;
    }

    @Override
    public FileStatus[] listStatus(String path, PathFilter filter) throws IOException {
        return fs.listStatus(new Path(path), filter);
    }

    @Override
    public FileStatus[] listStatus(String path) throws IOException {
        return fs.listStatus(new Path(path));
    }

    @Override
    public void append(String filename, Object object) throws IOException {

    }

    @Override
    public boolean mkdirs(String filename) throws IOException {
        return fs.mkdirs(new Path(filename));
    }

    @Override
    public boolean exists(String filename) throws IOException {
        return fs.exists(new Path(filename));
    }

    @Override
    public void commit(String tempFile, String committedFile) {
        LogUtils.info(log, "commit file {} to {}", tempFile, committedFile);
        renameFile(tempFile, committedFile);
    }


    @Override
    public void delete(String filename) {
        try {
            LogUtils.info(log, "delete file {}", filename);
            fs.delete(new Path(filename), true);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    @Override
    public Wal wal(String topicsDir, TopicPartition topicPart) {
        return new FsWal(topicsDir, topicPart, this);
    }

    @Override
    public Configuration conf() {
        return conf;
    }

    @Override
    public String url() {
        return url;
    }

    private void renameFile(String sourcePath, String targetPath) {
        if (sourcePath.equals(targetPath)) {
            return;
        }
        try {
            final Path srcPath = new Path(sourcePath);
            final Path dstPath = new Path(targetPath);
            if (fs.exists(srcPath)) {
                fs.rename(srcPath, dstPath);
            }
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                    "failed to rename " + sourcePath + " to " + targetPath, e);
            throw new ConnectException(e);
        }
    }
}
