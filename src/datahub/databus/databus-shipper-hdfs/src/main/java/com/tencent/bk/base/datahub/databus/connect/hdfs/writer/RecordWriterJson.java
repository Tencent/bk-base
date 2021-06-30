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


package com.tencent.bk.base.datahub.databus.connect.hdfs.writer;


import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class RecordWriterJson implements RecordWriter<String> {

    private static final Logger log = LoggerFactory.getLogger(RecordWriterJson.class);

    private String url;
    private Configuration conf;
    private String fileName;
    private FileSystem fileSystem;
    private FSDataOutputStream outputStream;


    public RecordWriterJson(String url, Configuration conf, String fileName) {
        this.url = url;
        this.conf = conf;
        this.fileName = fileName;
    }

    /**
     * 初始化hdfs相关资源用于写数据到文件中
     *
     * @throws IOException 异常
     */
    private void initHdfsResource() throws IOException {
        // 创建写hdfs文件所需的相关资源
        if (fileSystem == null) {
            fileSystem = FileSystem.get(URI.create(url), conf);
        }
        if (outputStream == null) {
            outputStream = fileSystem.create(new Path(fileName));
        }
    }

    /**
     * 当发生IOException是，将hdfs相关资源释放，以便在下一次重试时重新构建这些资源。
     */
    private void cleanHdfsResourceOnException() {
        fileSystem = null;
        outputStream = null;
    }

    /**
     * 将json格式字符串写入到hdfs文件中
     *
     * @param record json格式的记录，一行一条记录
     * @throws IOException 异常
     */
    @Override
    public void write(String record) throws IOException {
        try {
            long start = System.currentTimeMillis();
            initHdfsResource();
            outputStream.write(record.getBytes("utf8"));

            long duration = System.currentTimeMillis() - start;
            if (duration > 10000) {
                LogUtils.warn(log, "TIMEOUT: it takes {}(ms) to write just one record {} to {}", duration, record,
                        fileName);
            }
        } catch (IOException ioe) {
            LogUtils.warn(log,
                    String.format("going to close hdfs resource as failed to write json record %s to file %s", record,
                            fileName), ioe);
            // 将一些资源设置为null，便于在下一个周期内重新创建输出流
            cleanHdfsResourceOnException();
            throw ioe;
        }
    }

    /**
     * hdfs文件写入完毕后,关闭相关资源
     */
    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            LogUtils.info(log, "Closing file: {}", fileName);
            IOUtils.closeStream(outputStream);
            // 关闭文件后，将stream设置为null，避免多次调用close方法
            outputStream = null;
        }
    }

}
