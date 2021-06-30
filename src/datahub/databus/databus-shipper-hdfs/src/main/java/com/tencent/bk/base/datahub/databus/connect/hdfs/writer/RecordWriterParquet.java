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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RecordWriterParquet implements RecordWriter<GenericArray<GenericRecord>> {

    private static final Logger log = LoggerFactory.getLogger(RecordWriterParquet.class);

    private static final int BLOCK_SIZE = 128 * 1024 * 1024; // 128MB
    private static final int PAGE_SIZE = 1024 * 1024;  // 1MB

    private String url;
    private Configuration conf;
    private String fileName;
    private AvroParquetWriter<GenericRecord> writer;


    public RecordWriterParquet(String url, Configuration conf, String fileName) {
        this.url = url;
        this.conf = conf;
        this.fileName = fileName;
    }

    /**
     * 初始化hdfs相关资源用于写数据到文件中
     *
     * @throws IOException 异常
     */
    private void initHdfsResource(Schema schema) throws IOException {
        // 创建写hdfs文件所需的相关资源
        if (writer == null) {
            writer = new AvroParquetWriter<>(new Path(fileName), schema, CompressionCodecName.SNAPPY, BLOCK_SIZE,
                    PAGE_SIZE, false, conf);
        }
    }

    /**
     * 当发生IOException是，将hdfs相关资源释放，以便在下一次重试时重新构建这些资源。
     */
    private void cleanHdfsResourceOnException() {
        writer = null;
    }

    /**
     * 将avro array中的多条记录写入到对应的parquet文件中
     *
     * @param avroArray 待写入的avro记录数组
     * @throws IOException 异常
     */
    @Override
    public void write(GenericArray<GenericRecord> avroArray) throws IOException {
        try {
            long start = System.currentTimeMillis();
            for (GenericRecord record : avroArray) {
                initHdfsResource(record.getSchema());
                writer.write(record);
            }

            long duration = System.currentTimeMillis() - start;
            if (duration > 10000) {
                LogUtils.warn(log, "TIMEOUT: it takes {}(ms) to flush avro records to file {}", duration, fileName);
            }
        } catch (IOException ioe) {
            LogUtils.warn(log, String.format("failed to write avro records to parquet file %s", fileName), ioe);
            cleanHdfsResourceOnException(); // IOException时清理相关资源
            throw ioe;
        }
    }

    /**
     * 关闭parquet文件
     */
    @Override
    public void close() throws IOException {
        try {
            if (writer != null) {
                writer.close();
                // 关闭文件后，将writer设置为null，避免多次调用close方法
                writer = null;
            }
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_CLOSE_FILE_ERR,
                    "failed to close outputStream " + fileName, e);
            throw e;
        }
    }

}
