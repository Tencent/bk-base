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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsParqueReader implements RecordsReader {

    private static final Logger log = LoggerFactory.getLogger(HdfsParqueReader.class);

    private Path path;
    private String[] columns;
    private int processedCnt = 0;
    private ParquetReader<GenericRecord> reader;
    private boolean reachEnd = false;

    /**
     * 构造函数
     *
     * @param conf hdfs的file system对象
     * @param path 文件的path对象
     * @param columns 需要读取的字段集合，顺序排列的
     * @throws IOException 异常
     */
    public HdfsParqueReader(Configuration conf, Path path, String[] columns) throws IOException {
        this.path = path;
        this.columns = columns;
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
        builder.withConf(conf);
        reader = builder.build();
    }

    @Override
    public void seekTo(int skipCount) throws IOException {
        if (skipCount > 0) {
            while (reader.read() != null) {
                processedCnt++;
                if (processedCnt >= skipCount) {
                    break;
                }
            }
        }
        LogUtils.debug(log, "seeking to line {} of file {}, skipCount is set to {}", processedCnt, path.getName(),
                skipCount);

    }

    @Override
    public List<List<Object>> readRecords(int maxCount) throws IOException {
        List<List<Object>> result = new ArrayList<>(maxCount);

        GenericRecord line = reader.read();
        while (line != null) {
            List<Object> item = new ArrayList<>(columns.length);
            for (String column : columns) {
                item.add(line.get(column));
            }
            result.add(item);
            processedCnt++;
            if (--maxCount == 0) {
                break;
            }
            line = reader.read();
        }

        if (line == null) {
            reachEnd = true;
        }

        LogUtils.debug(log, "reading {} records in file {}", result.size(), path.getName());

        return result;
    }

    @Override
    public String getFilename() {
        return path.getName();
    }

    @Override
    public boolean reachEnd() {
        return reachEnd;
    }

    @Override
    public int processedCnt() {
        return processedCnt;
    }

    @Override
    public void closeResource() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                LogUtils.warn(log, "{} failed to close parquet reader. {}", path.getName(), e.getMessage());
            }
        }
    }
}
