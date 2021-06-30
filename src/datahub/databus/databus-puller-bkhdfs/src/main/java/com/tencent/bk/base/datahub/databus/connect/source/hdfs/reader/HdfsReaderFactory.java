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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsReaderFactory {

    public static final String JSON_TYPE = "json";
    public static final String PARQUET_TYPE = "parquet";

    /**
     * @param fs hdfs的file system对象
     * @param conf hdfs文件系统参数
     * @param path 文件的path对象
     * @param columns 需要读取的字段集合，顺序排列的
     * @throws IOException 异常
     */
    public static RecordsReader createRecordsReader(FileSystem fs, Configuration conf, Path path, String[] columns)
            throws IOException {
        String fileName = path.getName();
        int slash = fileName.lastIndexOf(".");
        String dataType = fileName.substring(slash + 1);
        if (PARQUET_TYPE.equals(dataType)) {
            return new HdfsParqueReader(conf, path, columns);
        } else if ("".equals(dataType) || JSON_TYPE.equals(dataType)) {
            return new HdfsJsonReader(fs, path, columns);
        }

        return null;
    }
}
