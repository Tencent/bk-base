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

package com.tencent.bk.base.datahub.databus.connect.common.tools;


import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConvertAvroToJson {

    /**
     * 从avro格式的文件中读取数据,转换为json格式,打印在屏幕上。
     *
     * @param args 参数列表(文件)
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println(
                    "Usage : java -classpath \"*\" com.tencent.bk.base.datahub.databus.connect.common.tools.ConvertAvroToJson file1 file2 ...");
            System.exit(1);
        }

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        GenericRecord avroRecord = null;

        for (String filename : args) {
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(filename));
                ByteArrayInputStream input = new ByteArrayInputStream(bytes);
                DataFileStream<GenericRecord> dataReader = new DataFileStream<>(input, reader);

                while (dataReader.hasNext()) {
                    // 一条kafka msg中只含有一个avro记录，每个avro记录包含多条实际的数据记录
                    System.out.println(dataReader.next(avroRecord).toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
