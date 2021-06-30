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

package com.tencent.bk.base.datahub.databus.pipe.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zip压缩工具类
 */
public class ZipUtils {

    private static int BUFSIZE = 1024;
    private static Logger logger = LoggerFactory.getLogger(ZipUtils.class);

    /**
     * 解压.
     */
    public static byte[] unGzip(byte[] msgGzip) throws IOException {
        ByteArrayInputStream byteArrayInputStream = null;
        GZIPInputStream gzipInputStream = null;
        ByteArrayOutputStream byteArrayOutputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(msgGzip);
            gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] dataBuffer = new byte[BUFSIZE];
            int numCount;
            while ((numCount = gzipInputStream.read(dataBuffer, 0, dataBuffer.length)) != -1) {
                byteArrayOutputStream.write(dataBuffer, 0, numCount);
            }
            return byteArrayOutputStream.toByteArray();
        } finally {
            closeResources(byteArrayOutputStream, gzipInputStream, byteArrayInputStream);
        }
    }

    /**
     * 资源释放
     *
     * @param resources 资源列表
     */
    private static void closeResources(Closeable... resources) {
        if (resources != null) {
            for (Closeable resource : resources) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (IOException e) {
                        logger.error("close resource error!");
                    }
                }
            }
        } else {
            logger.warn("resources is null!");
        }
    }
}
