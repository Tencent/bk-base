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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

public class JarFileMerge {

    private static final Logger LOGGER = Logger.getLogger(JarFileMerge.class);

    private static final byte[] BUFFER = new byte[4096 * 1024];


    /**
     * 合并 class 文件使用
     *
     * @param baseJar 基础 jar 文件（全路径）
     * @param classFiles 需要合并的子 class 文件列表（全路径）
     * @param outputJar 输出 jar（全路径）
     * @throws Exception
     */
    public static void mergeClassToJar(String baseJar, List<String> classFiles, String outputJar)
            throws IOException {
        JarFile jarFile = null;
        JarOutputStream append = null;

        try {
            LOGGER.info("base jar: " + baseJar);
            LOGGER.info("output jar: " + outputJar);
            jarFile = new JarFile(baseJar);
            append = new JarOutputStream(new FileOutputStream(outputJar));

            Enumeration<? extends JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry e = entries.nextElement();
                append.putNextEntry(e);
                if (!e.isDirectory()) {
                    try (InputStream inputStream = jarFile.getInputStream(e)) {
                        copy(inputStream, append);
                    }
                }
                append.closeEntry();
            }

            // now append jar
            for (String classFile : classFiles) {
                if (!classFile.endsWith(".class")) {
                    continue;
                }
                JarEntry jarEntry = new JarEntry(FilenameUtils.getName(classFile));
                LOGGER.info("append jar: " + jarEntry.getName());
                append.putNextEntry(jarEntry);
                InputStream jarIn = null;
                try {
                    jarIn = new FileInputStream(classFile);
                    copy(jarIn, append);
                    append.closeEntry();
                } finally {
                    if (null != jarIn) {
                        jarIn.close();
                    }
                }
            }
        } finally {
            if (null != jarFile) {
                jarFile.close();
            }
            if (null != append) {
                append.close();
            }
        }
    }

    private static void copy(InputStream input, OutputStream output) throws IOException {
        int bytesRead;
        while ((bytesRead = input.read(BUFFER)) != -1) {
            output.write(BUFFER, 0, bytesRead);
        }
    }
}
