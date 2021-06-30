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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class HdfsUtil {

    private static final Logger LOGGER = Logger.getLogger(HdfsUtil.class);

    private static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
    }

    private static FileSystem getFileSystem(String hdfsPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", getHdfsHead(hdfsPath));
        return FileSystem.get(conf);
    }

    private static String getHdfsHead(String hdfsPath) {
        String[] pathPart = hdfsPath.split("//", 2);
        return pathPart[0] + "//" + pathPart[1].split("/", 2)[0];
    }

    /**
     * mkdir on given path
     *
     * @param path
     * @throws IOException
     */
    public static void mkdirs(String path) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (!fs.exists(destPath)) {
                fs.mkdirs(destPath);
            }
        } finally {
            if (fs != null) {
                closeAll(fs);
            }
        }
    }

    /**
     * delete path
     *
     * @param path
     * @throws IOException
     */
    public static void delete(String path) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                fs.delete(destPath, true);
            } else {
                LOGGER.warn("hdfs path " + path + " may not exist");
            }
        } finally {
            if (fs != null) {
                closeAll(fs);
            }
        }
    }

    /**
     * list file status under given path
     *
     * @param path
     * @throws IOException
     */
    public static FileStatus[] list(String path) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                return fs.listStatus(destPath);
            } else {
                LOGGER.warn("hdfs path " + path + " may not exist");
            }
        } finally {
            if (fs != null) {
                closeAll(fs);
            }
        }
        return null;
    }

    /**
     * check if path exist
     *
     * @param path
     * @return true if path exist
     * @throws IOException
     */
    public static boolean exist(String path) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            return fs.exists(destPath);
        } finally {
            if (fs != null) {
                closeAll(fs);
            }
        }
    }

    public static void closeAll(FileSystem fs) throws IOException {
        LOGGER.warn("close file system.");
        fs.close();
        FileSystem.clearStatistics();
        FileSystem.closeAll();
    }


    /**
     * update file to HDFS
     *
     * @param sourceFile
     * @param dest
     * @throws IOException
     */
    public static void uploadFileToHdfs(String sourceFile, String dest) throws IOException {
        FileSystem fs = null;
        FileInputStream fis = null;
        OutputStream os = null;
        try {
            mkdirs(dest);
            fs = getFileSystem();
            File source = new File(sourceFile);
            fis = new FileInputStream(source);
            Path destPath = new Path(dest + File.separator + source.getName());
            os = fs.create(destPath);
            //copy
            IOUtils.copyBytes(fis, os, 4096, true);
        } finally {
            if (fis != null) {
                fis.close();
            }
            if (os != null) {
                os.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
    }

    /**
     * download file from HDFS
     *
     * @param sourceFile
     * @param dest
     * @throws IOException
     */

    public static void downloadFromHdfs(String sourceFile, String dest) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem(sourceFile);
            fs.copyToLocalFile(new Path(sourceFile), new Path(dest));
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }
}
