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

package com.tencent.bk.base.dataflow.jobnavi.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
                fs.close();
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
                fs.close();
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
                fs.close();
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
                fs.close();
            }
        }
    }

    /**
     * get file size
     *
     * @param path
     * @return file size
     * @throws IOException
     */
    public static Long getFileSize(String path) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                FileStatus fileStatus = fs.getFileStatus(destPath);
                return fileStatus.getLen();
            } else {
                LOGGER.error("file not found.");
                return null;
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }

    /**
     * read from file
     *
     * @param path
     * @return file content
     * @throws IOException
     */
    public static String readFile(String path) throws IOException {
        try (FileSystem fs = getFileSystem()) {
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                try (InputStream in = fs.open(destPath)) {
                    return org.apache.commons.io.IOUtils.toString(in, StandardCharsets.UTF_8);
                }
            } else {
                throw new IOException("file has not found.");
            }
        }
    }

    /**
     * read from file
     *
     * @param path file path
     * @param begin begin byte offset
     * @param end end byte offset
     * @return file content
     */
    public static String readFile(String path, long begin, long end) throws IOException {
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                FSDataInputStream in = null;
                try {
                    in = fs.open(destPath);
                    int byteCount = 0;
                    FileStatus fileStatus = fs.getFileStatus(destPath);
                    long fileLength = fileStatus.getLen();
                    in.seek(begin);
                    byteCount = (int) (Math.min(end, fileLength) - begin);
                    byte[] buffer = new byte[byteCount];
                    byteCount = in.read(buffer, 0, byteCount);
                    return new String(buffer, 0, byteCount, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    LOGGER.error("read file error.", e);
                    return null;
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            } else {
                LOGGER.error("file not found.");
                return null;
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }

    /**
     * extract content from file
     *
     * @param path file path
     * @param regex regular expression pattern
     * @return list of match content
     * @throws IOException
     */
    public static List<String> extractFromFile(String path, String regex) throws IOException {
        FileSystem fs = null;
        List<String> result = new LinkedList<>();
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                FSDataInputStream in = null;
                try {
                    in = fs.open(destPath);
                    String line;
                    InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
                    BufferedReader br = new BufferedReader(isr);
                    while ((line = br.readLine()) != null) {
                        Pattern pattern = Pattern.compile(regex);
                        Matcher matcher = pattern.matcher(line);
                        if (matcher.find()) {
                            result.add(matcher.group(0));
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("extract from file error.", e);
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            } else {
                LOGGER.error("file not found.");
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return result;
    }

    /**
     * extract last match content from file
     *
     * @param path file path
     * @param regex regular expression pattern
     * @return list of match content
     * @throws IOException
     */
    public static String extractLastFromFile(String path, String regex) throws IOException {
        FileSystem fs = null;
        FSDataInputStream in = null;
        Pattern pattern = Pattern.compile(regex);
        String result = "";
        try {
            fs = getFileSystem();
            Path destPath = new Path(path);
            if (fs.exists(destPath)) {
                in = fs.open(destPath);
                FileStatus fileStatus = fs.getFileStatus(destPath);
                long fileLength = fileStatus.getLen();
                int batch = (int) Math.min(fileLength, 512);
                long pos = fileLength - batch;
                //read from tail of file
                in.seek(pos);
                byte[] buffer = new byte[batch];
                StringBuilder line = new StringBuilder();
                int readBytes = in.read(buffer, 0, batch);
                while (readBytes > 0) {
                    String lines = new String(buffer, 0, readBytes, StandardCharsets.UTF_8);
                    //split into lines
                    String[] split = lines.split("\n");
                    for (int i = split.length - 1; i >= 0; --i) {
                        if (i < split.length - 1) {
                            //read a whole line
                            Matcher matcher = pattern.matcher(line);
                            if (matcher.find()) {
                                return matcher.group(0);
                            }
                            line.delete(0, line.length());
                        }
                        int bytes = split[i].length();
                        line.insert(0, split[i].substring(0, bytes));
                    }
                    batch = (int) Math.min(pos, batch);
                    pos -= batch;
                    in.seek(pos);
                    readBytes = in.read(buffer, 0, batch);
                }
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    return matcher.group(0);
                }
            } else {
                LOGGER.error("file:" + path + " not found.");
            }
        } catch (Exception e) {
            LOGGER.error("extract '" + regex + "' from log file:" + path + " failed", e);
        } finally {
            if (in != null) {
                in.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
        return result;
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
}
