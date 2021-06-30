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

package com.tencent.bk.base.dataflow.udf.util;

import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractUdaf;
import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractUdf;
import com.tencent.bk.base.dataflow.core.function.base.udtf.AbstractUdtf;
import com.tencent.bk.base.dataflow.udf.fs.IOUtils;
import com.tencent.bk.base.dataflow.udf.fs.LocalFileSystem;
import com.tencent.bk.base.dataflow.udf.fs.Path;
import com.tencent.bk.base.dataflow.udf.fs.AbstractFSDataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfUtils {

    public static final String FUNCTION_PREFIX = "com.tencent.bk.base.dataflow.udf.functions";
    public static final String FLINK_UDF_DIRECTORY = "com.tencent.bk.base.dataflow.udf.codegen.flink";
    public static final String HIVE_UDF_DIRECTORY = "com.tencent.bk.base.dataflow.udf.codegen.hive";
    private static final Logger LOGGER = LoggerFactory.getLogger(UdfUtils.class);

    /**
     * Get the abstract udf class by func file name.
     *
     * @param functionFile func file name
     * @return the instance of abstract udf
     */
    public static AbstractUdf getUdfInstance(String functionFile) {
        Object object = getInstance(functionFile);
        if (object instanceof AbstractUdf) {
            return (AbstractUdf) object;
        }
        throw new RuntimeException("When the udf is obtained, the udf implementation cannot be found.");
    }

    /**
     * Get the abstract udtf instance by func file name.
     *
     * @param functionFile func file name
     * @return the instance of abstract udf
     */
    public static AbstractUdtf getUdtfInstance(String functionFile) {
        Object object = getInstance(functionFile);
        if (object instanceof AbstractUdtf) {
            return (AbstractUdtf) object;
        }
        throw new RuntimeException("When the udtf is obtained, the udtf implementation cannot be found.");
    }

    /**
     * Get the abstract udaf instance by func file name.
     *
     * @param functionFile func file name
     * @return the instance of abstract udaf
     */
    public static AbstractUdaf getUdafInstance(String functionFile) {
        Object object = getInstance(functionFile);
        if (object instanceof AbstractUdaf) {
            return (AbstractUdaf) object;
        }
        throw new RuntimeException("When the udtf is obtained, the udtf implementation cannot be found.");
    }

    private static Object getInstance(String functionFile) {
        String classFullName = MessageFormat.format("{0}.{1}", FUNCTION_PREFIX, functionFile.replace(".java", ""));
        try {
            Class<?> clazz = Class.forName(classFullName);
            return clazz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Refection class failed.", e);
        }
    }

    /**
     * create user-defined function file.
     *
     * @param javaFile the file which to create.
     * @param code the code which is auto generate.
     */
    public static void createUdfFile(String javaFile, String code) {
        try {
            FileUtils.deleteQuietly(new File(javaFile));
            FileUtils.writeStringToFile(new File(javaFile), code, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解压 python 脚本的 zip 包
     *
     * @param functionName 自定义函数名称
     * @param targetDir 解压目录
     */
    public static void unzipPythonLibrary(String functionName, Path targetDir) {
        try {
            LocalFileSystem targetFs = targetDir.getFileSystem();
            ClassLoader classLoader = UdfUtils.class.getClassLoader();
            try (ZipInputStream zis = new ZipInputStream(
                    classLoader.getResourceAsStream(MessageFormat.format("python-{0}.zip", functionName)))) {
                ZipEntry entry = zis.getNextEntry();
                while (entry != null) {
                    String fileName = entry.getName();
                    Path newFile = new Path(targetDir, fileName);
                    if (entry.isDirectory()) {
                        targetFs.mkdirs(newFile);
                    } else {
                        AbstractFSDataOutputStream fsDataOutputStream = targetFs
                                .create(newFile, LocalFileSystem.WriteMode.OVERWRITE);
                        IOUtils.copyBytes(zis, fsDataOutputStream, false);
                    }
                    zis.closeEntry();
                    entry = zis.getNextEntry();
                }
                zis.closeEntry();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to unzip flink python library.", e);
        }
    }

    /**
     * 删除udf相关目录和文件 当UDF目录中包含多个脚本时，不允许删除__init__.py，以防jep引用不到用户的UDF脚本 删除父目录时忽略结果
     *
     * @param dir udf的父目录
     * @param udfName udf名称
     */
    public static void deleteQuietlyUdfFiles(String dir, String udfName) {
        LOGGER.info("To delete udf " + udfName + " related directories and files. " + dir);

        File udfPy = new File(String.format("%s/udf/%s.py", dir, udfName));
        FileUtils.deleteQuietly(udfPy);

        File udfPyc = new File(String.format("%s/udf/%s.pyc", dir, udfName));
        FileUtils.deleteQuietly(udfPyc);

        File pyCache = new File(String.format("%s/udf/__pycache__", dir));
        FileUtils.deleteQuietly(pyCache);
        File udfCommon = new File(String.format("%s/udf/%s_util", dir, udfName));
        FileUtils.deleteQuietly(udfCommon);

        File[] udfFiles = new File(dir, "udf").listFiles();
        if (null != udfFiles && udfFiles.length <= 1) {
            FileUtils.deleteQuietly(new File(String.format("%s/udf/__init__.py", dir)));
        }

        boolean udfDeleteResult = new File(dir, "udf").delete();
        LOGGER.info("The dir " + dir + " udf delete result is " + udfDeleteResult);
        boolean deleteResult = new File(dir).delete();
        LOGGER.info("The dir " + dir + " delete result is " + deleteResult);
    }

    /**
     * 获取任务运行pid-[thread id]
     *
     * @return pid-[thread id]
     */
    public static String getPidAndThreadId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return String.format("%s-%s", name.split("@")[0], Thread.currentThread().getId());
    }
}
