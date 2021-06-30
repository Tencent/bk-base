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

import static java.util.stream.Collectors.joining;

import com.tencent.bk.base.dataflow.jobnavi.adaptor.flink.compiler.FileCompiler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class CustomJarsUtils {

    private static final Logger logger = Logger.getLogger(CustomJarsUtils.class);


    public static boolean needDownloadCustomJars(String customJarsValue) {
        if (StringUtils.isNotBlank(customJarsValue)) {
            List<?> jars = JsonUtils.readList(customJarsValue);
            for (Object jarInfo : jars) {
                if (jarInfo instanceof Map<?, ?>) {
                    String customJarFile = (String) ((Map<?, ?>) jarInfo).get("path");
                    if (null != customJarFile
                            && customJarFile.startsWith("hdfs://")
                            && customJarFile.endsWith(".jar")) {
                        // 存在设置的hdfs custom jar file 则需要下载自定义包
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /***
     * 动态编译代码并合并字节文件和 jar 包
     * @param options
     * @param jobName
     * @param env
     * @throws Exception
     */
    public static void autoGenJarsAndMerge(FlinkOptions options, String jobName, String env) throws Exception {
        if (!(options instanceof FlinkOptionsV2)) {
            return;
        }
        String userMainClass = (String) ((FlinkOptionsV2) options).getUserMainClass().getValue();
        String codeValue = (String) ((FlinkOptionsV2) options).getCode().getValue();
        String typeName = options.getTypeName();
        // 校验：判断是否值为空
        if (StringUtils.isBlank(userMainClass)
                || StringUtils.isBlank(codeValue)
                || StringUtils.isBlank(jobName)
                || StringUtils.isBlank(typeName)) {
            logger.info("The task does not have user code.");
            return;
        }
        // 指定 job 打包后存放的路径
        String jobJarRootPath = System.getProperty("JOBNAVI_HOME") + "/adaptor/" + typeName + "/opt/" + jobName;
        // 自动生成类的路径
        String generateClassPath = jobJarRootPath + "/classes";
        // 编译依赖包路径
        String libPath = System.getProperty("JOBNAVI_HOME") + "/env/" + env + "/lib";
        logger.info("The dependent libPath: " + libPath);
        Collection<File> classPathFiles = FileUtils.listFiles(
                new File(libPath), null, false);
        // 构造完整 classpath
        String compilerClassPath = classPathFiles.stream().map(file -> file.getPath()).collect(joining(":"));
        String baseJar = System.getProperty("JOBNAVI_HOME") + "/adaptor/" + typeName + "/job/"
                + options.getJarFileName().getValue().toString();
        // 将当前待提交包加入 classpath
        compilerClassPath = baseJar + ":" + compilerClassPath;
        // 若存在，删除旧编译文件目录
        logger.info("Removing class directory: " + generateClassPath);
        FileUtils.deleteDirectory(new File(generateClassPath));
        // 编译 userMainClass
        new FileCompiler(generateClassPath, compilerClassPath).compile(userMainClass, codeValue);
        // 最终 JAR 路径
        String outJar = jobJarRootPath + "/" + jobName + ".jar";
        // 合并
        JarFileMerge.mergeClassToJar(
                baseJar,
                Files.list(
                        new File(generateClassPath).toPath()).map(
                        file -> file.toAbsolutePath().toString()).collect(Collectors.toList()),
                outJar);
    }
}
