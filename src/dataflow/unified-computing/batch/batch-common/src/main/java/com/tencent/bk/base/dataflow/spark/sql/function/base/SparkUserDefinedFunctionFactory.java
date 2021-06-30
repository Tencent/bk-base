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

package com.tencent.bk.base.dataflow.spark.sql.function.base;

import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctionConfig;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions;
import com.tencent.bk.base.dataflow.udf.UdfRegister;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.BatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUserDefinedFunctionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SparkUserDefinedFunctionFactory.class);

    public static void registerAll(final SparkSession sparkSession, UserDefinedFunctions functions) {
        List<String> jars = new ArrayList<>();
        for (UserDefinedFunctionConfig config : functions.getFunctions()) {
            jars.add(config.getHdfsPath());
        }
        addHdfsUdfJar(jars, sparkSession);

        for (UserDefinedFunctionConfig config : functions.getFunctions()) {
            //add jar
            LOG.info("The hdfs path is " + config.getHdfsPath());

            sparkSession.sparkContext().addJar(config.getHdfsPath());
            String className = UdfRegister.getUdfClassName("batch", config.getName(), config.getLanguage());
            sparkSession.sql("CREATE TEMPORARY FUNCTION " + config.getName() + " AS '" + className + "'");
        }
    }

    private static void addHdfsUdfJar(List<String> jars, SparkSession sparkSession) {
        URLClassLoader currentClassLoader = (URLClassLoader) SparkUserDefinedFunctionFactory.class.getClassLoader();
        Class sysClass = URLClassLoader.class;
        String udfTempDir = BatchUtils.createTempDir();
        LOG.info(String.format("Create temporary directory for user jar file: %s", udfTempDir));

        try {
            Method method = sysClass.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            for (String jar : jars) {
                Path path = Paths.get(jar);
                String fileName = path.getFileName().toString();
                LOG.info(String.format("Start to download user package from %s to %s", jar, udfTempDir));
                BatchUtils.downloadFiles(jar, udfTempDir, fileName, sparkSession.sparkContext().conf(),
                        sparkSession.sparkContext().hadoopConfiguration());
                LOG.info("Download completed");
                String tempJarPathStr = String.format("%s/%s", udfTempDir, fileName);
                Path tempJarPath = Paths.get(tempJarPathStr);
                LOG.info(String.format("Added path %s to class loader", tempJarPath.toString()));
                method.invoke(currentClassLoader, tempJarPath.toUri().toURL());
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException("Error, could not add URL to current classloader", t.getCause());
        }
    }
}
