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

package com.tencent.bk.base.dataflow.jobnavi.util.classloader;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class ClassLoaderBuilder {

    private static final Logger logger = Logger.getLogger(ClassLoaderBuilder.class);

    /**
     * build classloader from lib and conf
     *
     * @param libPaths
     * @param confPaths
     * @return
     *
     * @throws  Exception
     */
    public static URLClassLoader build(List<String> libPaths, List<String> confPaths) throws Exception {
        List<URL> urlList = new ArrayList<>();
        for (String libPath : libPaths) {
            loadJar(libPath, urlList);
        }
        for (String confPath : confPaths) {
            loadConf(confPath, urlList);
        }
        URLClassLoader taskClassLoader = new URLClassLoader(urlList.toArray(new URL[urlList.size()]));
        for (URL url : taskClassLoader.getURLs()) {
            logger.info("load resource: " + url.toURI().getPath());
        }
        return taskClassLoader;
    }


    private static void loadJar(String path, List<URL> jarUrlList) throws Exception {
        logger.info("load path: " + path);
        File file = new File(path);
        if (file.exists() && file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    URL url = f.toURI().toURL();
                    jarUrlList.add(url);
                }
            }
        } else {
            logger.error("directory " + path + " may not exist.");
        }
    }

    private static void loadConf(String confPath, List<URL> jarUrlList) throws Exception {
        File f = new File(confPath);
        if (f.exists()) {
            URL url = f.toURI().toURL();
            jarUrlList.add(url);
        } else {
            logger.error("directory " + confPath + " may not exist.");
        }
    }
}
