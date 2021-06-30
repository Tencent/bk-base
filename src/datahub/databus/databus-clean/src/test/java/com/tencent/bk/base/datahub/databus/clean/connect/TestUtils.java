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

package com.tencent.bk.base.datahub.databus.clean.connect;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtils {

  public static ETLResult parse(String configFile, String dataFile) throws Exception {
    String conf = getFileContent(configFile);
    String data = getFileContent(dataFile);

    ETL etl = new ETLImpl(conf);
    ETLResult result = etl.handle(data);

    return result;
  }

  public static String getFileContent(String resource) throws Exception {
    return getFileContent(resource, "UTF8");
  }

  public static String getFileContent(String resource, String encoding) throws Exception {
    URL url = TestUtils.class.getResource(resource);
    String path = url.getPath();
    // 兼容windows和linux
    path = path.replaceFirst("^/(.:/)", "$1");
    String result = new String(Files.readAllBytes(Paths.get(path)), encoding);
    return result;
  }

  public static String[] readlines(String resource, String encoding) throws Exception {
    String result = getFileContent(resource, encoding);
    return result.split("\n");
  }
}
