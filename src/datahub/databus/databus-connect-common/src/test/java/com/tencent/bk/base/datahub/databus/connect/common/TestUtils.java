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

package com.tencent.bk.base.datahub.databus.connect.common;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtils {

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


    /**
     * 查询tsdb的shuju
     *
     * @param db 数据库名称
     * @param data 数据字符串
     * @param url tsdb的连接地址
     * @param userPass tsdb的账户信息
     * @return 查询结果字符串
     */
    public static String queryData(String db, String data, String url, String userPass) {

        try {
            // 使用ms作为时间的精度
            //URLConnection connection = new URL(url + "?db=" + db + "&precision=ms").openConnection();
            //URLConnection connection = new URL(url + "?db=" + db + "&q=" + data).openConnection();
            URLConnection connection = new URL(url + "?db=" + db + "&q=" + data).openConnection();
            //connection.setDoOutput(true); // Triggers POST.
            connection.setRequestProperty("Accept-Charset", "utf-8");
            connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userPass.getBytes());
            connection.setRequestProperty("Authorization", basicAuth);
//      try (OutputStream output = connection.getOutputStream()) {
//        output.write(data.getBytes("utf-8"));
//      }

            int statusCode = ((HttpURLConnection) connection).getResponseCode();
            if (statusCode != 204) {
                BufferedReader br;
                if (200 <= statusCode && statusCode <= 299) {
                    br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                } else {
                    br = new BufferedReader(new InputStreamReader(((HttpURLConnection) connection).getErrorStream()));
                }
                StringBuilder response = new StringBuilder();
                String line = null;
                while ((line = br.readLine()) != null) {
                    response.append(line).append("\n");
                }

                return response.toString();
            }
        } catch (FileNotFoundException e) {
            //TODO 这里不会抛出异常，如果db不存在应该是statuscode=404
            return null; // db不存在，跳过这个数据
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

    /**
     * 提交数据到tsdb中
     *
     * @param db 数据库名称
     * @param data 数据字符串
     * @param url tsdb的连接地址
     * @param userPass tsdb的账户信息
     * @return true/false
     */
    public static Boolean delData(String db, String data, String url, String userPass) {
        if (data == null) {
            return true;
        }

        try {
            // 使用ms作为时间的精度
            //URLConnection connection = new URL(url + "?db=" + db + "&precision=ms").openConnection();
            URLConnection connection = new URL(url + "?db=" + db + "&q=" + data).openConnection();
            connection.setDoOutput(true); // Triggers POST.
            connection.setRequestProperty("Accept-Charset", "utf-8");
            connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userPass.getBytes());
            connection.setRequestProperty("Authorization", basicAuth);

            try (OutputStream output = connection.getOutputStream()) {
                output.write(data.getBytes("utf-8"));
            }

            int statusCode = ((HttpURLConnection) connection).getResponseCode();
            if (statusCode != 204) {
                BufferedReader br;
                if (200 <= statusCode && statusCode <= 299) {
                    br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                } else {
                    br = new BufferedReader(new InputStreamReader(((HttpURLConnection) connection).getErrorStream()));
                }
                StringBuilder response = new StringBuilder();
                String line = null;
                while ((line = br.readLine()) != null) {
                    response.append(line).append("\n");
                }

                System.out.println("response:" + response);

                return false;
            }
        } catch (FileNotFoundException e) {
            //TODO 这里不会抛出异常，如果db不存在应该是statuscode=404
            e.printStackTrace();
            return true; // db不存在，跳过这个数据
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
