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

package com.tencent.bk.base.datahub.databus.commons.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class InfluxdbUtils {

    private static final Logger log = LoggerFactory.getLogger(InfluxdbUtils.class);

    private static final int[] INTERNAL = {1000, 2000, 3000, 5000, 10000, 20000, 40000, 55000, 70000, 90000};

    /**
     * 提交数据到tsdb中
     *
     * @param db 数据库名称
     * @param data 数据字符串
     * @param url tsdb的地址
     * @param userPass tsdb的账号信息
     */
    public static void submitData(String db, String data, String url, String userPass) {
        submitData(db, data, url, userPass, null);
    }

    /**
     * 提交数据到tsdb中
     *
     * @param db 数据库名称
     * @param data 数据字符串
     * @param url tsdb的地址
     * @param userPass tsdb的账号信息
     * @param optArgs 可选参数
     */
    public static void submitData(String db, String data, String url, String userPass, Map<String, String> optArgs) {
        int retryNum = 3;
        for (int i = 0; i < retryNum; i++) {
            if (handleSubmitData(db, data, url, userPass, optArgs)) {
                return;
            } else {
                try {
                    LogUtils.info(log, "submit data failed, remaining retry num " + (4 - i - 1));
                    Thread.sleep(INTERNAL[i]);
                } catch (InterruptedException e) {
                    LogUtils.warn(log, "get interrupted exception!", e);
                }
            }
        }
    }

    /**
     * 提交数据到tsdb中
     *
     * @param db 数据库名称
     * @param data 数据字符串
     * @param url tsdb的连接地址
     * @param userPass tsdb的账户信息
     * @param optArgs 可选参数，可以为null
     * @return true/false
     */
    private static Boolean handleSubmitData(String db, String data, String url, String userPass,
            Map<String, String> optArgs) {
        if (data == null) {
            return true;
        }

        BufferedReader br = null;
        try {
            StringBuilder urlSb = new StringBuilder().append(url).append("?db=").append(db);
            if (optArgs != null) {
                for (Map.Entry<String, String> entry : optArgs.entrySet()) {
                    urlSb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
                }
            }

            URLConnection connection = new URL(urlSb.toString()).openConnection();
            connection.setDoOutput(true); // Triggers POST.
            connection.setRequestProperty("Accept-Charset", "utf-8");
            connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter
                    .printBase64Binary(userPass.getBytes(StandardCharsets.ISO_8859_1));
            connection.setRequestProperty("Authorization", basicAuth);

            try (OutputStream output = connection.getOutputStream()) {
                output.write(data.getBytes(StandardCharsets.UTF_8));
            }

            int statusCode = ((HttpURLConnection) connection).getResponseCode();
            if (statusCode != 204) {
                if (200 <= statusCode && statusCode <= 299) {
                    br = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
                } else {
                    br = new BufferedReader(new InputStreamReader(((HttpURLConnection) connection).getErrorStream(),
                            StandardCharsets.UTF_8));
                }
                StringBuilder response = new StringBuilder();
                String line = null;
                while ((line = br.readLine()) != null) {
                    response.append(line).append("\n");
                }

                LogUtils.debug(log, "{}, {}, {}, {}, {}", urlSb, statusCode,
                        ((HttpURLConnection) connection).getResponseMessage(), response.toString(), data);
                return false;
            }
        } catch (FileNotFoundException e) {
            LogUtils.info(log, db + " db not exists, discard data");
            return true; // db不存在，跳过这个数据
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.TSDB_ERR,
                    String.format("db/url/data: %s %s %s", db, url, data), e);
            return false;
        } finally {
            // 关闭资源
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) {
                    LogUtils.debug(log, "failed to close buffered reader!", ignore);
                }
            }
        }

        return true;
    }

}

