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

package com.tencent.bk.base.dataflow.core.common;

import com.tencent.bk.base.dataflow.core.conf.ConfigConstants;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpUtils {

    /**
     * api post请求
     *
     * @param request post url
     * @param param post 请求参数
     * @return post result
     * @throws Exception post 请求异常
     */
    public static String post(String request, Map param) throws Exception {
        String body = Tools.writeValueAsString(param);
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("maxPostSize", "0");
            connection.setRequestProperty("Content-Length", String.valueOf(body.getBytes("utf-8").length));
            connection.setUseCaches(false);
            connection.setConnectTimeout(ConfigConstants.HTTP_CONNECT_TIMEOUT);
            connection.setReadTimeout(ConfigConstants.HTTP_READ_TIMEOUT);
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            try {
                wr.write(body.getBytes("utf-8"));
                wr.flush();
            } finally {
                wr.close();
            }
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }


    /**
     * post 请求
     *
     * @param request 请求url
     * @param body 参数为&拼接起来的
     * @return 结果
     * @throws Exception ex
     */
    public static String post(String request, String body) throws Exception {
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", "" + Integer.toString(body.getBytes("utf-8").length));
            connection.setUseCaches(false);
            connection.setConnectTimeout(ConfigConstants.HTTP_CONNECT_TIMEOUT);
            connection.setReadTimeout(ConfigConstants.HTTP_READ_TIMEOUT);
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            try {
                wr.write(body.getBytes("utf-8"));
                wr.flush();
            } finally {
                wr.close();
            }
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * http get请求
     *
     * @param request 请求url
     * @return 返回值
     * @throws Exception ex
     */
    public static String get(String request) throws Exception {
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("charset", "utf-8");
            connection.setUseCaches(false);
            connection.setConnectTimeout(ConfigConstants.HTTP_CONNECT_TIMEOUT);
            connection.setReadTimeout(ConfigConstants.HTTP_READ_TIMEOUT);
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }

    private static String handleResponse(HttpURLConnection connection) throws Exception {
        int responseCode = connection.getResponseCode();
        if (responseCode >= 200 && responseCode < 300) {
            return readAll(connection.getInputStream());
        } else {
            throw new HttpError(responseCode, readAll(connection.getErrorStream()));
        }
    }

    private static String readAll(InputStream inputStream) throws Exception {
        if (null == inputStream) {
            return null;
        }
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF8"));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while (null != (line = reader.readLine())) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } finally {
            inputStream.close();
        }
    }

    @SuppressWarnings("serial")
    public static class HttpError extends RuntimeException {

        public final int responseCode;
        public final String output;

        public HttpError(int responseCode, String output) {
            super("[" + responseCode + "]: " + output);
            this.responseCode = responseCode;
            this.output = output;
        }

        @Override
        public String toString() {
            return "Error{" + "responseCode=" + responseCode + ", output='" + output + '\'' + '}';
        }
    }
}
