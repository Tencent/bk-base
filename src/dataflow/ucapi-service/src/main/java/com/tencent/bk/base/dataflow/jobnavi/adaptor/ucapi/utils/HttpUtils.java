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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Map;
import org.apache.log4j.Logger;

public class HttpUtils {

    private static final Logger logger = Logger.getLogger(HttpUtils.class);
    private static final int HTTP_CONNECT_TIMEOUT = 10000;
    private static final int HTTP_READ_TIMEOUT = 10000;

    /**
     * api post请求
     *
     * @param request request url
     * @param param request body
     * @return  request result
     * @throws Exception http error
     */
    public static String post(String request, Map param) throws Exception {
        logger.info(MessageFormat.format("The post request is {0}, and param is {1}", request, param.toString()));
        String body = writeValueAsString(param);
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
            connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
            connection.setReadTimeout(HTTP_READ_TIMEOUT);
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
        logger.info("The get request is " + request);
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("charset", "utf-8");
            connection.setUseCaches(false);
            connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
            connection.setReadTimeout(HTTP_READ_TIMEOUT);
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

    /**
     * 将object转换为string
     * @param obj 需要转换的object
     * @return 返回转换后的string
     */
    public static String writeValueAsString(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException();
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
