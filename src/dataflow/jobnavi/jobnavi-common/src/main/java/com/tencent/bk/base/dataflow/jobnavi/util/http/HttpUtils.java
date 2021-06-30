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

package com.tencent.bk.base.dataflow.jobnavi.util.http;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;

public class HttpUtils {

    private static final Logger LOGGER = Logger.getLogger(HttpUtils.class);

    private static final ThreadLocal<PoolingHttpClientConnectionManager> cm = new ThreadLocal<>();

    static {
        // set reserved word enable，like 'Host'
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    }

    /**
     * create HTTP client
     *
     * @return
     */
    public static CloseableHttpClient createHttpClient() {
        if (null == cm.get()) {
            cm.set(new PoolingHttpClientConnectionManager() {
                       {
                           setMaxTotal(16);
                           setDefaultMaxPerRoute(4);
                       }
                   }
            );
        }
        return HttpClients.custom().setConnectionManager(cm.get()).setConnectionManagerShared(true).build();
    }

    /**
     * send put request
     *
     * @param request request URL
     * @param param json param
     * @return response
     * @throws Exception
     */
    public static String put(String request, Map<String, Object> param, Map<String, String> header) throws Exception {
        if (header == null) {
            header = new HashMap<>();
        }
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        String body = JsonUtils.writeValueAsString(param);
        return put(request, body, header);
    }

    /**
     * send put request
     *
     * @param request
     * @param body
     * @param header
     * @return response data
     * @throws Exception
     */
    public static String put(String request, String body, Map<String, String> header) throws Exception {
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", "" + body.getBytes(StandardCharsets.UTF_8).length);
            if (header != null) {
                for (Map.Entry<String, String> properties : header.entrySet()) {
                    connection.setRequestProperty(properties.getKey(), properties.getValue());
                }
            }
            connection.setUseCaches(false);
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(600000);
            try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                wr.writeBytes(body);
                wr.flush();
            }
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * send put request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String put(String request, String body) throws Exception {
        return put(request, body, null);
    }

    /**
     * send put request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String put(String request, Map<String, Object> body) throws Exception {
        return put(request, body, null);
    }

    /**
     * send post request
     *
     * @param request
     * @param param
     * @return response data
     * @throws Exception
     */
    public static String post(String request, Map<String, Object> param) throws Exception {
        return post(request, param, null);
    }

    /**
     * send post request
     *
     * @param request
     * @param param
     * @param header
     * @return response data
     * @throws Exception
     */
    public static String post(String request, Map<String, Object> param, Map<String, String> header) throws Exception {
        if (header == null) {
            header = new HashMap<>();
        }
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        String body = JsonUtils.writeValueAsString(param);
        return post(request, body, header);
    }

    /**
     * send post request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String post(String request, String body) throws Exception {
        return post(request, body, null);
    }

    /**
     * send post request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String post(String request, String body, Map<String, String> header) throws Exception {
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", "" + body.getBytes(StandardCharsets.UTF_8).length);
            if (header != null) {
                for (Map.Entry<String, String> properties : header.entrySet()) {
                    connection.setRequestProperty(properties.getKey(), properties.getValue());
                }
            }
            //LOGGER.info(connection.getHeaderField("Host"));
            connection.setUseCaches(false);
            connection.setConnectTimeout(600000);
            connection.setReadTimeout(600000);
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            try {
                wr.write(body.getBytes(StandardCharsets.UTF_8));
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
     * send get request
     *
     * @param request
     * @return response data
     * @throws Exception
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
            connection.setConnectTimeout(600000);
            connection.setReadTimeout(600000);
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * send get request
     *
     * @param request
     * @return response data
     * @throws Exception
     */
    public static String get(String request, Map<String, String> header) throws Exception {
        URL url = new URL(request);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("charset", "utf-8");
            if (header != null) {
                for (Map.Entry<String, String> properties : header.entrySet()) {
                    connection.setRequestProperty(properties.getKey(), properties.getValue());
                }
            }
            connection.setUseCaches(false);
            connection.setConnectTimeout(600000);
            connection.setReadTimeout(600000);
            return handleResponse(connection);
        } finally {
            connection.disconnect();
        }
    }


    /**
     * send path request
     *
     * @param request request URL
     * @param param json param
     * @return response data
     * @throws Exception
     */
    public static String patch(String request, Map<String, Object> param, Map<String, String> header) throws Exception {
        if (header == null) {
            header = new HashMap<>();
        }
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        String body = JsonUtils.writeValueAsString(param);
        return patch(request, body, header);
    }

    /**
     * send path request
     *
     * @param request
     * @param body
     * @param header
     * @return response data
     * @throws Exception
     */
    public static String patch(String request, String body, Map<String, String> header) throws Exception {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPatch httpPatch = new HttpPatch(request);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(60000).build();
        httpPatch.setConfig(requestConfig);
        httpPatch.setEntity(new StringEntity(body));
        httpPatch.setHeader("charset", "utf-8");
        if (header != null) {
            for (Map.Entry<String, String> properties : header.entrySet()) {
                httpPatch.setHeader(properties.getKey(), properties.getValue());
            }
        }
        try (CloseableHttpResponse response = httpClient.execute(httpPatch)) {
            return handleResponse(response);
        } finally {
            httpClient.close();
        }
    }

    /**
     * send path request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String patch(String request, String body) throws Exception {
        return patch(request, body, null);
    }

    /**
     * send path request
     *
     * @param request
     * @param body
     * @return response data
     * @throws Exception
     */
    public static String patch(String request, Map<String, Object> body) throws Exception {
        return patch(request, body, null);
    }

    private static String handleResponse(HttpURLConnection connection) throws Exception {
        int responseCode = connection.getResponseCode();
        if (responseCode >= 200 && responseCode < 300) {
            return readAll(connection.getInputStream());
        } else {
            throw new HttpError(responseCode, readAll(connection.getErrorStream()));
        }
    }

    private static String handleResponse(HttpResponse httpResponse) throws Exception {
        int responseCode = httpResponse.getStatusLine().getStatusCode();
        if (responseCode >= 200 && responseCode < 300) {
            return readAll(httpResponse.getEntity().getContent());
        } else {
            throw new HttpError(responseCode, readAll(httpResponse.getEntity().getContent()));
        }
    }

    private static String readAll(InputStream inputStream) throws Exception {
        if (null == inputStream) {
            return null;
        }
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
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
