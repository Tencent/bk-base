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

package com.tencent.bk.base.dataflow.core.http.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.common.HttpUtils;
import com.tencent.bk.base.dataflow.core.common.Tools;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAPI.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String method;
    private String url;
    private String module;
    private final int maxRetryTimes = 3;

    public DataAPI(String method, String url, String module) {
        this.method = method;
        this.url = url;
        this.module = module;
    }

    public DataResponse call(HashMap<String, Object> args) {
        return this.sendRequest(args);
    }

    private DataResponse sendRequest(Map<String, Object> args) {
        Date startTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        String requestDateTime = formatter.format(startTime);
        DataResponse response = this.send(args, requestDateTime, 1);
        Date endTime = new Date();
        String log = this.buildStandardLog(
                args, response, requestDateTime, String.valueOf((endTime.getTime() - startTime.getTime()) / 1000.0));
        LOGGER.info(log);
        return response;
    }

    private DataResponse send(Map<String, Object> args, String requestDateTime, int retryTimes) {
        this.buildActualUrl(args);
        try {
            if ("get".equalsIgnoreCase(this.method)) {
                String response = DataAPI.get(this.buildGetUrl(args));
                Map<String, Object> resp = Tools.readMap(response);
                return new DataResponse(resp);
            } else if ("post".equalsIgnoreCase(this.method)) {
                String response = HttpUtils.post(this.url, args);
                Map<String, Object> resp = Tools.readMap(response);
                return new DataResponse(resp);
            } else {
                throw new RuntimeException(String.format("异常请求方式，%s", this.method));
            }
        } catch (SocketTimeoutException e) {
            // 超时
            Map errorMessage = new HashMap<String, Object>();
            errorMessage.put("message", String.format("Request Timeout, retry %s.", retryTimes));
            LOGGER.error(this.buildStandardLog(args, new DataResponse(errorMessage), requestDateTime, "-1"));
            if (retryTimes >= maxRetryTimes) {
                throw new RuntimeException(e);
            }
            retryTimes++;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            return this.send(args, requestDateTime, retryTimes);
        } catch (Exception e) {
            // 其它错误
            Map errorMessage = new HashMap<String, Object>() {{
                put("message", "Request Error. ");
            }};
            LOGGER.error(this.buildStandardLog(args, new DataResponse(errorMessage), requestDateTime, "-1"));
            throw new RuntimeException(e);
        }
    }

    private String buildStandardLog(Map<String, Object> args, DataResponse response, String requestDateTime,
            String costTime) {
        StringBuilder logStringBuilder = new StringBuilder("query_params: ");
        String requestData;
        try {
            requestData = objectMapper.writeValueAsString(args);
        } catch (JsonProcessingException e) {
            LOGGER.info(e.getMessage());
            requestData = args.toString();
        }
        logStringBuilder.append(requestData);

        logStringBuilder.append("&&cost_time: ");
        logStringBuilder.append(costTime);

        logStringBuilder.append("&&request_datetime: ");
        logStringBuilder.append(requestDateTime);

        logStringBuilder.append("&&module: ");
        logStringBuilder.append(this.module);

        logStringBuilder.append("&&url: ");
        logStringBuilder.append(this.url);

        logStringBuilder.append("&&response_code: ");
        logStringBuilder.append(response.code());

        logStringBuilder.append("&&response_message: ");
        logStringBuilder.append(response.message());

        logStringBuilder.append("&&response_result: ");
        logStringBuilder.append(response.isSuccess());

        logStringBuilder.append("&&request_id: ");
        logStringBuilder.append(UUID.randomUUID().toString());

        logStringBuilder.append("&&response_data: ");
        String responseData;
        try {
            responseData = objectMapper.writeValueAsString(response.data());
        } catch (JsonProcessingException e) {
            LOGGER.info(e.getMessage());
            responseData = response.data().toString();
        }
        logStringBuilder.append(responseData);

        return logStringBuilder.toString();
    }

    private String buildActualUrl(Map<String, Object> args) {
        for (Map.Entry<String, Object> entry : args.entrySet()) {
            String format = String.format("{%s}", entry.getKey());
            if (this.url.indexOf(format) != -1) {
                if (!(entry.getValue() instanceof String)) {
                    throw new RuntimeException(String.format("参数占位符 %s 替换符对应的值需为字符串", entry.getKey()));
                }
                this.url = this.url.replace(format, entry.getValue().toString());
            }
        }
        return this.url;
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

    private String buildGetUrl(Map<String, Object> args) {
        List<String> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : args.entrySet()) {
            if (entry.getValue() instanceof List) {
                for (String v : (List<String>) (entry.getValue())) {
                    params.add(entry.getKey() + "=" + v);
                }
            } else {
                params.add(entry.getKey() + "=" + entry.getValue());
            }
        }
        if (params.size() > 0) {
            return this.url + "?" + StringUtils.join(params, "&");
        } else {
            return this.url;
        }
    }

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
            connection.setConnectTimeout(3000);
            connection.setReadTimeout(60000);
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
            throw new HttpUtils.HttpError(responseCode, readAll(connection.getErrorStream()));
        }
    }
}
