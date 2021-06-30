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

package com.tencent.bk.base.datalab.queryengine.server.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.IOException;
import java.util.Map;

/**
 * Http 工具类
 */
public class HttpUtil {

    static {
        Unirest.setObjectMapper(new com.mashape.unirest.http.ObjectMapper() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public <T> T readValue(String s, Class<T> t) {
                try {
                    return objectMapper.readValue(s, t);
                } catch (IOException e) {
                    throw new IllegalArgumentException(s, e);
                }
            }

            @Override
            public String writeValue(Object o) {
                try {
                    return objectMapper.writeValueAsString(o);
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException(o.toString());
                }
            }
        });
    }

    /**
     * 发送 http get 请求
     *
     * @param url 请求 url
     * @return JsonNode 返回信息
     * @throws UnirestException When connect or read timeout
     */
    public static JsonNode httpGet(String url) throws UnirestException {
        return Unirest.get(url)
                .header("accept", "application/json")
                .header("Content-Type", "application/json")
                .asJson()
                .getBody();
    }

    /**
     * 发送 http post 请求
     *
     * @param url 请求 url
     * @param params 请求体
     * @return JsonNode 返回信息
     * @throws UnirestException When connect or read timeout
     */
    public static JsonNode httpPost(String url, Map<String, Object> params) throws UnirestException {
        return Unirest.post(url)
                .header("accept", "application/json")
                .header("Content-Type", "application/json")
                .body(params)
                .asJson()
                .getBody();
    }

    /**
     * 发送 http delete 请求
     *
     * @param url 请求 url
     * @param body 请求体
     * @return JsonNode 返回信息
     * @throws UnirestException When connect or read timeout
     */
    public static JsonNode httpDelete(String url, String body) throws UnirestException {
        return Unirest.delete(url)
                .header("accept", "application/json")
                .header("Content-Type", "application/json")
                .body("{\"queryset_delete\": true}")
                .asJson()
                .getBody();
    }
}
