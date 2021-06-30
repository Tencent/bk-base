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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtils {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        MAPPER.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
    }

    /**
     * 解析api返回的结果，当解析失败时，返回默认的api返回结果（请求失败）
     *
     * @param response API返回结果字符串
     * @return 解析后的API返回结果对象，包含result/code/errors/message/data等信息
     */
    public static ApiResult parseApiResult(String response) {
        try {
            return MAPPER.readValue(response, ApiResult.class);
        } catch (IOException e) {
            LogUtils.warn(log, "failed to parse api response: " + response, e);
            return new ApiResult();
        }
    }

    /**
     * 解析json字符串，转换为java对象
     *
     * @param json json字符串
     * @param clazz java对象的class名称
     * @param <T> java对象的类型
     * @return java对象
     * @throws IOException 异常
     */
    public static <T> T parseBean(String json, Class<T> clazz) throws IOException {
        return MAPPER.readValue(json, clazz);
    }

    /**
     * 将java对象转换为json字符串，当发生异常时，返回空的json串
     *
     * @param obj java对象
     * @return json字符串
     */
    public static String toJsonWithoutException(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LogUtils.warn(log, "failed to covert object as json string: " + obj.toString(), e);
        }
        return "{}"; // 返回空的json串
    }

    /**
     * 将java对象转换为json格式化字符串，当发生异常时，返回空的json串
     *
     * @param obj java对象
     * @return json字符串
     */
    public static String toPrettyJsonWithoutException(Object obj) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LogUtils.warn(log, "failed to covert object as pretty json string: " + obj.toString(), e);
        }
        return "{}"; // 返回空的json串
    }

    /**
     * 将java对象转换为json字符串，当发生异常时，抛出异常
     *
     * @param obj java对象
     * @return json字符串
     * @throws IOException 异常
     */
    public static String toJson(Object obj) throws IOException {
        return MAPPER.writeValueAsString(obj);
    }

    /**
     * 将json字符串解析为map结构的java对象，当发生异常时，返回空的map对象
     *
     * @param json json字符串
     * @return map对象
     */
    public static Map<String, Object> toMapWithoutException(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            LogUtils.warn(log, String.format("failed to convert json to map: %s", json), e);
            return new HashMap<>();
        }
    }

    /**
     * 将json字符串解析为map结构的java对象，当发生异常时，抛出异常
     *
     * @param json json字符串
     * @return map对象
     * @throws IOException 异常
     */
    public static Map<String, Object> toMap(String json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
        });
    }

    /**
     * 将json字符串解析为List结构的java对象，当发生异常时，返回空的list对象
     *
     * @param json json字符串
     * @return list对象
     */
    public static List<Object> toListWithoutException(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<List<Object>>() {
            });
        } catch (IOException e) {
            LogUtils.warn(log, String.format("failed to convert json to list: %s", json), e);
            return new ArrayList<>();
        }
    }

    /**
     * 将json字符串解析为List结构的java对象，当发生异常时，抛出异常
     *
     * @param json json字符串
     * @return list对象
     * @throws IOException 异常
     */
    public static List<Object> toList(String json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<List<Object>>() {
        });
    }

    /**
     * 读取json文件
     *
     * @param filePath 文件路径
     * @return map
     */
    public static Map<Object, Object> readJsonFileWithoutException(String filePath) {
        try {
            return MAPPER.readValue(new File(filePath), new TypeReference<Map<Object, Object>>() {
            });
        } catch (IOException e) {
            LogUtils.warn(log, "failed to read json file: {}", filePath, e);
            return new HashMap<>();
        }
    }

}
