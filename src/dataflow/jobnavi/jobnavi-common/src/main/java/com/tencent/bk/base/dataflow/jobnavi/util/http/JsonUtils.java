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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class JsonUtils {

    private static final Logger LOGGER = Logger.getLogger(JsonUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * parse json to list
     *
     * @param json
     * @return
     */
    public static List<?> readList(String json) {
        try {
            return objectMapper.readValue(json, List.class);
        } catch (IOException e) {
            LOGGER.error("the json is : " + json);
            throw new RuntimeException(e);
        }
    }

    /**
     * parse json map from string
     *
     * @param json
     * @return json map
     */
    public static Map<String, Object> readMap(String json) {
        if (StringUtils.isBlank(json)) {
            return new HashMap<>();
        }
        try {
            Map<?, ?> parsedMap = objectMapper.readValue(json, Map.class);
            Map<String, Object> jsonMap =  new HashMap<>();
            for (Map.Entry<?, ?> entry : parsedMap.entrySet()) {
                Object key = entry.getKey();
                Object value = entry.getValue();
                if (key instanceof String) {
                    jsonMap.put((String) key, value);
                }
            }
            return jsonMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * read json map from stream
     *
     * @param is
     * @return json map
     */
    public static Map<String, Object> readMap(InputStream is) {
        try {
            Map<?, ?> parsedMap = objectMapper.readValue(is, HashMap.class);
            Map<String, Object> jsonMap =  new HashMap<>();
            for (Map.Entry<?, ?> entry : parsedMap.entrySet()) {
                Object key = entry.getKey();
                Object value = entry.getValue();
                if (key instanceof String) {
                    jsonMap.put((String) key, value);
                }
            }
            return jsonMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                LOGGER.error("failed to close", e);
            }
        }
    }

    public static String writeValueAsString(Object obj) {
        return writeValueAsString(objectMapper, obj);
    }

    /**
     * serialize object to json string
     *
     * @param objectMapper
     * @param obj
     * @return
     */
    public static String writeValueAsString(ObjectMapper objectMapper, Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * check if the given string is a valid json string
     *
     * @param json string to check
     * @return true if valid
     */
    public static boolean validateJson(String json) {
        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
