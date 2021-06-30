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

package com.tencent.bk.base.dataflow.codecheck.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

    /**
     * readList
     *
     * @param json
     * @return java.util.List
     */
    public static List readList(String json) {
        try {
            return objectMapper.readValue(json, List.class);
        } catch (IOException e) {
            LOGGER.error("the json is : " + json);
            throw new RuntimeException(e);
        }
    }

    /**
     * readMap
     *
     * @param json
     * @return java.util.Map-java.lang.String, java.lang.Object
     */
    public static Map<String, Object> readMap(String json) {
        return readMap(objectMapper, json);
    }


    /**
     * readMap
     *
     * @param objectMapper
     * @param json
     * @return java.util.Map-java.lang.String, java.lang.Object
     */
    private static Map<String, Object> readMap(ObjectMapper objectMapper, String json) {
        if (StringUtils.isBlank(json)) {
            return new HashMap<String, Object>();
        }
        try {
            return objectMapper.readValue(json, HashMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * readMap
     *
     * @param is
     * @return java.util.Map-java.lang.String, java.lang.Object
     */
    public static Map<String, Object> readMap(InputStream is) {
        try {
            return objectMapper.readValue(is, HashMap.class);
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

    /**
     * writeValueAsString
     *
     * @param obj
     * @return java.lang.String
     */
    public static String writeValueAsString(Object obj) {
        return writeValueAsString(objectMapper, obj);
    }

    /**
     * writeValueAsString
     *
     * @param objectMapper
     * @param obj
     * @return java.lang.String
     */
    private static String writeValueAsString(ObjectMapper objectMapper, Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
