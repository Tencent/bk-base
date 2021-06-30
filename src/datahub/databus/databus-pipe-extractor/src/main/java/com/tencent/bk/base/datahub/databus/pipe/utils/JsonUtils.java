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

package com.tencent.bk.base.datahub.databus.pipe.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 读取json列表.
     */
    @SuppressWarnings("rawtypes")
    public static List readList(String json) throws IOException {
        return objectMapper.readValue(json, List.class);
    }

    public static Map<String, Object> readMap(String json) throws IOException {
        return readMap(objectMapper, json);
    }

    /**
     * 读取json对象.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> readMap(ObjectMapper objectMapper, String json) throws IOException {
        if (StringUtils.isBlank(json)) {
            return new HashMap<>();
        } else {
            return objectMapper.readValue(json, HashMap.class);
        }
    }

    public static String writeValueAsString(Object obj) throws IOException {
        return writeValueAsString(objectMapper, obj);
    }

    /**
     * dump对象成字符串.
     */
    public static String writeValueAsString(ObjectMapper objectMapper, Object obj) throws IOException {
        return objectMapper.writeValueAsString(obj);
    }
}
