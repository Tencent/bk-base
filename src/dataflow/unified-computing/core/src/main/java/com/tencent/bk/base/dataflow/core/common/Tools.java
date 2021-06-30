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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Tools {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tools.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 将object转换为string
     *
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

    /**
     * 将json转换为map
     *
     * @param json 需要转换的json
     * @return 返回转换后的map
     */
    public static Map<String, Object> readMap(String json) {
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
     * 将 json转换为list
     *
     * @param json json
     * @return list
     */
    public static List readList(String json) {
        try {
            return objectMapper.readValue(json, List.class);
        } catch (IOException e) {
            LOGGER.error("the json is : {}", json);
            throw new RuntimeException(e);
        }
    }

    /**
     * pause
     *
     * @param second second
     */
    public static void pause(int second) {
        // 30 minutes max
        if (second > 30 * 60) {
            second = 30 * 60;
        }
        try {
            Thread.sleep(second * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
