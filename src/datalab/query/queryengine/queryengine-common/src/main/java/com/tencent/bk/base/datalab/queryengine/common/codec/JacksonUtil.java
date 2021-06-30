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

package com.tencent.bk.base.datalab.queryengine.common.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public final class JacksonUtil {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        //序列化时候统一日期格式
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        //设置 null 时候不序列化(只针对对象属性)
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        //反序列化时，属性不存在的兼容处理
        OBJECT_MAPPER.getDeserializationConfig()
                .withoutFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //单引号处理
        OBJECT_MAPPER
                .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    /**
     * 将 json 字符串转成相应的 Map
     *
     * @param jsonStr json 字符串
     * @return Map 实例
     */
    public static Map<String, Object> convertJson2Map(String jsonStr) {
        return json2Object(jsonStr, Map.class);
    }

    /**
     * 将 json 转成相应的 List
     *
     * @param jsonStr json 字符串
     * @return List 实例
     */
    public static List convertJson2List(String jsonStr) {
        return json2Object(jsonStr, List.class);
    }

    /**
     * 反序列化 json 字符串
     *
     * @param json json 字符串
     * @param clazz class object
     * @param <T> Object Class type
     * @return 反序列化后的对象
     */
    public static <T> T json2Object(String json, Class<T> clazz) {
        T result;
        try {
            result = OBJECT_MAPPER.readValue(checkedData(json), clazz);
        } catch (Exception e) {
            result = null;
        }
        return result;
    }

    /**
     * 反序列化 obj
     *
     * @param obj origin object
     * @param clazz class object
     * @param <T> Object Class type
     * @return 反序列化后的对象
     */
    public static <T> T convertValue(Object obj, Class<T> clazz) {
        T result;
        try {
            result = OBJECT_MAPPER.convertValue(obj, clazz);
        } catch (Exception e) {
            result = null;
        }
        return result;
    }

    /**
     * 序列化 java 对象
     *
     * @param entity object instance
     * @param <T> object class type
     * @return json string
     */
    public static <T> String object2Json(T entity) {
        String result;
        try {
            result = OBJECT_MAPPER.writeValueAsString(entity);
        } catch (Exception e) {
            result = null;
        }
        return result;
    }

    /**
     * 将 json 字符串转为 JsonNode
     *
     * @param json json 字符串
     * @return 返回 JsonNode
     */
    public static JsonNode readTree(String json) {
        JsonNode result;
        try {
            result = OBJECT_MAPPER.readTree(checkedData(json));
        } catch (Exception e) {
            result = null;
        }
        return result;
    }

    /**
     * java 对象序列化为 JsonNode 对象
     *
     * @param value java 对象
     * @param <T> JsonNode class type
     * @return JsonNode instance
     */
    public static <T extends JsonNode> T valueToTree(Object value) {
        T result;
        try {
            result = OBJECT_MAPPER.valueToTree(value);
        } catch (Exception e) {
            e.printStackTrace();
            result = null;
        }
        return result;
    }

    /**
     * treeNode 反序列化为 java 对象
     *
     * @param treeNode TreeNode实例
     * @param tClass class object of T
     * @param <T> object class type
     * @return object instance
     */
    public static <T> T treeToValue(TreeNode treeNode, Class<T> tClass) {
        T result;
        try {
            result = OBJECT_MAPPER.treeToValue(treeNode, tClass);
        } catch (Exception e) {
            result = null;
        }
        return result;
    }

    /**
     * 数据合法性校验
     *
     * @param json 输入字符串
     * @return 校验后的数据
     */
    public static String checkedData(String json) {
        if (StringUtils.isBlank(json)) {
            return "";
        }
        return json;
    }
}