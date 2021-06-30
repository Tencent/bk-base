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


package com.tencent.bk.base.datahub.databus.connect.sink.es;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexAndMapping {

    private static final Logger log = LoggerFactory.getLogger(IndexAndMapping.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 获取result table的字段列表
     *
     * @param columns rt中定义的字段列表
     * @return result table的字段列表
     */
    public static Map<String, String> getFieldsForResultTable(Map<String, String> columns) {
        Map<String, String> result = new HashMap<>();
        result.put(Consts.DTEVENTTIME, "date");
        result.put(Consts.DTEVENTTIMESTAMP, "date");
        result.putAll(columns);
        result.remove(Consts.OFFSET);
        result.remove(Consts.TIMESTAMP);
        return result;
    }

    /**
     * 使用字段列表和字段特殊配置信息来构建mapping的json字符串。
     *
     * @param fields 字段列表
     * @param jsonConf json配置
     * @return json字符串的mapping
     */
    public static String constructMapping(String indexType, Map<String, String> fields, String jsonConf) {
        String result = "{}";
        // 解析额外的json配置数据
        Set<String> analyzedFields = new HashSet<>();
        Set<String> dateFields = new HashSet<>();
        try {
            JsonNode json = MAPPER.readTree(jsonConf);
            ArrayNode analyzedFieldsArr = (ArrayNode) json.get("analyzedFields");
            for (int i = 0; i < analyzedFieldsArr.size(); i++) {
                JsonNode node = analyzedFieldsArr.get(i);
                analyzedFields.add(node.asText());
            }

            ArrayNode dateFieldsArr = (ArrayNode) json.get("dateFields");
            for (int i = 0; i < dateFieldsArr.size(); i++) {
                JsonNode node = dateFieldsArr.get(i);
                dateFields.add(node.asText());
            }
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.ES_BAD_CONFIG_FOR_MAPPING,
                    "parse jsonConf failed, just ignore this config! jsonConfig: " + jsonConf, e);
        }

        // 构建mapping
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject().startObject(indexType);
            mapping.startObject("properties");
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                mapping.startObject(entry.getKey()); // object start
                String type = entry.getValue();
                if (dateFields.contains(entry.getKey())) {
                    mapping.field("type", "date")
                            .field("format", "strict_date_optional_time||yyyy-MM-dd HH:mm:ss||epoch_millis");
                } else {
                    if ("int".equals(type)) {
                        mapping.field("type", "integer");
                    } else if ("string".equals(type) || "long".equals(type) || "double".equals(type) || "float"
                            .equals(type)) {
                        mapping.field("type", type);
                    } else {
                        mapping.field("type", "string");
                    }
                }
                if (!analyzedFields.contains(entry.getKey())) { // 不分词的字段不包含在_all中
                    mapping.field("index", "not_analyzed");
                }
                mapping.endObject();  // object end
            }
            mapping.endObject(); // end the properties object
            mapping.endObject().endObject(); // end the indexType object
            result = mapping.toString();
            LogUtils.debug(log, "mapping: {}", result);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.ES_BUILD_MAPPING_FAIL,
                    "failed to construct the mapping! jsonConfig: " + jsonConf, e);
        }

        return result;
    }

}
