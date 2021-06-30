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

package com.tencent.bk.base.datahub.databus.commons.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DockerLogConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(DockerLogConverter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public ConvertResult getListObjects(String msgKey, String msgValue, String[] keysInOrder) {
        return new ConvertResult();
    }

    public ConvertResult getListObjects(String msgKey, byte[] msgValue, String[] keysInOrder) {
        return new ConvertResult();
    }

    /**
     * 获取json list转换结果
     *
     * @param msgKey kafka消息key
     * @param msgValue kafka消息value
     * @return json list转换结果
     */
    public ConvertResult getJsonList(String msgKey, String msgValue) {
        ConvertResult result = new ConvertResult();
        result.setMsgSize(msgKey.length() + msgValue.length());
        long timestamp = getDsTimeTag(msgKey);
        result.setTagTime(timestamp);
        result.setDateTime(getDateTime(timestamp * 1000));
        try {
            JsonNode node = MAPPER.readTree(msgValue);
            // 获取namespace字段的值,后续作为分发的目的地
            String nameSpace = node.get("container").get("labels").get("io.tencent.bcs.namespace").textValue();
            result.setDockerNameSpace(nameSpace);
            List<String> jsonList = new ArrayList<>();
            jsonList.add(msgValue);
            // 将原样的json数据放回到
            result.setJsonResult(jsonList);
        } catch (Exception ignore) {
            result.addFailedResult("unable to read namespace field from json! " + ignore.getMessage());
            result.addErrors(ignore.getClass().getSimpleName());
            LogUtils.warn(log, "bad json of docker log: " + msgValue, ignore);
        }

        return result;
    }

    @Override
    public ConvertResult getJsonList(String msgKey, byte[] msgValue) {
        String valueStr = new String(msgValue, StandardCharsets.UTF_8);
        return getJsonList(msgKey, valueStr);
    }

    /**
     * 将原始数据进行处理，转换，放入结果集中返回
     *
     * @param msgKey kafka msg key，字符串
     * @param msgValue kafka msg value，字符串
     * @return 结果集对象
     */
    @Override
    public ConvertResult getKeyValueList(String msgKey, String msgValue, char kvSplitter, char fieldSplitter) {
        return new ConvertResult();
    }

    /**
     * 从数据中解析出avro的GenericArray对象，包含多条记录
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @return 包含多条记录的结果
     */
    @Override
    public ConvertResult getAvroArrayResult(String msgKey, String msgValue) {
        return new ConvertResult();
    }


    /**
     * 获取datasvr里的时间戳打点信息,并返回时间戳的值。当不存在时,返回0。
     *
     * @param key kafka msg的key值,里面包含datasvr的打点信息
     * @return datasvr里的时间戳信息
     */
    private Long getDsTimeTag(String key) {
        Map<String, String> dsTags = readDataServerTags(key);
        Long dsTagOrig = 0L;
        try {
            dsTagOrig = Long.parseLong(dsTags.get("collector-ds"));
        } catch (NumberFormatException ignore) {
            LogUtils.debug(log, "failed to get dsTagOrig from ds tags {} {}", key, ignore.getMessage());
        }

        return dsTagOrig;
    }

    /**
     * 读取datasvr的打点tag信息
     *
     * @param data datasvr打点的key字符串
     * @return 解析后的tag信息map
     */
    private Map<String, String> readDataServerTags(String data) {
        Map<String, String> map = new HashMap<>();

        try {
            data = URLDecoder.decode(data, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            LogUtils.warn(log, "failed to decode ds tags {} {}", data, e.getMessage());
            return map;
        }

        String[] params = data.split("&");
        for (String param : params) {
            String[] parts = param.split("=");
            String name = parts[0];
            String value = parts.length > 1 ? parts[1] : "";
            map.put(name, value);
        }
        return map;
    }

    /**
     * 将时间戳转换为yyyyMMddHHmmss格式的数值
     *
     * @param timestampInMs 时间戳
     * @return 数值格式的年月日时分秒
     */
    private long getDateTime(long timestampInMs) {
        String dateStr = dateFormat.format(new Date(timestampInMs));
        return NumberUtils.toLong(dateStr);

    }
}
