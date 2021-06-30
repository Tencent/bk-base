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


import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

    protected JsonConverter() {
    }


    /**
     * 将原始数据进行处理，转换，放入结果集中返回
     *
     * @param msgKey kafka msg key，字符串
     * @param msgValue kafka msg value，字符串
     * @return 结果集对象
     */
    @Override
    public ConvertResult getListObjects(String msgKey, String msgValue, String[] keysInOrder) {
        ConvertResult result = new ConvertResult();
        result.setMsgSize(msgKey.length() + msgValue.length());

        if (msgValue.length() == 0 && msgKey.startsWith(Consts.DATABUS_EVENT)) {
            result.setIsDatabusEvent(true);
            return result;
        }

        Map<String, String> dsTags = Utils.readUrlTags(msgKey);
        long timestamp = Utils.getDsTimeTag(dsTags);
        result.setTagTime(timestamp);
        result.setMetricTag(dsTags.getOrDefault(Consts.COLLECTOR_DS_TAG, ""));

        // TODO 这里并不支持将json类型的数据转换为对象列表返回，所以这里返回的结果集为空列表
        result.addErrors("BadJsonListError");

        return result;
    }


    /**
     * 将清洗的结果转换为Object列表,放入结果集中返回。
     *
     * @param msgKey kafka msg key,字符串
     * @param msgValue kafka msg value,字节数组
     * @param keysInOrder 书序排列的column列表
     * @return 结果集对象
     */
    public ConvertResult getListObjects(String msgKey, byte[] msgValue, String[] keysInOrder) {
        // TODO 此处字符编码可能会有问题，需要从清洗或某处读取编码信息
        String valueStr = new String(msgValue, StandardCharsets.ISO_8859_1);
        return getListObjects(msgKey, valueStr, keysInOrder);
    }

    /**
     * 将原始数据进行处理，转换，放入结果集中返回
     *
     * @param msgKey kafka msg key，字符串
     * @param msgValue kafka msg value，字符串
     * @return 结果集对象
     */
    @Override
    public ConvertResult getJsonList(String msgKey, String msgValue) {
        ConvertResult result = new ConvertResult();
        result.setMsgSize(msgKey.length() + msgValue.length());

        if (msgValue.length() == 0 && msgKey.startsWith(Consts.DATABUS_EVENT)) {
            result.setIsDatabusEvent(true);
            return result;
        }

        Map<String, String> dsTags = Utils.readUrlTags(msgKey);
        long timestamp = Utils.getDsTimeTag(dsTags);
        result.setTagTime(timestamp);
        result.setMetricTag(dsTags.getOrDefault(Consts.COLLECTOR_DS_TAG, ""));

        // TODO 假设数据为json字符串，这里不对数据进行任何处理
        List<String> json = new ArrayList<>(1);
        json.add(msgValue);
        result.setJsonResult(json);

        return result;
    }

    @Override
    public ConvertResult getJsonList(String msgKey, byte[] msgValue) {
        String valueStr = new String(msgValue, StandardCharsets.ISO_8859_1);
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
}
