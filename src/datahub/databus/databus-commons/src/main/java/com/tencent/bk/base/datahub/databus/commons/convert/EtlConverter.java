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

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.FailedValue;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EtlConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(EtlConverter.class);

    private ETL etl;
    private SimpleDateFormat dateFormat;
    private BasicProps props = BasicProps.getInstance();
    private String etlConf;
    private int dataId;
    private Map<String, Map<String, String>> etlProps = new HashMap<>();

    protected EtlConverter(String conf, int dataId) {
        // TODO 修复初始化的逻辑
        LogUtils.debug(log, props.getCcCacheProps().toString());
        etlConf = conf;
        this.dataId = dataId;
        etlProps.put("cc_cache", props.getCcCacheProps());
        dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        initEtl();
    }

    /**
     * 初始化etl对象
     */
    private void initEtl() {
        validateEtlConf();
        try {
            // 创建etl对象
            etl = new ETLImpl(etlConf, etlProps);
        } catch (Exception e) {
            String msg = String.format("bad etl conf. dataId: %s, conf: %s ", dataId, etlConf);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, msg, e);
            etl = null;
            throw new ConnectException(msg, e);
        }
    }

    /**
     * 验证清洗的配置，当配置不正确时，抛出异常
     */
    private void validateEtlConf() {
        if (StringUtils.isNotBlank(etlConf)) {
            Map map = JsonUtils.toMapWithoutException(etlConf);
            if (map.containsKey(EtlConsts.EXTRACT) && map.containsKey(EtlConsts.CONF)) {
                return;
            }
        }
        // 非法的清洗配置，抛出异常
        throw new ConnectException(String.format("bad etl conf. dataId: %s, conf: %s ", dataId, etlConf));
    }

    /**
     * 将清洗的结果转化为Object列表,放入结果集中返回
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
     * @param keysInOrder 顺序排列的column列表
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
        result.setDateTime(getDateTime(timestamp * 1000));
        result.setMetricTag(dsTags.getOrDefault(Consts.COLLECTOR_DS_TAG, ""));

        try {
            ETLResult etlResult = etl.handle(msgValue);
            // 获取字段的index的列表
            List<Integer> idxs = new ArrayList<>(keysInOrder.length);
            Fields fields = etl.getSchema();
            for (String key : keysInOrder) {
                idxs.add(fields.fieldIndex(key));
            }

            // TODO 从TaskConext中获取 dataId (这里只允许输出到一个dataId中)
            for (Integer dataId : etlResult.getValues().keySet()) {
                List<List<Object>> list = etlResult.getValues().get(dataId);
                List<List<Object>> resultList = new ArrayList<>();
                // 将结果集按照指定的顺序排列字段,然后返回结果
                for (List<Object> record : list) {
                    List<Object> tmp = new ArrayList<>(idxs.size());
                    for (Integer idx : idxs) {
                        tmp.add(record.get(idx));
                    }
                    resultList.add(tmp);
                }
                //  TODO 如何保证数据是按照字段顺序排列的？
                result.setObjListResult(resultList);
                // 增加处理的异常记录
                List<FailedValue> failedList = etlResult.getFailedValues();
                for (FailedValue failed : failedList) {
                    result.addFailedResult(failed.toString());
                    result.addErrors(failed.getReason().getClass().getSimpleName());
                }
            }
        } catch (Exception e) {
            // 在解析结果中记录错误信息
            result.addErrors(e.getClass().getSimpleName());
            result.addFailedResult(e.getMessage());
            // 对打印到日志中的消息进行截断，控制输出的字符串长度
            if (msgValue.length() > 100) {
                msgValue = msgValue.substring(0, 100) + " ...";
            }
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_DATA_FOR_ETL, log,
                    dataId + " Bad Message: " + msgKey + " ## " + msgValue, e);
            // 重新构建etl对象
            initEtl();
        }

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
        try {
            String valueStr = new String(msgValue, etl.getEncoding());
            return getListObjects(msgKey, valueStr, keysInOrder);
        } catch (UnsupportedEncodingException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ENCODING,
                    "unsupported encoding in etl config. " + etl.getConfString(), e);
            return new ConvertResult();
        }
    }


    /**
     * 将清洗的结果转化为json字符串列表,放入结果集中返回。
     *
     * @param msgKey kafka msg key
     * @param msgValue kafka msg value
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
        result.setDateTime(getDateTime(timestamp * 1000));
        result.setMetricTag(dsTags.getOrDefault(Consts.COLLECTOR_DS_TAG, ""));

        try {
            ETLResult etlResult = etl.handle(msgValue);
            Fields fields = etl.getSchema();

            // TODO 从TaskConext中获取 dataId (这里只允许输出到一个dataId中)
            List<String> jsonList = new ArrayList<>();
            for (Integer dataId : etlResult.getValues().keySet()) {
                List<List<Object>> list = etlResult.getValues().get(dataId);
                //  TODO 如何保证数据是按照字段顺序排列的？
                for (List<Object> obj : list) {
                    Map<Object, Object> record = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = (Field) fields.get(i);
                        String name = field.getName();
                        // 这里转换为json时，需要将timestamp的值赋给dtEventTimeStamp字段，便于query模块查询es数据
                        if (name.equals(Consts.TIMESTAMP)) {
                            // timestamp需转换为dtEventTimeStamp，单位为毫秒
                            record.put(Consts.DTEVENTTIMESTAMP, obj.get(i));
                        } else if (obj.get(i) != null && (field.getType().equals(EtlConsts.BIGINT) || field.getType()
                                .equals(EtlConsts.BIGDECIMAL))) {
                            record.put(field.getName(), obj.get(i).toString());
                        } else {
                            record.put(field.getName(), obj.get(i));
                        }
                    }
                    try {
                        jsonList.add(JsonUtils.toJson(record));
                    } catch (Exception e) {
                        LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.JSON_FORMAT_ERR,
                                "unable to convert to json: " + record, e);
                        result.addFailedResult("unable to convert to json! " + e.getMessage());
                        result.addErrors(e.getClass().getSimpleName());
                    }
                }
                // 增加处理的异常记录
                List<FailedValue> failedList = etlResult.getFailedValues();
                for (FailedValue failed : failedList) {
                    result.addFailedResult(failed.toString());
                    result.addErrors(failed.getReason().getClass().getSimpleName());
                }
            }
            result.setJsonResult(jsonList);
        } catch (Exception e) {
            // 在解析结果中记录错误信息
            result.addErrors(e.getClass().getSimpleName());
            result.addFailedResult(e.getMessage());
            // 对打印到日志中的消息进行截断，控制输出的字符串长度
            if (msgValue.length() > 100) {
                msgValue = msgValue.substring(0, 100) + " ...";
            }
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_DATA_FOR_ETL, log,
                    dataId + " Bad Message: " + msgKey + " ## " + msgValue, e);
            // 重新构建etl对象
            initEtl();
        }

        return result;
    }

    @Override
    public ConvertResult getJsonList(String msgKey, byte[] msgValue) {
        try {
            String valueStr = new String(msgValue, etl.getEncoding());
            return getJsonList(msgKey, valueStr);
        } catch (UnsupportedEncodingException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_ENCODING,
                    "unsupported encoding in etl config. " + etl.getConfString(), e);
            return new ConvertResult();
        }
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
        // TODO 这里需实现将清洗结果转换为avro格式的数组，以便将相关逻辑从清洗包中剥离
        return new ConvertResult();
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
