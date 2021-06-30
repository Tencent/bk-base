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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ConvertResult {

    // 结果对象列表
    private List<List<Object>> objListResult = new ArrayList<>();
    private List<String> jsonResult = new ArrayList<>();
    private List<String> kvResult = new ArrayList<>();
    private StringBuilder failedResult = new StringBuilder();
    private List<String> errors = new ArrayList<>();
    private Schema avroSchema = null;
    private GenericArray<GenericRecord> avroValues = null;
    private long tagTime = 0;
    private int msgSize = 0;
    private long dateTime = 0;    // yyyyMMddHHmmss
    private String metricTag = "";
    private String dockerNameSpace = ""; // docker日志中的namespace字段值
    private boolean isDatabusEvent = false; // 标记当前的消息是否是一个总线的事件消息

    public ConvertResult() {
    }

    /**
     * 设置object array格式的结果集
     *
     * @param result Object List格式的结果列表
     */
    protected void setObjListResult(List<List<Object>> result) {
        this.objListResult = result;
    }

    /**
     * 设置json格式的结果集
     *
     * @param result json格式的结果列表
     */
    protected void setJsonResult(List<String> result) {
        this.jsonResult = result;
    }

    /**
     * 设置key-value格式的结果集
     *
     * @param result key-valud格式的结果列表
     */
    protected void setKvResult(List<String> result) {
        this.kvResult = result;
    }

    /**
     * 设置处理失败的结果集
     *
     * @param result 失败的提示信息
     */
    protected void addFailedResult(String result) {
        this.failedResult.append(result).append("\n");
    }

    /**
     * 添加处理数据时的异常
     *
     * @param error 异常描述字符串
     */
    protected void addErrors(String error) {
        this.errors.add(error);
    }

    /**
     * 设置本条数据的avro schema
     *
     * @param schema avro schema对象
     */
    protected void setAvroSchema(Schema schema) {
        this.avroSchema = schema;
    }

    /**
     * 设置avro记录的值
     *
     * @param array avro数据的数组
     */
    protected void setAvroValues(GenericArray<GenericRecord> array) {
        this.avroValues = array;
    }

    /**
     * 设置消息中的tagTime
     *
     * @param time 时间
     */
    public void setTagTime(long time) {
        this.tagTime = time;
    }

    public void setMsgSize(int size) {
        this.msgSize = size;
    }

    public void setDateTime(long dateTime) {
        this.dateTime = dateTime;
    }

    public void setMetricTag(String metricTag) {
        this.metricTag = metricTag;
    }

    protected void setDockerNameSpace(String nameSpace) {
        this.dockerNameSpace = nameSpace;
    }

    public void setIsDatabusEvent(boolean bool) {
        this.isDatabusEvent = bool;
    }

    // getters
    public List<List<Object>> getObjListResult() {
        return objListResult;
    }

    /**
     * 这里jsonResult可能是初始值（空集），这种情况下需检查avroValues是否有值，如果有值，
     * 则将其转换为json list返回
     *
     * @return json格式的数据列表
     */
    public List<String> getJsonResult() {
        if (jsonResult.isEmpty() && avroValues != null) {
            jsonResult = AvroConverter.convertAvroArrayToJsonList(avroValues);
        }
        return jsonResult;
    }

    /**
     * 这里kvResult可能是初始值（空集），这种情况下需检查avroValues是否有值，如果有值，
     * 则将其转换为key-valud list返回
     *
     * @return key-valud格式的数据列表
     */
    public List<String> getKeyValueResult(char kvSplitter, char fieldSplitter) {
        if (kvResult.isEmpty() && avroValues != null) {
            kvResult = AvroConverter.convertAvroArrayToKvList(avroValues, kvSplitter, fieldSplitter);
        }
        return kvResult;
    }

    public String getFailedResult() {
        return failedResult.toString();
    }

    public List<String> getErrors() {
        return errors;
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }

    public GenericArray<GenericRecord> getAvroValues() {
        return avroValues;
    }

    public long getTagTime() {
        return tagTime;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public int getTheDate() {
        return (int) (dateTime / 1000000);
    }

    public long getDateTime() {
        return dateTime;
    }

    public String getMetricTag() {
        return metricTag;
    }

    public String getDockerNameSpace() {
        return dockerNameSpace;
    }

    public boolean getIsDatabusEvent() {
        return isDatabusEvent;
    }

    /**
     * 获取数据的tag信息
     *
     * @return tag信息(metricTag或者tagTime)
     */
    public String getTag() {
        if (StringUtils.isBlank(metricTag)) {
            return "" + tagTime;
        } else {
            return metricTag;
        }
    }

    @Override
    public String toString() {
        return "{objListResult=" + objListResult
                + ", jsonResult=" + jsonResult
                + ", kvResult=" + kvResult
                + ", failedResult=" + failedResult
                + ", msgSize=" + msgSize
                + ", dateTime=" + dateTime
                + ", tagTime=" + tagTime + "}";
    }
}
