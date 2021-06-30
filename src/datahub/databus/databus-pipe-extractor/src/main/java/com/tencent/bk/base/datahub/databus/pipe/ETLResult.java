
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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据返回数据结构.
 */
public class ETLResult {

    /**
     * 解析后，正常的数据.
     */
    private Map<Integer, List<List<Object>>> values;

    /**
     * 解析后，异常的数据.
     */
    private List<FailedValue> failedValues;

    /**
     * 根据用户的ETL配置（有可能一个数据产生多个输出记录），应该返回的总记录条数.
     */
    private int totalMsgNum;

    /**
     * 数据切分出的量.
     */
    private int splitMsgNum = 0;

    /**
     * 解析正常的记录条数.
     */
    private int successMsgNum;

    private ETL etl;

    /**
     * 清洗结果.
     */
    @SuppressWarnings("rawtypes")
    public ETLResult(ETL etl, Map flattenedValues, List failedValues,
            int totalNum, int successNum, int splitMsgNum) {

        this.values = (Map<Integer, List<List<Object>>>) flattenedValues;
        this.failedValues = (List<FailedValue>) failedValues;

        this.totalMsgNum = totalNum;
        this.successMsgNum = successNum;
        this.splitMsgNum = splitMsgNum;

        this.etl = etl;
    }

    @Override
    public String toString() {
        List<Field> fields = new ArrayList<Field>();

        for (Object field : etl.getSchema()) {
            if (((Field) field).isOutputField()) {
                fields.add((Field) field);
            }
        }

        Map<Integer, List<List<Object>>> values = new HashMap<Integer, List<List<Object>>>();
        for (Integer dataid : getValues().keySet()) {
            values.put(dataid, new ArrayList<List<Object>>());
            for (List<Object> oneValue : getValues().get(dataid)) {
                List<Object> newOneValue = new ArrayList<Object>();
                for (int i = 0; i < oneValue.size(); i++) {
                    if (((Field) etl.getSchema().get(i)).isOutputField()) {
                        newOneValue.add(oneValue.get(i));
                    }
                }
                values.get(dataid).add(newOneValue);
            }
        }

        return "==\ntotalNum:" + getTotalMsgNum()
                + "\nsucceedNum:" + getSucceedMsgNum()
                + "\nfailedNum:" + getFailedMsgNum()
                + "\nshcema:" + fields
                + "\nnormal_values:" + values
                + "\nsplitMsgNUm:" + getSplitMsgNum()
                + "\nfailed_values:" + getFailedValues() + "\n==\n\n";
    }

    /**
     * 返回清洗后的结果数据.
     *
     * @return the values
     */
    public Map<Integer, List<List<Object>>> getValues() {
        return values;
    }

    /**
     * 返回清洗后的失败结果数据.
     *
     * @return the failedValues
     */
    public List<FailedValue> getFailedValues() {
        return failedValues;
    }

    /**
     * 获取清洗总条数.
     *
     * @return the totalMsgNum
     */
    public int getTotalMsgNum() {
        return totalMsgNum;
    }

    /**
     * 获取切分出去的条数.
     *
     * @return the splitMsgNum
     */
    public int getSplitMsgNum() {
        return splitMsgNum;
    }

    /**
     * 返回成功清洗的条数.
     *
     * @return the successMsgNum
     */
    public int getSucceedMsgNum() {
        return successMsgNum;
    }


    public int getFailedMsgNum() {
        return totalMsgNum - successMsgNum;
    }

    /**
     * 根据字段名称获取数据对象.
     */
    public Object getValByName(List<Object> oneValue, String fieldName) {
        return oneValue.get(etl.getSchema().fieldIndex(fieldName));
    }

    /**
     * 返回序列化好的数据.
     */
    public Map<Integer, List<String>> getSerializedValues() {
        String delimiter = etl.getDelimiter();
        Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
        for (Map.Entry<Integer, List<List<Object>>> entry : values.entrySet()) {
            List<String> l = new ArrayList<String>();
            for (List<Object> oneValue : entry.getValue()) {
                l.add(StringUtils.join(oneValue, delimiter));
            }
            ret.put(entry.getKey(), l);
        }
        return ret;
    }

}
