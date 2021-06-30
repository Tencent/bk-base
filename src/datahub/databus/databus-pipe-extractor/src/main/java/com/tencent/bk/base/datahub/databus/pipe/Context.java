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

import com.tencent.bk.base.datahub.databus.pipe.cal.Cal;
import com.tencent.bk.base.datahub.databus.pipe.convert.Convert;
import com.tencent.bk.base.datahub.databus.pipe.dispatch.Expr;
import com.tencent.bk.base.datahub.databus.pipe.exception.TimeFormatError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Context {

    public String origData;

    private static final Logger log = LoggerFactory.getLogger(Context.class);
    private Fields schema;
    private List<Object> values;
    private boolean isVerifyConf = false;
    private EtlParseDetails etlParseDetails = new EtlParseDetails();
    public Map<String, String> ccCacheConfig = null;


    /**
     * 解析错误的日志.
     */
    public List<FailedValue> badValues;
    Stack<Fields> schemaStack;
    Stack<List<Object>> valuesStack;

    // 平坦化的数据条数
    private int totalMsgNum;
    // 成功平坦化的数据条数
    private int successMsgNum;
    // 数据切分的计数
    private int splitMsgNum;

    public ETLImpl etl;
    public Map<String, Integer> ignoreFields;

    private void init() {
        schema = new Fields();
        schemaStack = new Stack<>();
        valuesStack = new Stack<>();
        badValues = new ArrayList<>();
        ignoreFields = new HashMap<>();
        clearValues();
    }

    public Context() {
        init();
    }

    public Context(ETLImpl etl) {
        this.etl = etl;
        init();
    }

    /**
     * 扩充解析失败列表.
     */
    public void appendBadValues(Object data, RuntimeException e) {
        FailedValue item = new FailedValue(data, e);
        badValues.add(item);
    }

    /**
     * 扩充解析失败列表.
     */
    public void appendBadValues(Object data, RuntimeException e, boolean allFailed) {
        if (allFailed) {
            successMsgNum = 0;
        }
        appendBadValues(data, e);
    }


    /**
     * 重置context中收集的数据.
     */
    public void clearValues() {
        values = new ArrayList<Object>();
        badValues = new ArrayList<FailedValue>();
        totalMsgNum = 0;
        successMsgNum = 0;
        splitMsgNum = 0;
    }

    /**
     * 遇到Fields类型的字段需要将当前的schema推到栈中, 采用新的子schema.
     */
    public void pushSchemaStack() {
        schemaStack.push(schema);
        Fields subSchema = new Fields();
        schema.append(subSchema);
        schema = subSchema;
    }

    /**
     * 取得上层的schema.
     */
    public void popSchemaStack() {
        schema = schemaStack.pop();
    }

    /**
     * 数据压栈.
     */
    public void pushValuesStack() {
        valuesStack.push(values);
        List<Object> subValues = new ArrayList<Object>();
        values.add((Object) subValues);
        values = subValues;
    }

    /**
     * 数据压栈.
     */
    public void pushValuesStack(Fields schema) {
        valuesStack.push(values);
        List<Object> subValues = new ArrayList<Object>();
        setValue(schema, subValues);
        values = subValues;
    }

    /**
     * 再填充值的之后会根据schema中的字段信息来填充(主要是map的提取). 这里需要保证list已经初始化好足够的位置
     */
    public void ensureValuesCapacity(int minimal) {
        int more = minimal - values.size();
        while (more-- > 0) {
            values.add(null);
        }
    }

    public void popValuesStack() {
        values = valuesStack.pop();
    }

    /**
     * 获取schema.
     *
     * @return the schema
     */
    public Fields getSchema() {
        return schema;
    }

    /**
     * 设置schema.
     *
     * @param schema the schema to set
     */
    public void setSchema(Fields schema) {
        this.schema = schema;
    }

    /**
     * 获取清洗后的数据.
     *
     * @return the values
     */
    public List<Object> getValues() {
        return values;
    }

    /**
     * 设置值.
     *
     * @param values the values to set
     */
    public void setValues(List<Object> values) {
        this.values = values;
    }

    /**
     * 增长消息总条数计数.
     */
    public void incrTotalMsgNum() {
        this.totalMsgNum += 1;
    }

    public void incrTotalMsgNum(int num) {
        this.totalMsgNum += num;
    }


    /**
     * 增长成功请你先消息计数+1.
     */
    public void incrSuccessMsgNum() {
        this.successMsgNum += 1;
    }

    public void incrSuccessMsgNum(int num) {
        this.successMsgNum += num;
    }


    /**
     * 根据字段对象设置在values中的值.
     */
    public void setValue(Field field, Object value) {
        int index = getSchema().fieldIndex(field);
        ensureValuesCapacity(index + 1);
        values.set(index, value);
    }

    /**
     * 根据字段对象设置在values中的值.
     */
    public void setValue(Fields fields, Object value) {
        int index = getSchema().fieldIndex(fields);
        ensureValuesCapacity(index + 1);
        values.set(index, value);
    }

    /**
     * 平坦化结构字段信息.
     */
    public List<Field> flattenSchema(Fields schema) {
        List<Field> joined = new ArrayList<Field>();
        int innerIdx = -1;

        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i) instanceof Fields) {
                innerIdx = i;
            } else {
                Field field = (Field) schema.get(i);
                joined.add(field);
            }
        }

        if (innerIdx != -1) {
            joined.addAll(flattenSchema((Fields) schema.get(innerIdx)));
        }

        return joined;
    }

    /**
     * 平坦化收集的值信息.
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> flattenValues(Fields schema, List<Object> values) {

        List<List<Object>> joined = new ArrayList<List<Object>>();
        List<Object> sampleValues = new ArrayList<Object>();
        int innerIdx = -1;

        if (values.size() != schema.size()) {
            return joined;
        }

        //找到嵌套的迭代器， 当时我为什么要这么做？
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i) instanceof Fields) {
                innerIdx = i; //难道这里写是假设数据中只有一个嵌套的迭代么？
            } else {
                sampleValues.add(values.get(i));
            }
        }

        if (innerIdx != -1) {

            if (values.get(innerIdx) == null) {
                int dataId = etl == null ? 0 : etl.getDataId();
                log.debug("{} null context: {} {}, orig data: {}", dataId, values, schema, origData);
                return joined;
            }

            List<List<Object>> complex = new ArrayList<List<Object>>();
            for (List<Object> oneField : (List<List<Object>>) values.get(innerIdx)) {
                complex.addAll(flattenValues((Fields) schema.get(innerIdx), oneField));
            }

            //sample * complex 将嵌套的迭代和外层简单的数据进行笛卡尔积计算
            for (List<Object> oneList : complex) {
                List<Object> collector = new ArrayList<Object>();

                for (Object sampleItem : sampleValues) {
                    collector.add(sampleItem);
                }
                for (Object complexItem : oneList) {
                    collector.add(complexItem);
                }

                joined.add(collector);
            }
        } else {
            joined.add(sampleValues);
        }

        return joined;
    }

    /**
     * 创建ETLResult对象，返回ETL的结果.
     */
    @SuppressWarnings("unchecked")
    public ETLResult toETLResult(ETLImpl etl) {

        Map<Integer, Object> mvalue = new HashMap<Integer, Object>();

        if (etl.getDerive() != null) {
            etl.getDerive().appendPlaceHolder(this);
        }

        // 数据计算
        if (etl.getCals() != null) {
            for (Cal cal : etl.getCals()) {
                cal.appendPlaceHolder(this, null);
            }
        }

        List<List<Object>> flattenedValue = flattenValues(getSchema(), getValues());

        if (etl.timeFieldIdx != -1) {
            flattenedValue = handleTimeField(flattenedValue);
        }

        // 数据转换
        if (etl.converts != null) {
            for (List<Object> oneValue : flattenedValue) {
                for (Convert convert : etl.converts) {
                    if (null != convert && null != oneValue) {
                        oneValue = convert.execute(etl, oneValue);
                    }
                }
            }
        }

        // 数据计算
        if (etl.getCals() != null) {
            for (Cal cal : etl.getCals()) {
                flattenedValue = cal.execute(flattenedValue, etl.getSchema());
            }
        }

        // 数据派生
        if (etl.getDerive() != null) {
            List<List<Object>> newFlattenedValue = new ArrayList<List<Object>>();
            for (List<Object> oneValue : flattenedValue) {
                newFlattenedValue.addAll(etl.getDerive().execute(oneValue, this));
            }
            flattenedValue = newFlattenedValue;
        }

        // 数据切分
        if (etl.dispatch == null) {
            mvalue.put(etl.getDataId(), flattenedValue);
        } else {
            mvalue.put(etl.getDataId(), flattenedValue);
            this.handleDispatch(flattenedValue, mvalue);
        }

        return new ETLResult(etl, mvalue, badValues, totalMsgNum, successMsgNum, splitMsgNum);
    }

    private void handleDispatch(List<List<Object>> flattenedValue, Map<Integer, Object> mvalue) {
        for (List<Object> oneValue : flattenedValue) {

            for (Integer dataId : etl.dispatch.keySet()) {

                Expr expr = etl.dispatch.get(dataId);
                if (expr.execute(etl, oneValue)) {
                    splitMsgNum++;
                    List<List<Object>> l = (List<List<Object>>) mvalue.get(dataId);
                    if (mvalue.get(dataId) == null) {
                        l = new ArrayList<>();
                    }
                    l.add(oneValue);
                    mvalue.put(dataId, l);
                }
            }
        }
    }

    private List<List<Object>> handleTimeField(List<List<Object>> flattenedValue) {
        int idx = 0;
        List<Integer> timeFieldIdxes = new ArrayList<Integer>(); //标记数据中时间解析失败的index

        for (List<Object> oneValue : flattenedValue) {
            try {
                changeDtEventTime(etl, oneValue);

                if (successMsgNum == 0) { // 现在只有遍历的时候加这个，如果数据没有遍历，+1
                    incrTotalMsgNum();
                    incrSuccessMsgNum();
                }

            } catch (TimeFormatError e) {
                this.appendBadValues(oneValue, e);
                successMsgNum--;
                timeFieldIdxes.add(idx);
            }
            idx++;
        }

        // 从列表中剔除时间解析失败的数据
        if (!timeFieldIdxes.isEmpty()) {
            List<List<Object>> tmpFlattenedValue = new ArrayList<List<Object>>();
            for (int i = 0; i < flattenedValue.size(); i++) {
                if (!timeFieldIdxes.contains(i)) {
                    tmpFlattenedValue.add(flattenedValue.get(i));
                }
            }

            flattenedValue = tmpFlattenedValue;
        }

        return flattenedValue;
    }

    /**
     * 将数据时间转换成timestamp.
     */
    private void changeDtEventTime(ETLImpl etl, List<Object> l) throws TimeFormatError {
        // 传入的timeFieldIdx为预设值时, 使用本地时间作为清洗默认时间
        if (etl.timeFieldIdx == EtlConsts.DEFAULT_TIME_FIELD_INDEX) {
            // 使用默认时间时,传入的时间格式为 Unix Time Stamp(milliseconds), 时间长度为13
            Long timestamp = etl.getTimeTransformer().transform(new Date().getTime());
            l.add(timestamp);
        } else {
            // 使用用户定义的时间
            Object data = l.get(etl.timeFieldIdx);
            try {
                Long timestamp = etl.getTimeTransformer().transform(data);
                // 不修改原始数据值，在清洗输出结果的尾部增加用户时间字段转换后的时间戳，然后再添加内部补充字段
                l.add(timestamp);
            } catch (TimeFormatError error) {
                // 记录时间解析失败的相关信息
                addErrorMessage(String.format("%s: %s -> (%s)!", TimeFormatError.class.getSimpleName(), data,
                        etl.getTimeTransformer().getFormat()));
                throw error;
            }
        }
    }

    /**
     * 设置忽略的字段.
     */
    public void setIgnoreFields(List<String> ignoreFields) {

        if (ignoreFields != null) {
            for (String fieldName : ignoreFields) {
                this.ignoreFields.put(fieldName, 1);
            }
        }
    }

    /**
     * 获取验证清洗配置的结果
     *
     * @return 清洗验证结果
     */
    public Map<String, Object> getVerifyResult() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(EtlConsts.NODES, etlParseDetails.getNodeDetails());
        result.put(EtlConsts.ERRORS, etlParseDetails.getErrors());
        result.put(EtlConsts.ERROR_MESSAGE, etlParseDetails.getErrorMessage());
        result.put(EtlConsts.OUTPUT_TYPE, etlParseDetails.getNodeOutputType());

        return result;
    }

    /**
     * 设置context为验证清洗配置模式
     */
    public void setContextToVerifyConf() {
        isVerifyConf = true;
    }

    public boolean getVerifyConf() {
        return isVerifyConf;
    }

    /**
     * 根据每个节点的label设置其ETL解析结果
     *
     * @param nodeLabel ETL中节点的label
     * @param result 节点ETL的解析结果
     */
    public void setNodeDetail(String nodeLabel, Object result) {
        if (isVerifyConf) {
            etlParseDetails.setNodeDetail(nodeLabel, result);
        }
    }

    /**
     * 设置节点的错误提示信息
     *
     * @param nodeLabel 节点标签
     * @param errorMsg 节点的错误消息
     */
    public void setNodeError(String nodeLabel, String errorMsg) {
        if (isVerifyConf) {
            etlParseDetails.setNodeErrorMsg(nodeLabel, errorMsg);
        }
    }

    /**
     * 设置时间解析失败的错误提示信息
     *
     * @param errMsg 错误提示信息
     */
    private void addErrorMessage(String errMsg) {
        if (isVerifyConf) {
            etlParseDetails.addErrorMessage(errMsg);
        }
    }

    /**
     * 设置节点的输出数据的类型
     *
     * @param nodeLable 节点标签
     * @param type 节点输出的数据的类型
     * @param value 节点输出的数据
     */
    public void setNodeOutput(String nodeLable, String type, Object value) {
        if (isVerifyConf) {
            etlParseDetails.setNodeOutput(nodeLable, type, value);
        }
    }
}
