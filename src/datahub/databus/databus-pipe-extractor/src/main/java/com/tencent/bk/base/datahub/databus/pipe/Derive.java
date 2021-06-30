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
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Derive {

    String deriveDimensionFieldName;

    String deriveValueFieldName;

    int deriveDimensionFieldIdx;

    int deriveValueFieldIdx;

    int dimensionNum = 0;

    Map<String, Integer> dimension2idx;

    Fields etlFlattenedSchema;

    /**
     * 维度字段衍生.
     */
    public Derive(Context ctx, Map<String, Object> conf) {

        dimension2idx = new HashMap<String, Integer>();

        Map<String, String> deriveConf = (Map<String, String>) conf.get(EtlConsts.DERIVE);
        Map<String, Object> simpleConf = (Map<String, Object>) conf.get(EtlConsts.CONF);

        Fields fields = ctx.getSchema();
        String type = null;

        for (String fieldName : deriveConf.keySet()) {
            Field fromField = fields.searchFieldByName(fieldName);
            if (fromField != null) {
                if (type != null && !type.equals(fromField.getType())) {
                    throw new RuntimeException("Derive: type of all demension field are not same "
                            + type + " vs " + fromField.getType());
                }
                type = fromField.getType();
            }
        }

        if (type == null) {
            throw new RuntimeException("Derive: missing type");
        }

        deriveDimensionFieldName = (String) simpleConf.get(EtlConsts.DERIVE_DIMENSION_FIELD_NAME);
        deriveValueFieldName = (String) simpleConf.get(EtlConsts.DERIVE_VALUE_FIELD_NAME);

        Field diemensionField = new Field(deriveDimensionFieldName, EtlConsts.STRING);
        Field valueField = new Field(deriveValueFieldName, type);

        ctx.getSchema().append(diemensionField);
        ctx.getSchema().append(valueField);

        this.etlFlattenedSchema = new Fields();
        for (Field field : ctx.flattenSchema(ctx.getSchema())) {
            this.etlFlattenedSchema.append(field);
        }

        for (Map.Entry<String,String> entry : deriveConf.entrySet()) {
            dimension2idx.put(entry.getValue(), etlFlattenedSchema.fieldIndex(entry.getKey()));
        }

        deriveDimensionFieldIdx = etlFlattenedSchema.fieldIndex(deriveDimensionFieldName);
        deriveValueFieldIdx = etlFlattenedSchema.fieldIndex(deriveValueFieldName);

        dimensionNum = deriveConf.keySet().size();
    }

    /**
     * 执行维度扩种节点树.
     */
    public List<List<Object>> execute(List<Object> flattenOneValue, Context ctx) {
        if (this.etlFlattenedSchema == null) {
            throw new RuntimeException("Derive missing schema");
        }

        List<List<Object>> ret = new ArrayList<List<Object>>();

        for (Map.Entry<String, Integer> entry : dimension2idx.entrySet()) {
            List<Object> collector = new ArrayList<Object>();
            collector.addAll(flattenOneValue);
            collector.set(deriveDimensionFieldIdx, entry.getKey());
            collector.set(deriveValueFieldIdx, flattenOneValue.get(entry.getValue()));
            ret.add(collector);
        }

        ctx.incrSuccessMsgNum(dimensionNum - 1);
        ctx.incrTotalMsgNum(dimensionNum - 1);

        return ret;
    }

    /**
     * 为扩展字段增加占位符.
     */
    public void appendPlaceHolder(Context ctx) {
        ctx.setValue(ctx.getSchema().searchFieldByName(deriveDimensionFieldName), null);
        ctx.setValue(ctx.getSchema().searchFieldByName(deriveValueFieldName), null);
    }

}
