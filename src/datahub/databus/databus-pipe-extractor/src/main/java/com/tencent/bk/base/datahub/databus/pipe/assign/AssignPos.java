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

package com.tencent.bk.base.datahub.databus.pipe.assign;

import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.exception.NotListDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssignPos extends Node {

    private Map<Integer, Field> idx2varname;

    /**
     * 位置赋值.
     */
    public AssignPos(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        generateNodeLabel();

        idx2varname = new HashMap<>();
        List<Map<String, Object>> assignConf = (List<Map<String, Object>>) nodeConf.get(EtlConsts.ASSIGN);

        for (Map<String, Object> kv : assignConf) {
            String varName = (String) kv.get(EtlConsts.ASSIGN_TO);
            String type = kv.containsKey(EtlConsts.TYPE) ? ((String) kv.get(EtlConsts.TYPE)) : EtlConsts.STRING;
            Boolean isDimension = kv.containsKey(EtlConsts.IS_DIMENSION) ? true : false;
            Field field = new Field(varName, type, isDimension);

            int index;
            if (kv.get(EtlConsts.INDEX) instanceof String) {
                index = Integer.parseInt((String) kv.get(EtlConsts.INDEX));
            } else {
                index = (Integer) kv.get(EtlConsts.INDEX);
            }

            idx2varname.put(index, field);

            ctx.getSchema().append(field);
        }
    }

    @Override
    public boolean validateNext() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object execute(Context ctx, Object o) {
        Map<String, Object> nodeResult = new HashMap<>();

        if (o instanceof Map) {
            ctx.setNodeDetail(label, null);
            String errMsg = String
                    .format("%s: %s is not List, can't assign by position %s", NotListDataError.class.getSimpleName(),
                            o.getClass().getSimpleName(), idx2varname.keySet());
            ctx.setNodeError(label, String.format("%s", NotListDataError.class.getSimpleName()));
            throw new NotListDataError(errMsg);
        } else if (o instanceof List) {
            List<String> lst = (List<String>) o;

            for (Map.Entry<Integer, Field> entry : idx2varname.entrySet()) {
                Field field = entry.getValue();

                // 按照目标对象类型对数据进行转换,如果转换失败,则默认使用null值填充
                Object value = null;
                try {
                    value = field.castType(lst.get(entry.getKey()));
                } catch (TypeConversionError e) {
                    ctx.appendBadValues(o, e);
                    ctx.setNodeError(label, String.format("%s: %s(%s) = %s", TypeConversionError.class.getSimpleName(),
                            field.getName(), field.getType(), lst.get(entry.getKey())));
                } catch (IndexOutOfBoundsException e) {
                    // ignore
                }
                ctx.setValue(field, value);
                nodeResult.put(field.getName(), value);
            }

        } else {
            for (Map.Entry<Integer, Field> entry : idx2varname.entrySet()) { // 单值赋值，这里应该只有1个元素
                Field field = entry.getValue();
                // 按照目标对象类型对数据进行转换,如果转换失败,则默认使用null值填充
                Object value = null;
                try {
                    value = field.castType(o);
                } catch (TypeConversionError e) {
                    ctx.appendBadValues(o, e);
                    ctx.setNodeError(label, String.format("%s: %s -> %s", TypeConversionError.class.getSimpleName(),
                            o.getClass().getSimpleName(), field.getType()));
                }
                ctx.setValue(field, value);
                nodeResult.put(field.getName(), value);
            }
        }

        ctx.setNodeDetail(label, nodeResult);

        return null;
    }
}

