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
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;

import java.util.HashMap;
import java.util.Map;

public class AssignValue extends Node {

    private Field field;

    /**
     * 位置赋值.
     */
    public AssignValue(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        generateNodeLabel();

        // 获取清洗配置中的 assign_to 和 type 属性
        Map<String, Object> assignConf = (Map<String, Object>) nodeConf.get(EtlConsts.ASSIGN);
        type = assignConf.containsKey(EtlConsts.TYPE) ? ((String) assignConf.get(EtlConsts.TYPE)) : EtlConsts.STRING;
        String assignTo = assignConf.get(EtlConsts.ASSIGN_TO).toString();
        Boolean isDimension = assignConf.containsKey(EtlConsts.IS_DIMENSION) ? true : false;
        field = new Field(assignTo, type, isDimension);
        ctx.getSchema().append(field);
    }

    @Override
    public boolean validateNext() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object execute(Context ctx, Object o) {
        Map<String, Object> nodeResult = new HashMap<>();
        // 按照目标对象类型对数据进行转换,如果转换失败,则默认使用null值填充
        Object value = null;
        try {
            value = field.castType(o);
        } catch (TypeConversionError e) {
            ctx.appendBadValues(o, e);
            ctx.setNodeError(label, String.format("%s: %s(%s) = %s", TypeConversionError.class.getSimpleName(),
                    field.getName(), field.getType(), o.toString()));
        }
        ctx.setValue(field, value);
        nodeResult.put(field.getName(), value);

        ctx.setNodeDetail(label, nodeResult);

        return null;
    }
}
