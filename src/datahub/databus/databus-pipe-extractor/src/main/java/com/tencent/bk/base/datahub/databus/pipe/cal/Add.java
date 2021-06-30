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

package com.tencent.bk.base.datahub.databus.pipe.cal;

import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import java.util.List;
import java.util.Map;

public class Add extends Fun {

    String lval;
    String rval;

    /**
     * 加法.
     */
    public Add(Context ctx, Map<String, Object> config) {
        lval = (String) ((List) config.get(EtlConsts.ARGS)).get(0);
        rval = (String) ((List) config.get(EtlConsts.ARGS)).get(1);

    }

    @Override
    public List<List<Object>> execute(String varName,
            List<List<Object>> flattenedValue,
            Fields flattenedSchema) {
        int lvalIdx = flattenedSchema.fieldIndex(this.lval);
        int varIdx = flattenedSchema.fieldIndex(varName);
        int rvalIdx = flattenedSchema.fieldIndex(this.rval);
        Field field = (Field) flattenedSchema.get(varIdx);

        for (List<Object> value : flattenedValue) {
            Number lval = (Number) value.get(lvalIdx);
            Number rval = (Number) value.get(rvalIdx);

            value.set(varIdx, castType(field.getType(),
                    (Number) (lval.doubleValue()
                            + rval.doubleValue())));
        }
        return flattenedValue;
    }

}
