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
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import java.util.List;
import java.util.Map;


public abstract class Fun {

    public abstract List<List<Object>> execute(String varName,
            List<List<Object>> flattenedValue,
            Fields flattenedSchema);

    /**
     * 函数节点生成工场.
     */
    @SuppressWarnings("unchecked")
    public static Fun genNode(Context ctx, Map<String, Object> config) {

        String method = (String) config.get(EtlConsts.METHOD);

        if (method.equals(EtlConsts.MINUS)) {
            return new Minus(ctx, config);
        }
        if (method.equals(EtlConsts.PERCENT)) {
            return new Percent(ctx, config);
        }
        if (method.equals(EtlConsts.MAX)) {
            return new Max(ctx, config);
        }

        return null;
    }

    /**
     * 数值类型转换.
     */
    public static Number castType(String type, Number o) {
        if (type.equals(EtlConsts.INT)) {
            return o.intValue();
        } else if (type.equals(EtlConsts.LONG)) {
            return o.longValue();
        } else if (type.equals(EtlConsts.DOUBLE)) {
            return o.doubleValue();
        }
        return null;
    }
}
