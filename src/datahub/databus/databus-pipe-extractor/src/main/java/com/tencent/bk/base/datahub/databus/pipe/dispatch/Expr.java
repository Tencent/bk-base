
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

package com.tencent.bk.base.datahub.databus.pipe.dispatch;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;

import java.util.List;

public class Expr {

    /**
     * 表达式类型.
     */
    private String type;

    /**
     * 数值（字面值).
     */
    private Object val;

    /**
     * 当表达式是字段的时候这个才有效.
     */
    private int fieldIdx;

    public Expr(ETL etl, String type, String val) {
        this.type = type;
        this.val = val;
    }

    public Expr(ETL etl, String type, Number val) {
        this.type = type;
        this.val = val;
    }

    /**
     * 表达式.
     */
    public Expr(ETL etl, String type, Field val, int idx) {
        this.type = EtlConsts.FIELD;
        this.val = val;
        this.fieldIdx = idx;
    }

    /**
     * 表达式相等.
     */
    public boolean equals(List<Object> oneValue, Expr obj) {

        if (obj.type.equals(EtlConsts.STRING)) {
            return this.equals(oneValue, (String) obj.val);

        } else if (obj.type.equals(EtlConsts.FIELD)) {
            Field field = (Field) obj.val;
            if (field.getType().equals(EtlConsts.STRING)) {
                return this.equals(oneValue, (String) oneValue.get(obj.fieldIdx));

            } else {
                return this.equals(oneValue, (Number) oneValue.get(obj.fieldIdx));
            }
        } else { //number
            return this.equals(oneValue, (Number) obj.val);
        }
    }

    /**
     * 表达式相等.
     */
    public boolean equals(List<Object> oneValue, Number obj) {
        Object object = oneValue.get(fieldIdx);
        if (object == null) {
            return false;
        }

        if (type.equals(EtlConsts.FIELD)) {
            return ((Number) object).equals(obj);
        }

        if (!(type.equals(EtlConsts.DOUBLE)
                || type.equals(EtlConsts.INT)
                || type.equals(EtlConsts.LONG))) {
            throw new RuntimeException("not number");
        }
        return obj.equals(val);
    }

    /**
     * 表达式相等.
     */
    public boolean equals(List<Object> oneValue, String obj) {
        Object object = oneValue.get(fieldIdx);
        if (object == null) {
            return false;
        }

        if (type.equals(EtlConsts.FIELD)) {
            return ((String) object).equals(obj);
        }

        if (!type.equals(EtlConsts.STRING)) {
            throw new RuntimeException("not string");
        }

        return ((String) val).equals(obj);
    }

    public boolean notEquals(List<Object> oneValue, Expr obj) {
        return !equals(oneValue, obj);
    }

    public boolean execute(ETL etl, List<Object> oneValue) {
        return false;
    }

    /**
     * return the value.
     *
     * @return the val
     */
    public Object getVal() {
        return val;
    }
}
