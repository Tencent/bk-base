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

import java.util.Map;

public class Dispatch {

    /**
     * 构建分发逻辑的节点树.
     */
    public static Expr buildExpr(ETL etl, Map<String, Object> config) {

        String op = (String) config.get(EtlConsts.OP);

        if (op.equals(EtlConsts.EQUAL)) {
            return new Equal(etl, config);

        } else if (op.equals(EtlConsts.NOT_EQUAL)) {
            return new NotEqual(etl, config);

        } else if (op.equals(EtlConsts.AND)) {
            return new And(etl, config);

        } else if (op.equals(EtlConsts.OR)) {
            return new Or(etl, config);

        } else if (op.equals(EtlConsts.VALUE)) { //值节点
            String type = (String) config.get(EtlConsts.TYPE);

            if (type.equals(EtlConsts.FIELD)) {
                int idx = etl.getSchema().fieldIndex((String) config.get(EtlConsts.NAME));
                return new Expr(etl, type, (Field) etl.getSchema().get(idx), idx);

            } else if (type.equals(EtlConsts.STRING)) {
                return new Expr(etl, type, (String) config.get(EtlConsts.VAL));

            } else if (type.equals(EtlConsts.DOUBLE)
                    || type.equals(EtlConsts.INT)
                    || type.equals(EtlConsts.LONG)) {
                return new Expr(etl, type, (Number) config.get(EtlConsts.VAL));
            }
        }
        return null;
    }

}
