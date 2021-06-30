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

import java.lang.IllegalArgumentException;
import java.util.List;
import java.util.Map;


public abstract class Expr {

    protected Expr lval;
    protected Expr rval;

    protected void init(Context ctx, Map<String, Object> config) {
        lval = Expr.genNode(ctx, (Map<String, Object>) config.get(EtlConsts.LVAL));
        rval = Expr.genNode(ctx, (Map<String, Object>) config.get(EtlConsts.RVAL));
    }

    public abstract Object execute(String varName, List<Object> oneValue,
            Fields flattenedSchema);

    /**
     * 函数节点生成工场.
     */
    @SuppressWarnings("unchecked")
    public static Expr genNode(Context ctx, Map<String, Object> config) {
        String type = (String) config.get(EtlConsts.TYPE);
        if (type != null && type.equals(EtlConsts.FUN)) { // 部分在cal中支持的函数
            String method = (String) config.get(EtlConsts.METHOD);
            switch (method) {
                case EtlConsts.MAP:
                    return new CacheMap(ctx, config);
                default:
                    throw new IllegalArgumentException("Unsupport cal function");
            }

        } else {
            String op = (String) config.get(EtlConsts.OP);
            switch (op) {
                case "+":
                    return new ExprAdd(ctx, config);
                case "-":
                    return new ExprSub(ctx, config);
                case "*":
                    return new ExprMul(ctx, config);
                case "/":
                    return new ExprDiv(ctx, config);
                case EtlConsts.VALUE:
                    return new ExprVal(ctx, config);
                default:
                    throw new IllegalArgumentException("Unsupported cal op");
            }

        }


    }

    /**
     * 执行四则运算.
     */
    public static List<List<Object>> run(Expr e, String varName,
            List<List<Object>> flattenedValues,
            Fields flattenedSchema) {

        int varIdx = flattenedSchema.fieldIndex(varName);
        Field field = (Field) flattenedSchema.get(varIdx);

        for (List<Object> value : flattenedValues) {

            value.set(varIdx,
                    Fun.castType(field.getType(),
                            (Number) (e.execute(varName, value,
                                    flattenedSchema))));
        }
        return flattenedValues;
    }

}
