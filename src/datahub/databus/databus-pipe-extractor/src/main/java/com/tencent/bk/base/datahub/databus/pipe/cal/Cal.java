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

public class Cal {

    private String assignType;

    // 计算结果的变量名
    private String varName;
    private Fun fun;
    private Expr expr;
    private Field field;
    private String type;

    // 标注是否使用extrac出来的字段（在cal配置中重名）, 如果是，appendPlaceHolder跳过
    private Boolean useOrigVar = false;

    /**
     * 计算节点.
     */
    @SuppressWarnings("unchecked")
    public Cal(Context ctx, Map<String, Object> config) {

        this.assignType = (String) config.get(EtlConsts.ASSIGN_TYPE);
        this.varName = (String) config.get(EtlConsts.ASSIGN_VAR);

        Map<String, Object> exprConfig = (Map<String, Object>) config.get(EtlConsts.EXPR);
        if (exprConfig == null) {
            exprConfig = (Map<String, Object>) config.get(EtlConsts.FUN_CALL); // 兼容老的json
        }

        type = (String) exprConfig.get(EtlConsts.TYPE);

        if (type.equals(EtlConsts.FUN)) {
            this.fun = Fun.genNode(ctx, exprConfig);
        } else {
            this.expr = Expr.genNode(ctx, exprConfig);
        }

        this.field = ctx.getSchema().searchFieldByName(varName);

        if (this.field == null) {
            this.field = new Field(varName, assignType);
            ctx.getSchema().append(this.field);
        } else {
            useOrigVar = true;
        }
    }

    /**
     * 为计算出的字段填充value的占位符.
     */
    public void appendPlaceHolder(Context ctx, Object o) {
        if (!useOrigVar) {
            ctx.setValue(field, null);
        }
    }

    /**
     * 执行计算.
     */
    public List<List<Object>> execute(List<List<Object>> flattenedValues, Fields flattenedSchema) {
        if (type.equals(EtlConsts.FUN)) {
            return fun.execute(varName, flattenedValues, flattenedSchema);
        } else {
            return Expr.run(expr, varName, flattenedValues, flattenedSchema);
        }
    }
}
