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

package com.tencent.bk.base.datahub.databus.pipe.fun.logic.expression;

import com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable.Variable;
import com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable.VariableFactory;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;

import java.util.Map;

public class BasicExpr implements Expression {

    protected Variable lvar;
    protected Variable rvar;
    protected boolean noError = true;

    @SuppressWarnings("unchecked")
    public BasicExpr(Map<String, Object> conf) {
        lvar = VariableFactory.createVariable((String) conf.get(EtlConsts.OP), conf.get(EtlConsts.LVAL));
        rvar = VariableFactory.createVariable((String) conf.get(EtlConsts.OP), conf.get(EtlConsts.RVAL));

        if (lvar == null || rvar == null) {
            noError = false;
        }
    }

    @Override
    public boolean execute(Object o) {
        if (noError) {
            lvar.initVal(o);
            rvar.initVal(o);
        }
        return false;
    }
}
