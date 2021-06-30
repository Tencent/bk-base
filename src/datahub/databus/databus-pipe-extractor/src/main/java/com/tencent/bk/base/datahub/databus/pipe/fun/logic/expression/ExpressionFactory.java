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

import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;

import java.util.Map;

public class ExpressionFactory {

    /**
     * 生成表达式
     */
    @SuppressWarnings("unchecked")
    public static Expression createExpression(Map<String, Object> conf) {
        String op = (String) conf.get(EtlConsts.OP);
        switch (op) {
            case EtlConsts.AND:
                return new AndExpr(conf);
            case EtlConsts.OR:
                return new OrExpr(conf);
            case EtlConsts.NOT:
                return new NotExpr(conf);
            case EtlConsts.LOGIC_EQUAL:
                return new EqualsExpr(conf);
            case EtlConsts.LOGIC_NOT_EQUAL:
                return new NotEqualsExpr(conf);
            case EtlConsts.LOGIC_CONTAINS:
                return new ContainsExpr(conf);
            case EtlConsts.LOGIC_STARTSWITH:
                return new StartsWithExpr(conf);
            case EtlConsts.LOGIC_ENDSWITH:
                return new EndsWithExpr(conf);
            case EtlConsts.LOGIC_GREATER:
                return new GreaterExpr(conf);
            case EtlConsts.LOGIC_NOT_GREATER:
                return new NotGreaterExpr(conf);
            case EtlConsts.LOGIC_LESS:
                return new LessExpr(conf);
            case EtlConsts.LOGIC_NOT_LESS:
                return new NotLessExpr(conf);
            default:
                return null;
        }
    }
}
