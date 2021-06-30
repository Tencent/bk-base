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

package com.tencent.bk.base.dataflow.bksql.mlsql.rel;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;

public class MLSqlRexCallImplTable {

    private final Map<SqlOperator, CallImplementor> implMap = new HashMap<>();

    public MLSqlRexCallImplTable() {
    }

    public void registerImpl(SqlOperator op, CallImplementor impl) {
        if (implMap.containsKey(op)) {
            throw new IllegalArgumentException("operator already registered: " + op);
        }
        implMap.put(op, impl);
    }

    public String implement(RexCall call) {
        SqlOperator op = call.getOperator();
        if (!implMap.containsKey(op)) {
            throw new UnsupportedOperationException("unsupported operator: " + op);
        }
        CallImplementor implementor = implMap.get(op);
        return implementor.implement(call);
    }

    @FunctionalInterface
    public interface CallImplementor {

        String implement(RexCall call);
    }
}
