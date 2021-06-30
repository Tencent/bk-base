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

import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

public class SimpleRexVisitorImpl<R> extends RexVisitorImpl<R> {

    protected SimpleRexVisitorImpl() {
        super(true);
    }

    @Override
    public final R visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException("local ref not supported");
    }

    @Override
    public final R visitOver(RexOver over) {
        throw new UnsupportedOperationException("over not supported");
    }

    @Override
    public final R visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException("correl variable not supported");
    }

    @Override
    public final R visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException("dynamic param not supported");
    }

    @Override
    public final R visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException("range ref not supported");
    }

    @Override
    public final R visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException("field access not supported");
    }

    @Override
    public final R visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException("subquery not supported");
    }

    @Override
    public final R visitTableInputRef(RexTableInputRef ref) {
        throw new UnsupportedOperationException("table input not supported");
    }

    @Override
    public final R visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException("pattern field not supported");
    }
}
