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

package com.tencent.bk.base.dataflow.flink.streaming.function;

import com.tencent.bk.base.dataflow.core.function.Division;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToOne;

public class FlinkDivision extends AbstractOneToOne<Division> {

    // ------------------divisor is integer---------------------------------
    public Double eval(final Long dividend, final Integer divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Integer dividend, final Integer divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Double dividend, final Integer divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Float dividend, final Integer divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    // ------------------divisor is long---------------------------------
    public Double eval(final Long dividend, final Long divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Integer dividend, final Long divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Double dividend, final Long divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Float dividend, final Long divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    // ------------------divisor is float---------------------------------
    public Double eval(final Long dividend, final Float divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Integer dividend, final Float divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Double dividend, final Float divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Float dividend, final Float divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    // ------------------divisor is double---------------------------------
    public Double eval(final Long dividend, final Double divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Integer dividend, final Double divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Double dividend, final Double divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    public Double eval(final Float dividend, final Double divisor) {
        return this.innerFunction.call(dividend, divisor);
    }

    @Override
    public Division getInnerFunction() {
        return new Division();
    }
}
