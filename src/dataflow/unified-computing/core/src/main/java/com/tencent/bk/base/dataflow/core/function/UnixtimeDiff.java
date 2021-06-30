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

package com.tencent.bk.base.dataflow.core.function;


import static com.tencent.bk.base.dataflow.core.function.base.udf.AbstractFunctionFactory.TRANS_TO_MILLISEC;

import com.tencent.bk.base.dataflow.core.function.base.IFunction;
import java.io.Serializable;

public class UnixtimeDiff implements IFunction, Serializable {

    /**
     * 计算两个时间戳(以秒计)的时间差
     * example: "unixtime_diff(1466436000,1466434200,'minute') as rt ==> 30
     *
     * @param ut1
     * @param ut2
     * @param diffIn
     * @return
     */
    public Integer call(final long ut1, final long ut2, final String diffIn) {
        if (!TRANS_TO_MILLISEC.containsKey(diffIn)) {
            throw new RuntimeException("参数diffIn取值有误");
        }
        return (int) ((ut1 * 1000 - ut2 * 1000) / TRANS_TO_MILLISEC.get(diffIn));
    }
}
