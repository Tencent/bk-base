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

import com.tencent.bk.base.dataflow.core.function.SplitFieldToRecords;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToMany;
import org.apache.flink.api.java.tuple.Tuple1;

public class FlinkSplitFieldToRecords extends AbstractOneToMany<SplitFieldToRecords, Tuple1<String>> {

    /**
     * 将特定字段的字符串值分割为多个值，并添加到新行中
     * example: split_field_to_records('1_2','_') as rt ==> ['1', '2']
     *
     * @param sourceStr
     * @param delim
     * @return
     */
    public void eval(final String sourceStr, final String delim) {
        String[] valuesArray = this.innerFunction.call(sourceStr, delim);
        Tuple1<String> result = new Tuple1<>();
        for (String value : valuesArray) {
            result.f0 = value;
            collect(result);
        }
    }

    /**
     * 获取通用转换类对象
     * 实现该方法时应指定实际的子类类型
     *
     * @return
     */
    @Override
    public SplitFieldToRecords getInnerFunction() {
        return new SplitFieldToRecords();
    }
}
