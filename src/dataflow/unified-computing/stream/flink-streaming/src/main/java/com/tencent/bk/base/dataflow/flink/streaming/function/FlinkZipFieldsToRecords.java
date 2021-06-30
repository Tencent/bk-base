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

import com.tencent.bk.base.dataflow.core.function.ZipFieldsToRecords;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.TransformerException;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToMany;
import org.apache.flink.api.java.tuple.Tuple1;

public class FlinkZipFieldsToRecords extends AbstractOneToMany<ZipFieldsToRecords, Tuple1<String>> {

    /**
     * 将多个特定字段根据特定分隔符分割后合并，再分多行输出
     * eval参数需至少一个字段名称，而不能全部是值，因此不支持无参
     * field = '*'
     * example: zip('*','11_21','_','12_22','_','13_23','_') as rt ==> 11*12*13
     * ==> 21*22*23
     *
     * @param concatation
     * @param fieldAndDelim
     * @return
     */
    public void eval(final String concatation, final String... fieldAndDelim)
            throws TransformerException {
        String[] valuesArray = this.innerFunction.call(concatation, fieldAndDelim);
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
    public ZipFieldsToRecords getInnerFunction() {
        return new ZipFieldsToRecords();
    }
}
