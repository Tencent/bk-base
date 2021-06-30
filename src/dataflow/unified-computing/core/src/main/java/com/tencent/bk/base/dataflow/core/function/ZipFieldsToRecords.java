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

import com.tencent.bk.base.dataflow.core.function.base.IFunction;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.TransformerException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ZipFieldsToRecords implements IFunction, Serializable {

    /**
     * 将多个特定字段根据特定分隔符分割后合并，再分多行输出
     * example: zip('*','11_21','_','12_22','_','13_23','_') as rt ==> 11*12*13
     * ==> 21*22*23
     *
     * @param concatation
     * @param fieldAndDelim
     * @return
     */
    public String[] call(final String concatation, final String... fieldAndDelim)
            throws TransformerException {
        try {
            if (fieldAndDelim == null || fieldAndDelim.length == 0 || fieldAndDelim.length % 2 != 0) {
                throw new TransformerException("待转换参数个数非偶数.");
            }
            // 求切分字段切分后的最大个数
            int splitLength = 0;
            List<String[]> splitFields = new ArrayList<String[]>();
            for (int i = 0; i < fieldAndDelim.length; i = i + 2) {
                String[] valueArray = fieldAndDelim[i].split(fieldAndDelim[i + 1], -1);
                if (splitLength < valueArray.length) {
                    splitLength = valueArray.length;
                }
                splitFields.add(valueArray);
            }
            List<String> concatFields = new ArrayList<String>();
            for (int i = 0; i < splitLength; i++) {
                // 拼接为新的数组
                StringBuilder concatField = new StringBuilder();
                // m用于末尾是否拼接字符
                int m = 0;
                for (String[] splitField : splitFields) {
                    if (m != splitFields.size() - 1) {
                        concatField.append(getValue(splitField, i)).append(concatation);
                    } else {
                        concatField.append(getValue(splitField, i));
                    }
                    m++;
                }
                concatFields.add(i, concatField.toString());
            }
            return concatFields.toArray(new String[concatFields.size()]);
        } catch (TransformerException e1) {
            // 不抛出异常，异常会终止整个job任务
            // 异常时返回空
            return new String[]{null};
        }
    }

    /**
     * 根据index获取数组具体的值，如果越界，则返回空字符串
     *
     * @param splitField 要获取的数组
     * @param index 获取数组的index
     * @return 返回数组值或者空字符串
     */
    private String getValue(String[] splitField, int index) {
        if (index < splitField.length) {
            return splitField[index];
        } else {
            return "";
        }
    }
}
