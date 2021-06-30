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
import java.io.Serializable;
import java.util.Arrays;

public class SubstringIndex implements IFunction, Serializable {

    /**
     * 根据分隔符计数截取字符串
     * example:
     * input - substring_index('3+288+65353+1688+0+0','+',2)
     * output - 3+288
     * input - substring_index('3+288+65353+1688+0+0','+',20)
     * output - 3+288+65353+1688+0+0
     *
     * @param str 字符串
     * @param delim 分隔符
     * @param index 分隔计数,为正则从左到右,为负则相反,为0则返回null,如果count数超过实际分割数，则返回原字符串
     * @return 结果字符串
     */
    public String call(final String str, final String delim, final int index) {
        String delimSplit = delim;
        if (delim.startsWith("+") || delim.startsWith("*") || delim.startsWith("|") || delim.startsWith("?")
                || delim.startsWith(".")) {
            delimSplit = "\\" + delim;
        }
        String[] temp = str.split(delimSplit);
        String rt = null;
        if (str.endsWith(delim)) {
            temp = Arrays.copyOf(temp, temp.length + 1);
            temp[temp.length - 1] = "";
        }
        if (Math.abs(index) > temp.length) {
            return str;
        }
        if (index > 0) {
            for (int i = 0; i <= index - 1; i++) {
                if (rt == null) {
                    rt = temp[i];
                } else {
                    rt = rt + delim + temp[i];
                }
            }
        } else {
            for (int i = temp.length - 1; i > temp.length - 1 + index; i--) {
                if (rt == null) {
                    rt = temp[i];
                } else {
                    rt = temp[i] + delim + rt;
                }

            }
        }
        return rt;
    }
}
