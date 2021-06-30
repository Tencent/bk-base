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

public class SplitIndex implements IFunction, Serializable {

    /**
     * 根据分隔符切分数据
     * split_index('a.b.v', '.', 2) ==> b
     *
     * @param str 被分隔字符串
     * @param delim 分隔符
     * @param index 分隔index，从1开始计数
     * @return 分隔后的值
     */
    public String call(final String str, final String delim, int index) {
        if (str == null || delim == null) {
            return null;
        }
        String delimSplit = getDelimSplitCharacter(delim);
        String[] arr = str.split(delimSplit);
        if (str.endsWith(delim)) {
            arr = Arrays.copyOf(arr, arr.length + 1);
            arr[arr.length - 1] = "";
        }
        if (index == 0) {
            return "";
        }
        if (Math.abs(index) > arr.length) {
            return "";
        }
        if (index < 0) {
            index = index + arr.length + 1;
        }
        return arr[index - 1];
    }

    private String getDelimSplitCharacter(String delim) {
        String delimSplit = delim;
        if (delim.startsWith("+") || delim.startsWith("*") || delim.startsWith("|") || delim.startsWith("?")
                || delim.startsWith(".")) {
            delimSplit = "\\" + delim;
        }
        return delimSplit;
    }
}
