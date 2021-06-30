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

public class Locate implements IFunction, Serializable {

    /**
     * 返回子字符串在指定字符串指定位置之后第一次出现的位置(从1开始)
     * example: locate('bc','abcbcd123', 3) as rt ==> 2
     *
     * @param substr 原字符串
     * @param str 指定字符串
     * @param pos 位置 从1开始
     * @return 返回子字符串
     */
    public Integer call(final String substr, final String str, final int pos) {
        return str.indexOf(substr, pos - 1) + 1;
    }

    /**
     * 返回子字符串在指定字符串第一次出现的位置(从1开始)
     * example: locate('bc','abcbcd123') as rt ==> 1
     *
     * @param substr 原字符串
     * @param str 指定字符串
     * @return 返回子字符串
     */
    public Integer call(final String substr, final String str) {
        return call(substr, str, 1);
    }
}
