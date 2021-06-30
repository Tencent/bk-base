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

public class Substring implements IFunction, Serializable {

    /**
     * 按指定起止位置(由1开始)截取字符串的子串
     * example: substring('11122', 3) as rt ==> '122'
     *
     * @param str 被截取字符串
     * @param beginIndex 起始位置
     * @return 截取后的字符串
     */
    public String call(final String str, final int beginIndex) {
        return bkdataSubstring(str, beginIndex);
    }

    /**
     * 按指定起止位置(由1开始)截取字符串的子串
     *
     * @param str 被截取字符串
     * @param beginIndex 起始位置
     * @param length 截取长度
     * @return 截取后的字符串
     */
    public String call(final String str, final int beginIndex, final int length) {
        return bkdataSubstring(str, beginIndex, length);
    }

    /**
     * 按指定起止位置(由1开始)截取字符串的子串
     * example: substring('11122', 30, 'uc-udf') as rt ==> 'uc-udf'
     *
     * @param str 被截取字符串
     * @param beginIndex 起始位置
     * @param defaultValue 设置返回默认值
     * @return 截取后的字符串
     */
    public String call(final String str, final int beginIndex, final String defaultValue) {
        if (Math.abs(beginIndex) > str.length()) {
            return defaultValue;
        }
        return bkdataSubstring(str, beginIndex);
    }

    /**
     * 按指定起止位置(由1开始)截取字符串的子串
     *
     * @param str 被截取字符串
     * @param beginIndex 起始位置
     * @param length 截取长度
     * @param defaultValue 设置返回默认值
     * @return 截取后的字符串
     */
    public String call(final String str, final int beginIndex, final int length, final String defaultValue) {
        if (Math.abs(beginIndex) > str.length()) {
            return defaultValue;
        }
        return bkdataSubstring(str, beginIndex, length);
    }

    private static String bkdataSubstring(String str, int index) {
        if (index == 0) {
            return "";
        }
        if (Math.abs(index) > str.length()) {
            return "";
        }
        if (index < 0) {
            return str.substring(str.length() + index);
        }
        return str.substring(index - 1);
    }

    private static String bkdataSubstring(String str, int index, int length) {
        if (index == 0) {
            return "";
        }
        if (Math.abs(index) > str.length()) {
            return "";
        }
        if (index < 0) {
            index = str.length() + index + 1;
        }

        if (length > (str.length() - (index - 1))) {
            length = str.length() - (index - 1);
        }

        return str.substring(index - 1, index - 1 + length);
    }
}
