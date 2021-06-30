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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegExpExtract implements IFunction, Serializable {

    /**
     * 将字符串subject按照pattern正则表达式的规则拆分，返回index指定的字符.
     * example: udf_regexp_extract('100-200', '(\\d+)-(\\d+)', 1) will return '100'
     *
     * @param s 要处理的字段.
     * @param regex 需要匹配的正则表达式.
     * @param extractIndex 0是显示与之匹配的整个字符串, 1 是显示第一个括号里面的, 2 是显示第二个括号里面的字段...
     * @return string.
     */
    public String call(final String s, final String regex, final Integer extractIndex) {
        if (s == null || regex == null) {
            return null;
        }

        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(s);
        if (m.find()) {
            MatchResult mr = m.toMatchResult();
            return mr.group(extractIndex);
        }
        return "";
    }
}
