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

import com.tencent.bk.base.dataflow.core.function.RegexpReplace;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToOne;

public class FlinkRegexpReplace extends AbstractOneToOne<RegexpReplace> {

    /**
     * 使用指定子串替换字符串中所有匹配正则表达的子串
     * example: regexp_replace('abcbcd123abcbcd123','cd\\\\d+','www') as rt ==> 'abcbwwwabcbwww'
     *
     * @param sourceStr
     * @param regex
     * @param newSubStr
     * @return
     */
    public String eval(final String sourceStr, final String regex, final String newSubStr) {
        return this.innerFunction.call(sourceStr, regex, newSubStr);
    }

    /**
     * 获取通用转换类对象
     * 实现该方法时应指定实际的子类类型
     *
     * @return
     */
    @Override
    public RegexpReplace getInnerFunction() {
        return new RegexpReplace();
    }
}
