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
import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractFunctionFactory;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang3.time.FastDateFormat;

public class FromMillisec implements IFunction, Serializable {


    /**
     * 将时间戳(以毫秒计)转换为日期时间(指定格式)字符串
     * example: from_millisec(1466436000000,'yyyy/MM/dd/HH/mm/ss') as rt ==> '2016/06/20/23/20/00'
     *
     * @param millisec
     * @param ft
     * @return
     */
    public String call(final long millisec, final String ft) {
        FastDateFormat sdf = FastDateFormat.getInstance(ft);
        return sdf.format(new Date(millisec));
    }

    /**
     * 将时间戳(以毫秒计)转换为日期时间(默认格式: 'yyyy-MM-dd HH:mm:ss')字符串
     * example: from_millisec(1466436000000) as rt ==> '2016-06-20 23:20:00'
     *
     * @param millisec
     * @return
     */
    public String call(final long millisec) {
        return call(millisec, AbstractFunctionFactory.DEFAULT_DATETIME_FORMAT);
    }
}
