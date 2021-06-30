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


import com.tencent.bk.base.dataflow.core.function.FromUnixTime;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToOne;

public class FlinkFromUnixTime extends AbstractOneToOne<FromUnixTime> {


    /**
     * 将时间戳(以秒计)转换为日期时间(指定格式)字符串
     * example: from_unixtime(1466436000,'yyyy/MM/dd/HH/mm/ss') as rt ==> '2016/06/20/23/20/00'
     *
     * @param seconds
     * @param ft
     * @return
     */
    public String eval(final long seconds, final String ft) {
        return this.innerFunction.call(seconds, ft);
    }

    /**
     * 将时间戳(以秒计)转换为日期时间(默认格式: 'yyyy-MM-dd HH:mm:ss')字符串
     * example: from_unixtime(1466436000) as rt ==> '2016-06-20 23:20:00'
     *
     * @param seconds
     * @return
     */
    public String eval(final long seconds) {
        return this.innerFunction.call(seconds);
    }

    /**
     * 获取通用转换类对象
     * 实现该方法时应指定实际的子类类型
     *
     * @return
     */
    @Override
    public FromUnixTime getInnerFunction() {
        return new FromUnixTime();
    }
}
