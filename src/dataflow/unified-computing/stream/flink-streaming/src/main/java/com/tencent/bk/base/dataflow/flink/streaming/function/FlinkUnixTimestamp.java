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


import com.tencent.bk.base.dataflow.core.function.UnixTimestamp;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.AbstractOneToOne;
import java.sql.Timestamp;

public class FlinkUnixTimestamp extends AbstractOneToOne<UnixTimestamp> {

    /**
     * 获取当前日期时间的时间戳(以秒计)
     * example: unix_timestamp(LOCALTIMESTAMP) as rt ==> 1466436000
     *
     * @param currentTimestamp
     * @return
     */
    public int eval(final Timestamp currentTimestamp) {
        return this.innerFunction.call(currentTimestamp);
    }

    /**
     * 获取指定日期时间(指定格式)的时间戳(以秒计)
     * example: unix_timestamp('2016/06/20/23/20/00','yyyy/MM/dd/HH/mm/ss') as rt ==> 1466436000
     *
     * @param dt
     * @param ft
     * @return
     */
    public Integer eval(final String dt, final String ft) {
        return this.innerFunction.call(dt, ft);
    }

    /**
     * 获取指定日期时间(默认格式: 'yyyy-MM-dd HH:mm:ss')的时间戳(以秒计)
     * example: unix_timestamp('2016-06-20 23:20:00') as rt ==> 1466436000
     *
     * @param dt
     * @return
     */
    public Integer eval(final String dt) {
        return this.innerFunction.call(dt);
    }

    /**
     * 获取通用转换类对象
     * 实现该方法时应指定实际的子类类型
     *
     * @return
     */
    @Override
    public UnixTimestamp getInnerFunction() {
        return new UnixTimestamp();
    }
}
