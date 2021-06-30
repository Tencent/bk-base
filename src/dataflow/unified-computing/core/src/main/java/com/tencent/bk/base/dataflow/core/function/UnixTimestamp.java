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
import java.sql.Timestamp;
import java.text.ParseException;
import org.apache.commons.lang3.time.FastDateFormat;

public class UnixTimestamp implements IFunction, Serializable {

    /**
     * 获取当前日期时间的时间戳(以秒计)
     * example: unix_timestamp(LOCALTIMESTAMP) as rt ==> 1466436000
     *
     * @param currentTimestamp
     * @return
     */
    public int call(final Timestamp currentTimestamp) {
        return (int) (currentTimestamp.getTime() / 1000);
    }

    /**
     * 获取指定日期时间(指定格式)的时间戳(以秒计)
     * example: unix_timestamp('2016/06/20/23/20/00','yyyy/MM/dd/HH/mm/ss') as rt ==> 1466436000
     * example: unix_timestamp() as rt ==> 1466436000
     *
     * @param dt
     * @param ft
     * @return
     */
    public Integer call(final String dt, final String ft) {
        try {
            FastDateFormat sdf = FastDateFormat.getInstance(ft);
            Long unixTimestamp = sdf.parse(dt).getTime();
            return (int) (unixTimestamp / 1000L);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取指定日期时间(默认格式: 'yyyy-MM-dd HH:mm:ss')的时间戳(以秒计)
     * example: unix_timestamp('2016-06-20 23:20:00') as rt ==> 1466436000
     *
     * @param dt
     * @return
     */
    public Integer call(final String dt) {
        return call(dt, AbstractFunctionFactory.DEFAULT_DATETIME_FORMAT);
    }
}
