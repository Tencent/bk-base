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

public class INetAToN implements IFunction, Serializable {


    /**
     * IP转Long
     * example: inet_aton('127.0.0.1') as rt ==> 2130706433
     *
     * @param strIp ip
     * @return ip 转 long 后的数值
     */
    public Long call(final String strIp) {
        long[] ip = getIpArray(strIp);
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }

    /**
     * IP 转 long
     *
     * @param strIp ip
     * @param reverse 是否 reverse
     * @return ip 转 long 后的数值
     */
    public Long call(final String strIp, final boolean reverse) {
        long[] ip = getIpArray(strIp);
        if (reverse) {
            return (ip[3] << 24) + (ip[2] << 16) + (ip[1] << 8) + ip[0];
        } else {
            return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
        }
    }

    private long[] getIpArray(String strIp) {
        long[] ip = new long[4];
        int position1 = strIp.indexOf(".");
        int position2 = strIp.indexOf(".", position1 + 1);
        int position3 = strIp.indexOf(".", position2 + 1);
        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));
        return ip;
    }
}
