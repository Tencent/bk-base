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

public class INetNToA implements IFunction, Serializable {

    /**
     * Long转IP
     * example: inet_ntoa(169681473, false) as rt ==> '127.0.0.1'
     *
     * @param longIp ip 对应的 long 值
     * @return ip
     */
    public String call(final long longIp) {
        StringBuffer sb = new StringBuffer("");
        sb.append(String.valueOf((longIp >>> 24)));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x000000FF)));
        return sb.toString();
    }

    /**
     * long 转 ip
     *
     * @param longIp ip 对应的 long 值
     * @param reverse 是否 reverse
     * @return ip
     */
    public String call(final long longIp, final boolean reverse) {
        StringBuffer sb = new StringBuffer("");
        if (reverse) {
            sb.append(String.valueOf((longIp & 0x000000FF)));
            sb.append(".");
            sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
            sb.append(".");
            sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
            sb.append(".");
            sb.append(String.valueOf((longIp >>> 24)));
        } else {
            sb.append(String.valueOf((longIp >>> 24)));
            sb.append(".");
            sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
            sb.append(".");
            sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
            sb.append(".");
            sb.append(String.valueOf((longIp & 0x000000FF)));
        }
        return sb.toString();
    }
}
