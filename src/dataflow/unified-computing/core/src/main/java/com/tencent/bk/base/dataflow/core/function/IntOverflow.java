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
import java.math.BigInteger;

public class IntOverflow implements IFunction, Serializable {

    /**
     * int类型qq溢出转换函数
     * example: int_overflow(-1448817156) as qq ==> 2846150140
     *
     * @param overflowValue
     * @return
     */
    public Long call(int overflowValue) {
        try {
            if (overflowValue >= 0) {
                return Long.valueOf(overflowValue);
            }
            // 转二进制（python转为二进制得到的字符串有b,去掉）
            overflowValue = Math.abs(overflowValue);
            String binaryInput = Integer.toBinaryString(overflowValue);
            //补位
            if (binaryInput.length() < 32) {
                binaryInput = "0" + binaryInput;
            }

            StringBuilder reverseInput = new StringBuilder();
            //将0转换为char类型
            int a = 0;
            String s = String.valueOf(a);
            char c = s.charAt(0);
            //取反
            for (int i = 0; i < binaryInput.length(); i++) {
                if (c == binaryInput.charAt(i)) {
                    reverseInput.append("1");
                } else {
                    reverseInput.append("0");
                }
            }
            //转为十进制并 +1
            BigInteger resultValue = new BigInteger(reverseInput.toString(), 2);
            return resultValue.longValue() + 1;
        } catch (Exception e1) {
            try {
                return Long.valueOf(overflowValue);
            } catch (Exception e2) {
                return null;
            }
        }
    }
}
