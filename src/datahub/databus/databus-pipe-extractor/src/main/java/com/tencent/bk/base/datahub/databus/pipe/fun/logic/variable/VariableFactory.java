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

package com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable;

import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariableFactory {

    private static final Logger log = LoggerFactory.getLogger(VariableFactory.class);

    /**
     * 根据配置生成变量
     * 返回结果null时，表达式判断值永远为false
     */
    public static Variable createVariable(String op, Object o) {
        if (o == null) {
            return null;
        }

        String symbol = o.toString();
        // $开头字符串表示从传进数据中生成变量
        // $ALL 将传入的整个数据作为判断变量
        // $数字 表示传入数据为数组，取指定数字下标值为变量
        // $key 表示传入数据为map，取键为key的值作为变量
        if (symbol.startsWith("$")) {
            // 变量
            if (o.equals("$ALL")) {
                return new ObjVar();
            }

            // 去除开头的$，取变量类型
            String str = symbol.substring(1).trim();
            try {
                Integer i = Integer.parseInt(str);
                return new IndexVar(i);
            } catch (NumberFormatException e) {
                // 不是索引类型变量，那么只能是键值类型变量。。。
                return new KeyVar(str);
            }
        }

        switch (op) {
            case EtlConsts.LOGIC_EQUAL:
            case EtlConsts.LOGIC_NOT_EQUAL:
            case EtlConsts.LOGIC_STARTSWITH:
            case EtlConsts.LOGIC_ENDSWITH:
            case EtlConsts.LOGIC_CONTAINS:
                // 字符串类型比较，生成字符串常量
                return new ConstantVar(symbol);
            case EtlConsts.LOGIC_GREATER:
            case EtlConsts.LOGIC_NOT_GREATER:
            case EtlConsts.LOGIC_LESS:
            case EtlConsts.LOGIC_NOT_LESS:
                if (o instanceof String) {
                    try {
                        Object target;
                        if (symbol.contains(".")) {
                            target = Double.parseDouble(symbol.trim());
                        } else {
                            target = Long.parseLong(symbol.trim());
                        }
                        return new ConstantVar(target);
                    } catch (NumberFormatException e) {
                        log.warn("expect number, but: {}. return null", symbol);
                        // 不是合法数字。。。
                        return null;
                    }
                } else {
                    return new ConstantVar(o);
                }
            default:
                log.warn("unknown op {}. return null", op);
                return null;
        }
    }
}
