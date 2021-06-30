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

package com.tencent.bk.base.dataflow.core.function.base.udf;

import java.util.HashMap;
import java.util.Map;

public class UDFTypes {

    public enum UcDataType {

        INT("int", "整型"),
        INTEGER("integer", "整型"),
        STRING("string", "字符串"),
        // ARRAY_xxx服务于一转多行场景
        ARRAY_STRING("string[]", "字符串数组"),
        UTF8STRING("utf8string", "字符串"),
        LONG("long", "长整型"),
        FLOAT("double", "单精度浮点型"),
        DOUBLE("double", "双精度浮点型"),
        BOOLEAN("boolean", "布尔类型"),
        LIST_STRING("list", "列表类型");

        private static final Map<String, UcDataType> intToTypeMap = new HashMap<>();

        private String value;
        private String desc;

        static {
            for (UcDataType type : UcDataType.values()) {
                intToTypeMap.put(type.value, type);
            }
        }

        UcDataType(String value, String desc) {
            this.value = value;
            this.desc = desc;
        }

        public String getValue() {
            return value;
        }

        public String getDesc() {
            return desc;
        }

        public static UcDataType getTypeByValue(String value) {
            UcDataType type = intToTypeMap.get(value);
            if (type == null) {
                return null;
            }
            return type;
        }
    }
}
