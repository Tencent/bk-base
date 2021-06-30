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

package com.tencent.bk.base.datalab.bksql.util;

public class BlueKingDataTypeMapper implements DataTypeMapper<String> {

    @Override
    public DataType toBKSqlType(String fromType) {
        switch (fromType) {
            case "string":
            case "text":
                return DataType.STRING;
            case "int":
                return DataType.INTEGER;
            case "long":
                return DataType.LONG;
            case "timestamp":
                return DataType.LONG;
            case "double":
                return DataType.DOUBLE;
            case "float":
                return DataType.FLOAT;
            case "bigint":
            case "bigdecimal":
            default:
                throw new IllegalArgumentException(
                        "unrecognizable blue king data type: " + fromType);
        }
    }

    @Override
    public String toExternalType(DataType fromType) {
        switch (fromType) {
            case INTEGER:
                return "int";
            case LONG:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
            case TIME_INDICATOR:
                return "string";
            case BOOLEAN:
            default:
                throw new IllegalArgumentException("unrecognizable data type: " + fromType);
        }
    }
}
