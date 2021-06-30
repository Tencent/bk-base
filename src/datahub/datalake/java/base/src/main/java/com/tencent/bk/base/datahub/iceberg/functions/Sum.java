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

package com.tencent.bk.base.datahub.iceberg.functions;

import org.apache.iceberg.data.Record;

public class Sum implements ValFunction {

    // 赋值的新值
    private final Number toAdd;

    /**
     * 构造函数
     *
     * @param toAdd 累加值
     */
    public Sum(Number toAdd) {
        this.toAdd = toAdd;
    }

    /**
     * 处理一条记录中的指定字段，将其与指定的值进行累加。
     *
     * @param record 一条记录
     * @param fieldName 待处理的字段
     */
    public void apply(Record record, String fieldName) {
        Object value = record.getField(fieldName);
        if (value instanceof Integer) {
            value = (Integer) value + toAdd.intValue();
        } else if (value instanceof Long) {
            value = (Long) value + toAdd.longValue();
        } else if (value instanceof Float) {
            value = (Float) value + toAdd.floatValue();
        } else if (value instanceof Double) {
            value = (Double) value + toAdd.doubleValue();
        }

        // 对于非Integer/Long/Float/Double类型，假定无法处理,仍然使用原值。
        record.setField(fieldName, value);
    }
}